import asyncio
import logging
from math import sqrt
from typing import Dict, List, Optional, Tuple

from adapters.external.notify.telegram_notifier import TelegramNotifier
from core.repositories.strategy_episode_repository import StrategyEpisodeRepository

from ..repositories.signal_repository import SignalRepository
from adapters.external.pipeline.pipeline_http_client import PipelineHttpClient


USD_SET = {"USDC","USDBC","USDCE","USDT","DAI","USDD","USDP","BUSD"}

class ExecuteSignalPipelineUseCase:
    """
    Consumes PENDING signals from Mongo and executes their steps IN ORDER
    via PipelineHttpClient (api-liquidity-provider).

    Rules:
      - Steps = [COLLECT, WITHDRAW, SWAP_EXACT_IN, OPEN] (some may be skipped).
      - Each step retries with backoff.
      - On hard failure -> mark FAILED and stop processing that signal.
      - On full success -> mark SENT.

    Runtime sizing logic:
      - before SWAP_EXACT_IN we read /status to pick direction/amount.
      - before OPEN we read /status again to snapshot idle caps.
    """

    def __init__(
        self,
        signal_repo: SignalRepository,
        episode_repo: StrategyEpisodeRepository,
        lp_client: PipelineHttpClient,
        logger: Optional[logging.Logger] = None,
        max_retries: int = 5,
        base_backoff_sec: float = 1.0,
        notifier: Optional[TelegramNotifier] = None,
    ):
        self._signal_repo = signal_repo
        self._episode_repo = episode_repo
        self._lp = lp_client
        self._logger = logger or logging.getLogger(self.__class__.__name__)
        self._max_retries = max_retries
        self._base_backoff = base_backoff_sec
        self._notifier = notifier
        self.EPS_POS = 1e-12  # usado para clamps de raiz e separação Pa<P<Pb
    
    async def _notify_telegram(self, text: str) -> None:
        """
        Helper para enviar mensagem Telegram (se configurado).
        Não deixa exceção do notifier quebrar o fluxo.
        """
        if not self._notifier:
            return
        try:
            await self._notifier.send_message(text)
        except Exception as exc:
            self._logger.warning("Failed to send Telegram message: %s", exc)
            
    def _tokens_from_L(self, L, Pa, Pb, P):
        xa, xb, x = sqrt(Pa), sqrt(Pb), sqrt(P)
        if P <= Pa:   # tudo vira token0
            t0 = L * (1/xa - 1/xb); t1 = 0
        elif P >= Pb: # tudo vira token1
            t0 = 0; t1 = L * (xb - xa)
        else:         # misto
            t0 = L * (1/x - 1/xb)
            t1 = L * (x - xa)
        return t0, t1

    def _ensure_valid_band(self, Pa: float, Pb: float, P: float) -> Tuple[float, float]:
        """
        Garante:
        - Pa >= EPS_POS
        - Pb >= Pa + EPS_POS
        - Banda não degenera no mid (respeita almofada mínima em torno de P)
        """
        Pa = max(self.EPS_POS, Pa)
        Pb = max(Pa + self.EPS_POS, Pb)

        # almofada mínima ao redor de P (como você já faz em alguns pontos)
        mid_pad = self.EPS_POS * max(1.0, P)
        Pa = min(P - mid_pad, Pa)
        Pb = max(P + mid_pad, Pb)

        # Se por alguma razão Pa ultrapassou Pb após clamps, corrige separando pelo mid_pad:
        if not (Pa < Pb):
            Pa = P - mid_pad
            Pb = P + mid_pad

        return Pa, Pb

    def _is_usd(self, sym: str) -> bool:
        try:
            return (sym or "").upper() in USD_SET
        except Exception:
            return False
        
    def _L_closed(self, total_P: float, P: float, Pa: float, Pb: float) -> float:
        # assegura banda válida
        Pa, Pb = self._ensure_valid_band(Pa, Pb, P)

        a  = sqrt(max(self.EPS_POS, P))
        xa = sqrt(max(self.EPS_POS, Pa))
        xb = sqrt(max(self.EPS_POS, Pb))

        denom = 2 * a - xa - (P / xb)
        if denom <= 0:
            denom = self.EPS_POS
        return total_P / denom

    async def execute_once(self) -> None:
        """
        Fetch up to N pending signals and attempt to execute them.
        """
        pending = await self._signal_repo.list_pending(limit=50)
        for sig in pending:
            try:
                ok = await self._process_single_signal(sig)
                if ok:
                    await self._signal_repo.mark_success(sig)
                # if not ok, _process_single_signal already marked FAILED
            except Exception as exc:
                self._logger.exception("Unexpected error processing signal %s: %s", sig, exc)
                msg = f"UNEXPECTED: {exc}"
                await self._signal_repo.mark_failure(sig, msg)

                try:
                    episode = sig.get("episode") or {}
                    dex = episode.get("dex") or "?"
                    alias = episode.get("alias") or "?"
                    sig_id = sig.get("_id") or "?"

                    msg_lines = [
                        "🔥 *Erro inesperado ao processar sinal*",
                        f"• Sinal: `{sig_id}`",
                        f"• DEX: `{dex}`",
                        f"• Vault: `{alias}`",
                        f"• Erro: `{msg}`",
                    ]
                    await self._notify_telegram("\n".join(msg_lines))
                except Exception:
                    pass
                
    async def _append_log(
        self,
        episode_id: Optional[str],
        base: Dict,
    ) -> None:
        """
        Helper: push a log line into the episode doc, if we have an episode_id.
        """
        if not episode_id:
            return
        try:
            await self._episode_repo.append_execution_log(episode_id, base)
        except Exception as log_exc:
            # logging de fallback pra não matar o fluxo
            self._logger.warning("Failed to append_execution_log for %s: %s", episode_id, log_exc)


    async def _process_single_signal(self, sig: Dict) -> bool:
        """
        Executes a single signal's steps sequentially.
        Returns True on full success, False if FAILED.
        """
        steps: List[Dict] = sig.get("steps") or []
        
        last_episode = sig.get("last_episode") or {}
        last_episode_id = last_episode.get("_id")
        
        episode = sig.get("episode") or {}
        episode_id = episode.get("_id")
        
        dex = episode.get("dex")
        alias = episode.get("alias")
        token0_addr = episode.get("token0_address")
        token1_addr = episode.get("token1_address")
        majority_flag = episode.get("majority_on_open")

        batch_res = None
        
        st = None
        for step in steps:
            action = step.get("action")
            self._logger.info("Executing step %s for %s/%s", action, dex, alias)

            if (not dex or not alias) and action != "NOOP_LEGACY":
                skip_msg = "Skipping step because no dex/alias is wired for this strategy."
                self._logger.info("%s %s", action, skip_msg)
                
                await self._append_log(
                    episode_id,
                    {
                        "step": action,
                        "phase": "skipped_no_dex_alias",
                        "reason": skip_msg,
                    },
                )
                
                continue
            
            success = False
            last_err: Optional[str] = None

            for attempt in range(self._max_retries):
                try:
                    if action == "NOOP_LEGACY":
                        success = True
                        await self._append_log(
                            episode_id,
                            {
                                "step": action,
                                "phase": "noop",
                                "attempt": attempt + 1,
                                "info": "NOOP_LEGACY executed",
                            },
                        )
                    
                    
                    elif action == "BATCH_REQUEST":
                        # after withdraw, capital is idle in vault.
                        st = await self._lp.get_status(dex, alias)
                        if not st:
                            raise RuntimeError("status_unavailable_before_swap")
                        
                        lower_price = step["payload"].get("lower_price")
                        upper_price = step["payload"].get("upper_price")
                        if lower_price is None or upper_price is None:
                            raise RuntimeError("range_prices_required")
                        
                        holdings = st.get("holdings", {}) or {}
                        totals = holdings.get("totals", {}) or {}
                        amt0 = float(totals.get("token0", 0.0))
                        amt1 = float(totals.get("token1", 0.0))
                        
                        prices = st.get("prices", {}) or {}
                        cur = prices.get("current", {}) or {}
                        p_t1_t0_spot = float(cur.get("p_t1_t0", 0.0))    # canônico (token1 per token0)
                        p_t0_t1_spot = (0.0 if p_t1_t0_spot == 0.0 else 1.0 / p_t1_t0_spot)

                        syms = (holdings.get("symbols") or {})
                        sym0 = (syms.get("token0") or "").upper()
                        sym1 = (syms.get("token1") or "").upper()

                        addrs = (holdings.get("addresses") or {})
                        t0_addr = addrs.get("token0") or token0_addr
                        t1_addr = addrs.get("token1") or token1_addr
                        
                        token0_is_usd = self._is_usd(sym0)
                        token1_is_usd = self._is_usd(sym1)

                        
                        # ---------- Escala HUMANA: USDC por 1 RISCO quando há USD em um dos lados ----------
                        # Pa/Pb já chegam nessa escala humana; então ajustamos o spot para a MESMA escala:
                        Pa_h = float(lower_price)
                        Pb_h = float(upper_price)
                        if Pa_h <= 0 or Pb_h <= 0:
                            raise RuntimeError("Prices must be positive.")
                        if Pa_h > Pb_h:
                            Pa_h, Pb_h = Pb_h, Pa_h

                        if token0_is_usd and not token1_is_usd:
                            # par USDC/CRYPTO → humano é p_t0_t1
                            P_h = p_t0_t1_spot
                            human_is_t0_t1 = True
                        elif token1_is_usd and not token0_is_usd:
                            # par CRYPTO/USDC → humano é p_t1_t0
                            P_h = p_t1_t0_spot
                            human_is_t0_t1 = False
                        else:
                            # sem USD: mantemos humano como p_t1_t0
                            P_h = p_t1_t0_spot
                            human_is_t0_t1 = False
                        
                        # ---------- Valoração em USD na escala humana ----------
                        # USD por unidade conforme a escala humana escolhida
                        if token1_is_usd:
                            usd_per_t0 = P_h     # se P_h=p_t1_t0, USD por token0 = P_h
                            usd_per_t1 = 1.0     # token1 já é USD
                        elif token0_is_usd:
                            usd_per_t0 = 1.0     # token0 já é USD
                            usd_per_t1 = P_h     # se P_h=p_t0_t1, USD por token1 = P_h
                        else:
                            # fallback: trate token1 como 'quote' (P_h ~ p_t1_t0)
                            usd_per_t0 = P_h
                            usd_per_t1 = 1.0

                        usd0 = amt0 * usd_per_t0
                        usd1 = amt1 * usd_per_t1
                        total_usd = usd0 + usd1

                        await self._append_log(
                            episode_id,
                            {
                                "step": action,
                                "phase": "pre_calc",
                                "attempt": attempt + 1,
                                "holdings_raw": {"amt0": amt0, "amt1": amt1},
                                "symbols": {"sym0": sym0, "sym1": sym1},
                                "human_scale": {
                                    "P_h": P_h, "Pa_h": Pa_h, "Pb_h": Pb_h,
                                    "human_is_t0_t1": human_is_t0_t1
                                },
                                "canonical_spot": {
                                    "p_t1_t0_spot": p_t1_t0_spot,
                                    "p_t0_t1_spot": p_t0_t1_spot
                                },
                                "usd_per_token": {"usd_per_t0": usd_per_t0, "usd_per_t1": usd_per_t1},
                                "valuation": {"usd0": usd0, "usd1": usd1, "total_usd": total_usd},
                            },
                        )
                        
                        # --------------- Conversão p/ CANÔNICO nas fórmulas internas ---------------
                        # Pa/Pb/P sempre em p_t1_t0 para _L_closed/_tokens_from_L
                        if human_is_t0_t1:
                            # humano (Pa_h,Pb_h) = p_t0_t1; converter para p_t1_t0
                            Pa_c = 1.0 / Pa_h
                            Pb_c = 1.0 / Pb_h
                            if Pa_c > Pb_c:
                                Pa_c, Pb_c = Pb_c, Pa_c
                            P_c = p_t1_t0_spot
                        else:
                            Pa_c, Pb_c, P_c = Pa_h, Pb_h, p_t1_t0_spot

                        # --------------- Liquidez alvo + AUTO-CALIBRAÇÃO ---------------
                        # 1) L provisório:
                        L_target_guess = self._L_closed(total_usd, P_c, Pa_c, Pb_c)

                        # 2) Tokens com L provisório:
                        t0_g, t1_g = self._tokens_from_L(L_target_guess, Pa_c, Pb_c, P_c)

                        # 3) Valuation desses tokens:
                        value_guess_usd = t0_g * usd_per_t0 + t1_g * usd_per_t1
                        scale = 1.0
                        if value_guess_usd > 1e-18:
                            scale = total_usd / value_guess_usd

                        # 4) L final calibrado e tokens finais coerentes com total_usd:
                        L_target = L_target_guess * scale
                        t0_needed, t1_needed = self._tokens_from_L(L_target, Pa_c, Pb_c, P_c)
                        value_final_usd = t0_needed * usd_per_t0 + t1_needed * usd_per_t1

                        await self._append_log(
                            episode_id,
                            {
                                "step": action,
                                "phase": "calc_tokens",
                                "attempt": attempt + 1,
                                "canonical_prices": {"Pa_c": Pa_c, "Pb_c": Pb_c, "P_c": P_c},
                                "liquidity": {
                                    "L_guess": L_target_guess,
                                    "value_guess_usd": value_guess_usd,
                                    "scale": scale,
                                    "L_final": L_target,
                                    "value_final_usd": value_final_usd,
                                    "total_usd": total_usd
                                },
                                "tokens_needed": {"t0_needed": t0_needed, "t1_needed": t1_needed}
                            },
                        )
                        
                        # ---------- Lados (USD vs risco) ----------
                        usd_side  = 0 if token0_is_usd else (1 if token1_is_usd else 1)
                        risk_side = 1 - usd_side

                        # USD atual em cada lado (na escala humana)
                        risk_usd_now = usd1 if risk_side == 1 else usd0
                        usd_usd_now  = usd1 if usd_side  == 1 else usd0

                        # USD necessário em cada lado (na escala humana)
                        if risk_side == 1:
                            risk_needed_usd = t1_needed * usd_per_t1
                            usd_needed_usd  = t0_needed * usd_per_t0
                        else:
                            risk_needed_usd = t0_needed * usd_per_t0
                            usd_needed_usd  = t1_needed * usd_per_t1

                        majority_flag = (episode.get("majority_on_open") or "").lower()  # "token1" (usd-like) | "token2" (risco)
                        align_usd = (majority_flag == "token1")

                        falta_usd = (usd_needed_usd - usd_usd_now) if align_usd else (risk_needed_usd - risk_usd_now)

                        # --------------- Encerrar cedo + log seguro ---------------
                        if abs(falta_usd) <= 1e-9:
                            await self._append_log(
                                episode_id,
                                {
                                    "step": action,
                                    "phase": "skip_small",
                                    "attempt": attempt + 1,
                                    "reason": f"no meaningful delta ({'usd side' if align_usd else 'risk side'})",
                                    "post_tokens_needed": {"t0_needed": t0_needed, "t1_needed": t1_needed},
                                    "post_needed_usd": {"risk_needed_usd": risk_needed_usd, "usd_needed_usd": usd_needed_usd},
                                    "balances_usd": {"risk_usd_now": risk_usd_now, "usd_usd_now": usd_usd_now},
                                    "falta_usd": falta_usd
                                }
                            )
                            success = True
                            break
                        
                        # --------------- Escolha token_in/out e amount_in em UNIDADES do token_in ---------------
                        token_in_addr = ""
                        token_out_addr = ""
                        if align_usd:
                            if falta_usd > 0:
                                # comprar USD vendendo risco
                                token_in_addr  = t1_addr if risk_side == 1 else t0_addr
                                token_out_addr = t0_addr if usd_side  == 0 else t1_addr
                                usd_per_in     = usd_per_t1 if (risk_side == 1) else usd_per_t0
                            else:
                                # vender USD e comprar risco
                                token_in_addr  = t0_addr if usd_side  == 0 else t1_addr
                                token_out_addr = t1_addr if risk_side == 1 else t0_addr
                                usd_per_in     = usd_per_t0 if (usd_side == 0) else usd_per_t1
                        else:
                            # alinhar lado de risco
                            if falta_usd > 0:
                                # comprar RISCO vendendo USD
                                token_in_addr  = t0_addr if usd_side  == 0 else t1_addr
                                token_out_addr = t1_addr if risk_side == 1 else t0_addr
                                usd_per_in     = usd_per_t0 if (usd_side == 0) else usd_per_t1
                            else:
                                # vender RISCO e comprar USD
                                token_in_addr  = t1_addr if risk_side == 1 else t0_addr
                                token_out_addr = t0_addr if usd_side  == 0 else t1_addr
                                usd_per_in     = usd_per_t1 if (risk_side == 1) else usd_per_t0

                        amount_in_tokens = abs(falta_usd) / max(usd_per_in, 1e-18)
                        
                        # --------------- Teto por saldo disponível do token_in ---------------
                        bal_in_tokens = amt1 if token_in_addr.lower() == (t1_addr or "").lower() else amt0
                        if amount_in_tokens > bal_in_tokens:
                            amount_in_tokens = bal_in_tokens

                        # margem minúscula para evitar “valor exato”
                        amount_in_tokens = max(0.0, amount_in_tokens - 1e-12)
                        
                        await self._append_log(
                            episode_id,
                            {
                                "step": action,
                                "phase": "calc_swap",
                                "attempt": attempt + 1,
                                "majority_flag": majority_flag,
                                "p_human": P_h,
                                "usd_per_token": {"usd_per_t0": usd_per_t0, "usd_per_t1": usd_per_t1},
                                "valuation": {"usd0": usd0, "usd1": usd1, "total_usd": total_usd},
                                "tokens_needed": {"t0_needed": t0_needed, "t1_needed": t1_needed},
                                "needed_usd": {"risk_needed_usd": risk_needed_usd, "usd_needed_usd": usd_needed_usd},
                                "balances_usd": {"risk_usd_now": risk_usd_now, "usd_usd_now": usd_usd_now},
                                "align_usd": align_usd,
                                "falta_usd": float(falta_usd),
                                "token_in": token_in_addr,
                                "token_out": token_out_addr,
                                "usd_per_in": usd_per_in,
                                "amount_in_tokens": amount_in_tokens,
                                "request_open": {"lower_price": lower_price, "upper_price": upper_price},
                                "liquidity_final": {"L_target": L_target, "Pa_c": Pa_c, "Pb_c": Pb_c, "P_c": P_c}
                            },
                        )

                        if amount_in_tokens <= 0.0:
                            await self._append_log(
                                episode_id,
                                {
                                    "step": action,
                                    "phase": "skip_small",
                                    "attempt": attempt + 1,
                                    "reason": "no meaningful delta (post-margin)"
                                }
                            )
                            success = True
                            break

                        batch_res = await self._lp.post_pancake_batch_unstake_exit_swap_open(
                            alias=alias,
                            token_in=token_in_addr,
                            token_out=token_out_addr,
                            amount_in=amount_in_tokens,   # unidades do token_in
                            amount_in_usd=None,           # evitar restrição ETH/USDC
                            lower_price=lower_price,
                            upper_price=upper_price,
                        )
                        await self._append_log(
                            episode_id,
                            {
                                "step": action,
                                "phase": "swap_call",
                                "attempt": attempt + 1,
                                "request": {
                                    "token_in": token_in_addr,
                                    "token_out": token_out_addr,
                                    "amount_in": amount_in_tokens,
                                },
                                "request_open": {"lower_price": lower_price, "upper_price": upper_price},
                                "response": batch_res,
                            },
                        )
                        if batch_res is None:
                            raise RuntimeError("swap_failed")
                        success = True
                        
                    elif action == "UNSTAKE":
                        # só desestaca se status indica que há gauge e está staked
                        st = await self._lp.get_status(dex, alias)
                        if not st:
                            raise RuntimeError("status_unavailable_before_unstake")

                        if bool(st.get("has_gauge")) and (st.get("staked") or st.get("position_location") == "gauge"):
                            res = await self._lp.post_unstake(dex, alias)
                            await self._append_log(episode_id, {
                                "step": action, "phase": "unstake_call",
                                "attempt": attempt + 1, "request": {"dex": dex, "alias": alias},
                                "response": res,
                            })
                            if res is None:
                                raise RuntimeError("unstake_failed")
                        else:
                            await self._append_log(episode_id, {
                                "step": action, "phase": "skip_no_stake",
                                "attempt": attempt + 1,
                                "reason": "no gauge or already unstaked",
                            })
                        success = True
                    
                    elif action == "COLLECT":
                        st = await self._lp.get_status(dex, alias)
                        if not st:
                            raise RuntimeError("status_unavailable_before_swap")
                        
                        position_location = st.get("position_location", None)
                        if position_location == "pool":
                            res = await self._lp.post_collect(dex, alias)
                            await self._append_log(
                                episode_id,
                                {
                                    "step": action,
                                    "attempt": attempt + 1,
                                    "request": {"dex": dex, "alias": alias},
                                    "response": res,
                                },
                            )
                            if res is None:
                                raise RuntimeError("collect_failed")
                        
                        success = True
                            
                    elif action == "WITHDRAW":
                        st = await self._lp.get_status(dex, alias)
                        if not st:
                            raise RuntimeError("status_unavailable_before_swap")
                        
                        position_location = st.get("position_location", None)
                        if position_location == "pool":
                            # always withdraw mode "pool" to bring capital back idle
                            res = await self._lp.post_withdraw(dex, alias, mode="pool")
                            await self._append_log(
                                episode_id,
                                {
                                    "step": action,
                                    "attempt": attempt + 1,
                                    "request": {"dex": dex, "alias": alias, "mode": "pool"},
                                    "response": res,
                                },
                            )
                            if res is None:
                                raise RuntimeError("withdraw_failed")
                            
                        success = True

                    elif action == "SWAP_EXACT_IN_REWARD":
                        # after withdraw, capital is idle in vault.
                        st = await self._lp.get_status(dex, alias)
                        if not st:
                            raise RuntimeError("status_unavailable_before_swap")
                        
                        gauge_reward_balances = st.get("gauge_reward_balances", {}) or {}
                        reward_token = gauge_reward_balances.get("token", None)
                        reward_symbol = gauge_reward_balances.get("symbol", None)
                        reward_in_vault = gauge_reward_balances.get("in_vault", 0.0)
                        reward_in_vault_to_swap = reward_in_vault - 0.000001
                        
                        token0_addr = episode.get("token0_address")  
                        token1_addr = episode.get("token1_address")  
                        
                        holdings = st.get("holdings", {}) or {}
                        syms = (holdings.get("symbols") or {})
                        sym0 = (syms.get("token0") or "").upper()
                        sym1 = (syms.get("token1") or "").upper()
                        
                        token0_is_usd = self._is_usd(sym0)
                        token1_is_usd = self._is_usd(sym1)
                        
                        token_out_addr = None
                        if token0_is_usd:
                            token_out_addr = token0_addr
                        else:
                            token_out_addr = token1_addr
                            
                        if reward_token and reward_symbol and reward_in_vault > 0:
                            # log snapshot BEFORE swap calc
                            await self._append_log(
                                episode_id,
                                {
                                    "step": action,
                                    "phase": "pre_calc",
                                    "attempt": attempt + 1,
                                    "reward_token": reward_token,
                                    "reward_symbol": reward_symbol,
                                    "reward_in_vault": reward_in_vault,
                                    "reward_in_vault_to_swap": reward_in_vault_to_swap
                                },
                            )
                            
                            res = await self._lp.post_swap_exact_in(
                                dex=dex if dex == "pancake" else "uniswap",
                                alias=alias,
                                token_in=reward_token,
                                token_out=token_out_addr,
                                amount_in=reward_in_vault_to_swap,
                                pool_override="CAKE_USDC" if dex == "pancake" else "AERO_USDC",
                                convert_gauge_to_usdc=True
                            )
                                
                            await self._append_log(
                                episode_id,
                                {
                                    "step": action,
                                    "phase": "swap_call",
                                    "attempt": attempt + 1,
                                    "request": {
                                        "token_in": reward_token,
                                        "token_out": token1_addr,
                                        "amount_in_usd": reward_in_vault_to_swap,
                                    },
                                    "response": res,
                                },
                            )

                            if res is None:
                                raise RuntimeError("swap_failed")

                            success = True
                        else:
                            await self._append_log(
                                episode_id,
                                    {
                                        "step": action,
                                        "phase": "skip_small",
                                        "attempt": attempt + 1,
                                        "reward_token": reward_token,
                                        "reward_symbol": reward_symbol,
                                        "reward_in_vault": reward_in_vault
                                    },
                                )
                            success = True
                            
                    elif action == "SWAP_EXACT_IN":
                        # after withdraw, capital is idle in vault.
                        st = await self._lp.get_status(dex, alias)
                        if not st:
                            raise RuntimeError("status_unavailable_before_swap")
                        
                        holdings = st.get("holdings", {}) or {}
                        totals = holdings.get("totals", {}) or {}
                        amt0 = float(totals.get("token0", 0.0))
                        amt1 = float(totals.get("token1", 0.0))
                        
                        prices = st.get("prices", {}) or {}
                        current_prices = prices.get("current", {}) or {}
                        p_t1_t0 = float(current_prices.get("p_t1_t0", 0.0))
                    
                        # log snapshot BEFORE swap calc
                        await self._append_log(
                            episode_id,
                            {
                                "step": action,
                                "phase": "pre_calc",
                                "attempt": attempt + 1,
                                "holdings_raw": {
                                    "amt0": amt0,
                                    "amt1": amt1,
                                },
                                "price_snapshot": {
                                    "p_t1_t0": p_t1_t0,
                                },
                            },
                        )

                        if p_t1_t0 <= 0.0:
                            # sem preço confiável = não dá pra calcular USD; nesse caso a gente não swapa
                            await self._append_log(
                                episode_id,
                                {
                                    "step": action,
                                    "phase": "skip_no_price",
                                    "attempt": attempt + 1,
                                    "reason": "p_t1_t0 <= 0",
                                },
                            )
                            success = True
                        else:
                            usd0 = amt0 * p_t1_t0  # quanto vale nosso token0 em USDC
                            usd1 = amt1            # token1 já é USDC
                            total_usd = usd0 + usd1
                            P = p_t1_t0
                            
                            Pa = step["payload"].get("lower_price")
                            Pb = step["payload"].get("upper_price")
                            L_target = self._L_closed(total_usd, P, Pa, Pb)
                            t0_needed, t1_needed = self._tokens_from_L(L_target, Pa, Pb, P)
                            
                            majority_flag = episode.get("majority_on_open")
                            
                            falta_t0 = None
                            falta_t1 = None
                            t0_needed_usd = None
                            if majority_flag == "token1":
                                # queremos alinhar token1 (USDC-like)
                                falta_t1 = t1_needed - usd1
                                if falta_t1 > 0:
                                    token_in_addr = token0_addr  # vender WETH
                                    token_out_addr = token1_addr # comprar USDC
                                    req_amount_usd = falta_t1
                                    direction = "WETH->USDC"
                                else:
                                    token_in_addr = token1_addr  # vender USDC
                                    token_out_addr = token0_addr # comprar WETH
                                    req_amount_usd = (-falta_t1)
                                    direction = "USDC->WETH"
                                    
                            else:
                                # majority_flag == "token2" (WETH)
                                t0_needed_usd = t0_needed * P
                                falta_t0  = t0_needed_usd - usd0
                                if falta_t0 > 0:
                                    token_in_addr = token1_addr  # vender USDC
                                    token_out_addr = token0_addr # comprar WETH
                                    req_amount_usd = falta_t0
                                    direction = "USDC->WETH"
                                else:
                                    token_in_addr = token0_addr  # vender WETH
                                    token_out_addr = token1_addr # comprar USDC
                                    req_amount_usd = (-falta_t0)
                                    direction = "WETH->USDC"

                            # para evitar o valor exato e causar erros de saldo
                            req_amount_usd = req_amount_usd - 0.01
                            
                            # log cálculo alvo
                            await self._append_log(
                                episode_id,
                                {
                                    "step": action,
                                    "phase": "calc_swap",
                                    "attempt": attempt + 1,
                                    "majority_flag": majority_flag,
                                    "p_t1_t0": p_t1_t0,
                                    "usd0": usd0,
                                    "usd1": usd1,
                                    "total_usd": total_usd,
                                    "t0_needed":t0_needed if t0_needed else None,
                                    "t1_needed": t1_needed if t1_needed else None,
                                    "falta_t1": falta_t1 if falta_t1 else None,
                                    "falta_t0": falta_t0 if falta_t0 else None,
                                    "t0_needed_usd": t0_needed_usd if t0_needed_usd else None,
                                    "req_amount_usd": req_amount_usd if req_amount_usd else None,
                                    "direction": direction,
                                    "request_amount_in_usd": req_amount_usd,
                                },
                            )

                            # se req_amount_usd ~ 0, nada a fazer
                            if req_amount_usd <= 0.0:
                                await self._append_log(
                                    episode_id,
                                    {
                                        "step": action,
                                        "phase": "skip_small",
                                        "attempt": attempt + 1,
                                        "reason": "no meaningful delta",
                                    },
                                )
                                success = True
                            else:
                                res = await self._lp.post_swap_exact_in(
                                    dex="aerodrome", # todas as pools sao WETH/USDC, entao o swap sempre aerodrome com fee 0.0008
                                    alias=alias,
                                    token_in=token_in_addr,
                                    token_out=token_out_addr,
                                    amount_in_usd=req_amount_usd if direction == "WETH->USDC" else None,
                                    amount_in=req_amount_usd if direction == "USDC->WETH" else None,
                                    pool_override="WETH_USDC" if dex == "pancake" else None
                                )

                                await self._append_log(
                                    episode_id,
                                    {
                                        "step": action,
                                        "phase": "swap_call",
                                        "attempt": attempt + 1,
                                        "request": {
                                            "token_in": token_in_addr,
                                            "token_out": token_out_addr,
                                            "amount_in_usd": req_amount_usd,
                                        },
                                        "response": res,
                                    },
                                )

                                if res is None:
                                    raise RuntimeError("swap_failed")

                                success = True
                                                 
                    elif action == "OPEN":
                        # Antes de abrir nova faixa, snapshot de idle caps atuais
                        st2 = await self._lp.get_status(dex, alias)
                        if not st2:
                            raise RuntimeError("status_unavailable_before_open")

                        hold2 = st2.get("holdings", {}) or {}
                        totals2 = hold2.get("totals", {}) or {}
                        cap0 = float(totals2.get("token0", 0.0))
                        cap1 = float(totals2.get("token1", 0.0))

                        lower_price = step["payload"].get("lower_price")
                        upper_price = step["payload"].get("upper_price")

                        await self._append_log(
                            episode_id,
                            {
                                "step": action,
                                "phase": "pre_open",
                                "attempt": attempt + 1,
                                "idle_caps": {
                                    "cap0": cap0,
                                    "cap1": cap1,
                                },
                                "range": {
                                    "lower_price": lower_price,
                                    "upper_price": upper_price,
                                },
                            },
                        )

                        # Chamar o novo endpoint open
                        res = await self._lp.post_open(
                            dex=dex,
                            alias=alias,
                            lower_price=lower_price,
                            upper_price=upper_price,
                            lower_tick=None,
                            upper_tick=None,
                        )

                        await self._append_log(
                            episode_id,
                            {
                                "step": action,
                                "phase": "open_call",
                                "attempt": attempt + 1,
                                "request": {
                                    "lower_price": lower_price,
                                    "upper_price": upper_price,
                                },
                                "response": res,
                            },
                        )

                        if res is None:
                            raise RuntimeError("open_failed")
                        success = True
                    
                    elif action == "STAKE":
                        # estaca somente se existir gauge e a posição estiver no pool (não-gauge)
                        st = await self._lp.get_status(dex, alias)
                        if not st:
                            raise RuntimeError("status_unavailable_before_stake")

                        if bool(st.get("has_gauge")) and (st.get("position_location") != "gauge"):
                            # token_id é opcional; o provider pode resolver internamente
                            token_id = step.get("payload", {}).get("token_id")
                            res = await self._lp.post_stake(dex, alias, token_id=token_id)
                            await self._append_log(episode_id, {
                                "step": action, "phase": "stake_call",
                                "attempt": attempt + 1, "request": {"token_id": token_id},
                                "response": res,
                            })
                            if res is None:
                                raise RuntimeError("stake_failed")
                        else:
                            await self._append_log(episode_id, {
                                "step": action, "phase": "skip_no_gauge",
                                "attempt": attempt + 1,
                                "reason": "no gauge or already staked",
                            })
                        success = True

                    else:
                        raise RuntimeError(f"unknown action {action}")

                    if success:
                        break

                except Exception as exc:
                    last_err = str(exc)
                    self._logger.warning(
                        "Step %s failed on attempt %s/%s: %s",
                        action, attempt + 1, self._max_retries, exc,
                    )
                    
                    await self._append_log(
                        episode_id,
                        {
                            "step": action,
                            "phase": "attempt_fail",
                            "attempt": attempt + 1,
                            "error": last_err,
                        },
                    )
                    
                    try:
                        dex_safe = dex or "?"
                        alias_safe = alias or "?"
                        msg_lines = [
                            "⚠️ *Falha ao executar step*",
                            f"• Step: `{action}`",
                            f"• DEX: `{dex_safe}`",
                            f"• Vault: `{alias_safe}`",
                            f"• Tentativa: `{attempt + 1}/{self._max_retries}`",
                            f"• Erro: `{last_err}`",
                        ]
                        # Marca explicitamente quando for a ÚLTIMA tentativa
                        if attempt + 1 >= self._max_retries:
                            msg_lines.append("")
                            msg_lines.append("🚨 *Última tentativa de retry atingida para este step.*")

                        await self._notify_telegram("\n".join(msg_lines))
                    except Exception:
                        # já é tratado dentro do _notify_telegram, então aqui é extra defensivo
                        pass
                    
                    # incremental backoff
                    await asyncio.sleep(self._base_backoff * (attempt + 1))

            if not success:
                # hard fail -> mark FAILED and stop this signal
                fail_msg = last_err or f"{action} failed"
                await self._signal_repo.mark_failure(sig, fail_msg)
                
                await self._append_log(
                    episode_id,
                    {
                        "step": action,
                        "phase": "hard_fail",
                        "error": fail_msg,
                    },
                )
                
                try:
                    dex_safe = dex or "?"
                    alias_safe = alias or "?"
                    sig_id = sig.get("_id") or "?"
                    msg_lines = [
                        "💥 *HARD FAIL ao processar sinal*",
                        f"• Sinal: `{sig_id}`",
                        f"• Step: `{action}`",
                        f"• DEX: `{dex_safe}`",
                        f"• Vault: `{alias_safe}`",
                        f"• Erro final: `{fail_msg}`",
                        "",
                        "Todos os retries foram esgotados para este step.",
                    ]
                    if episode_id:
                        msg_lines.append(f"• Episódio atual: `{episode_id}`")
                    if last_episode_id:
                        msg_lines.append(f"• Episódio anterior: `{last_episode_id}`")

                    await self._notify_telegram("\n".join(msg_lines))
                except Exception:
                    pass
                
                return False

        # all steps ok
        await self._append_log(
            episode_id,
            {
                "phase": "all_steps_done",
                "status": "SENT",
            },
        )
        
        try:
            if batch_res and last_episode_id and last_episode:
                # Usamos AFTER porque:
                # - o batch tira snapshot "before"
                # - executa tx + add_collected_fees_snapshot (atualiza fees_collected_cum no state_repo)
                # - tira snapshot "after"
                # Ou seja, o AFTER reflete o lifetime já incluindo o collect feito pelo batch.
                after = batch_res.get("after") or {}
                
                snapshot = after

                totals = snapshot.get("totals") or {}
                vault_idle = snapshot.get("vault_idle") or {}
                in_position = snapshot.get("in_position") or {}

                gauge_rewards = snapshot.get("gauge_rewards") or {}
                rewards_collected_cum = snapshot.get("rewards_collected_cum") or {}
                fees_uncollected = snapshot.get("fees_uncollected") or {}
                fees_collected_cum = snapshot.get("fees_collected_cum") or {}
                gauge_reward_balances = snapshot.get("gauge_reward_balances") or {}

                # -------------------------
                # 1) Lifetime atual em unidades de token (baseline "now")
                # -------------------------
                pending_cake = float(gauge_rewards.get("pending_amount") or 0.0)
                pending_cake_usd_est = float(gauge_rewards.get("pending_usd_est") or 0.0)
                cake_in_vault = float(gauge_reward_balances.get("in_vault") or 0.0)

                rewards_usdc_lifetime = float(rewards_collected_cum.get("usdc") or 0.0)

                fees_uncol_t0 = float(fees_uncollected.get("token0") or 0.0)
                fees_uncol_t1 = float(fees_uncollected.get("token1") or 0.0)
                fees_col_t0 = float(fees_collected_cum.get("token0") or 0.0)
                fees_col_t1 = float(fees_collected_cum.get("token1") or 0.0)

                lifetime_now = {
                    # CAKE
                    "gauge_rewards_pending_cake": pending_cake,
                    "gauge_rewards_pending_usd_est": pending_cake_usd_est,
                    "gauge_reward_balances_in_vault_cake": cake_in_vault,

                    # Rewards já realizados em USDC
                    "rewards_collected_usdc": rewards_usdc_lifetime,

                    # Fees LP
                    "fees_uncollected_token0": fees_uncol_t0,
                    "fees_uncollected_token1": fees_uncol_t1,
                    "fees_collected_token0": fees_col_t0,
                    "fees_collected_token1": fees_col_t1,
                }

                # agregados
                lifetime_now["fees_total_token0"] = lifetime_now["fees_uncollected_token0"] + lifetime_now["fees_collected_token0"]
                lifetime_now["fees_total_token1"] = lifetime_now["fees_uncollected_token1"] + lifetime_now["fees_collected_token1"]
                lifetime_now["rewards_total_cake"] = (
                    lifetime_now["gauge_rewards_pending_cake"] +
                    lifetime_now["gauge_reward_balances_in_vault_cake"]
                )

                # -------------------------
                # 2) Baseline anterior (prev) – em tokens
                # -------------------------
                prev_metrics = (last_episode.get("metrics") or {})
                prev_baseline_dict = prev_metrics.get("fees_lifetime_baseline") or {}

                def _prev(key: str) -> float:
                    return float(prev_baseline_dict.get(key) or 0.0)

                prev_lifetime_fee_t0       = _prev("fees_total_token0")
                prev_lifetime_fee_t1       = _prev("fees_total_token1")
                prev_rewards_usdc_lifetime = _prev("rewards_collected_usdc")
                prev_rewards_cake_lifetime = _prev("rewards_total_cake")

                prev_pending_cake          = _prev("gauge_rewards_pending_cake")
                prev_pending_cake_usd_est  = _prev("gauge_rewards_pending_usd_est")
                prev_cake_in_vault         = _prev("gauge_reward_balances_in_vault_cake")
                prev_uncol_t0              = _prev("fees_uncollected_token0")
                prev_uncol_t1              = _prev("fees_uncollected_token1")
                prev_col_t0                = _prev("fees_collected_token0")
                prev_col_t1                = _prev("fees_collected_token1")

                # -------------------------
                # 3) Deltas deste episódio em tokens (sempre >= 0)
                # -------------------------
                delta_fee_t0_tokens = max(0.0, lifetime_now["fees_total_token0"] - prev_lifetime_fee_t0)
                delta_fee_t1_tokens = max(0.0, lifetime_now["fees_total_token1"] - prev_lifetime_fee_t1)
                delta_rewards_usdc  = max(0.0, lifetime_now["rewards_collected_usdc"] - prev_rewards_usdc_lifetime)
                delta_cake_tokens   = max(0.0, lifetime_now["rewards_total_cake"] - prev_rewards_cake_lifetime)

                # -------------------------
                # 4) Baseline PARA O PRÓXIMO EPISÓDIO (monotônico em tokens)
                # -------------------------
                baseline_for_next = {
                    "gauge_rewards_pending_cake": max(prev_pending_cake, lifetime_now["gauge_rewards_pending_cake"]),
                    "gauge_rewards_pending_usd_est": max(prev_pending_cake_usd_est, lifetime_now["gauge_rewards_pending_usd_est"]),
                    "gauge_reward_balances_in_vault_cake": max(prev_cake_in_vault, lifetime_now["gauge_reward_balances_in_vault_cake"]),

                    "rewards_collected_usdc": max(prev_rewards_usdc_lifetime, lifetime_now["rewards_collected_usdc"]),

                    "fees_uncollected_token0": max(prev_uncol_t0, lifetime_now["fees_uncollected_token0"]),
                    "fees_uncollected_token1": max(prev_uncol_t1, lifetime_now["fees_uncollected_token1"]),
                    "fees_collected_token0": max(prev_col_t0, lifetime_now["fees_collected_token0"]),
                    "fees_collected_token1": max(prev_col_t1, lifetime_now["fees_collected_token1"]),

                    "fees_total_token0": max(prev_lifetime_fee_t0, lifetime_now["fees_total_token0"]),
                    "fees_total_token1": max(prev_lifetime_fee_t1, lifetime_now["fees_total_token1"]),
                    "rewards_total_cake": max(prev_rewards_cake_lifetime, lifetime_now["rewards_total_cake"]),
                }

                total_fees_lifetime_now = {
                    "fees_total_token0": lifetime_now["fees_total_token0"],
                    "fees_total_token1": lifetime_now["fees_total_token1"],
                    "rewards_total_cake": lifetime_now["rewards_total_cake"],
                    "rewards_collected_usdc": lifetime_now["rewards_collected_usdc"],
                }

                # -------------------------
                # 5) Conversão dos deltas -> USD (incluindo CAKE)
                # -------------------------
                prices = (snapshot.get("prices") or {})
                p_t1_t0 = float(prices.get("p_t1_t0") or 0.0)
                p_t0_t1 = float(prices.get("p_t0_t1") or (0.0 if p_t1_t0 == 0.0 else 1.0 / p_t1_t0))

                holdings_full = (st or {}).get("holdings", {}) or {}
                syms = (holdings_full.get("symbols") or {})
                sym0 = (syms.get("token0") or "").upper()
                sym1 = (syms.get("token1") or "").upper()

                token0_is_usd = self._is_usd(sym0)
                token1_is_usd = self._is_usd(sym1)

                # Conversão token0/token1 -> USD
                if token1_is_usd:
                    # token1 é USD-like → p_t1_t0 = USD por token0
                    usd_per_t0 = p_t1_t0
                    usd_per_t1 = 1.0
                elif token0_is_usd:
                    # token0 é USD-like → p_t0_t1 = USD por token1
                    usd_per_t0 = 1.0
                    usd_per_t1 = p_t0_t1
                else:
                    # nenhum é USD: trata token1 como quote
                    usd_per_t0 = p_t1_t0
                    usd_per_t1 = 1.0

                fees_lp_usd_this_episode = (
                    delta_fee_t0_tokens * usd_per_t0 +
                    delta_fee_t1_tokens * usd_per_t1
                )

                # Rewards em USDC (1:1)
                rewards_usdc_usd_this_episode = delta_rewards_usdc

                # CAKE -> USD: usa pending_usd_est / pending_amount como preço de referência
                price_cake_usd = 0.0
                if pending_cake > 0.0 and pending_cake_usd_est > 0.0:
                    price_cake_usd = pending_cake_usd_est / pending_cake

                rewards_cake_usd_this_episode = delta_cake_tokens * price_cake_usd

                fees_this_episode_usd = (
                    fees_lp_usd_this_episode +
                    rewards_usdc_usd_this_episode +
                    rewards_cake_usd_this_episode
                )

                # -------------------------
                # 6) APR (sempre numérico)
                # -------------------------
                total_position_usd = float(in_position.get("usd") or 0.0)

                qty_candles = int(last_episode.get("last_event_bar") or 0)
                out_above_streak_total = int(last_episode.get("out_above_streak_total") or 0)
                out_below_streak_total = int(last_episode.get("out_below_streak_total") or 0)

                total_candle_out = out_above_streak_total + out_below_streak_total

                qty_candles_out_in_formula = float(qty_candles - total_candle_out)
                if qty_candles_out_in_formula <= 0.0:
                    qty_candles_out_in_formula = 1.0

                APR_daily = 0.0
                APR_annualy = 0.0
                percentage_fee_vs_position = 0.0

                if total_position_usd > 0.0:
                    percentage_fee_vs_position = fees_this_episode_usd / total_position_usd
                    APR_daily = (1440.0 / qty_candles_out_in_formula) * percentage_fee_vs_position
                    APR_annualy = APR_daily * 365.0

                # -------------------------
                # 7) Monta métricas completas
                # -------------------------
                metrics = {
                    "totals": totals,
                    "vault_idle": vault_idle,
                    "in_position": in_position,
                    "gauge_rewards": gauge_rewards,
                    "rewards_collected_cum": rewards_collected_cum,
                    "gauge_reward_balances": gauge_reward_balances,
                    "fees_uncollected": fees_uncollected,
                    "fees_collected_cum": fees_collected_cum,

                    "fees_lifetime_now": lifetime_now,
                    "fees_lifetime_baseline": baseline_for_next,
                    "fees_lifetime_prev_baseline": prev_baseline_dict,
                    "total_fees_lifetime_now": total_fees_lifetime_now,

                    "fees_this_episode_tokens": {
                        "fee_token0": delta_fee_t0_tokens,
                        "fee_token1": delta_fee_t1_tokens,
                        "rewards_cake": delta_cake_tokens,
                        "rewards_usdc": delta_rewards_usdc,
                    },
                    "fees_this_episode_usd": fees_this_episode_usd,
                    "fees_this_episode_lp_usd": fees_lp_usd_this_episode,
                    "fees_this_episode_rewards_usdc": rewards_usdc_usd_this_episode,
                    "fees_this_episode_rewards_cake_usd": rewards_cake_usd_this_episode,
                    "price_cake_usd_ref": price_cake_usd,

                    "qty_candles": qty_candles,
                    "total_candle_out": total_candle_out,
                    "qty_candles_out_in_formula": qty_candles_out_in_formula,
                    "percentage_fee_vs_position": percentage_fee_vs_position,
                    "APR_daily": APR_daily * 100.0,
                    "APR_annualy": APR_annualy * 100.0,
                }

                await self._episode_repo.update_partial(
                    last_episode_id,
                    {
                        "metrics": {
                            **metrics,
                        }
                    },
                )

                if episode_id:
                    await self._episode_repo.update_partial(
                        episode_id,
                        {
                            "metrics": {
                                "fees_lifetime_baseline": baseline_for_next,
                            }
                        },
                    )
                
                # --- Após atualizar métricas do episódio anterior, envia notificação Telegram (se configurado) ---
                if self._notifier:
                    # tentar recuperar a faixa de abertura (lower/upper) do step BATCH_REQUEST
                    lower_price = None
                    upper_price = None
                    batch_step = next((s for s in (sig.get("steps") or []) if s.get("action") == "BATCH_REQUEST"), None)
                    if batch_step:
                        payload = batch_step.get("payload") or {}
                        lower_price = payload.get("lower_price")
                        upper_price = payload.get("upper_price")

                    fees_this_episode_usd = metrics.get("fees_this_episode_usd", 0.0)
                    qty_candles = metrics.get("qty_candles", 0)
                    total_candle_out = metrics.get("total_candle_out", 0)
                    
                    apr_daily_pct = metrics.get("APR_daily", 0.0)
                    apr_annualy_pct = metrics.get("APR_annualy", 0.0)
                    fees_episode_usd = metrics.get("fees_this_episode_usd", 0.0)
                    total_position_usd = float(in_position.get("usd") or 0.0)

                    pool_type = episode.get("pool_type", None)
                    mode_on_open = episode.get("mode_on_open", None)
                    open_price = episode.get("open_price", None)
                    symbol = episode.get("symbol", None)
                     
                    msg_lines = [
                        "🚀 *Novo sinal executado*",
                        f"• DEX: {dex}",
                        f"• Vault: {alias}",
                        f"• pool_type: {pool_type}",
                        f"• mode_on_open: {mode_on_open}",
                        f"• open_price: {open_price}",
                        f"• symbol: {symbol}",
                    ]

                    if lower_price is not None and upper_price is not None:
                        msg_lines.append(f"• Faixa aberta: `{lower_price}` → `{upper_price}`")

                    msg_lines.extend(
                        [
                            "",
                            "📊 *Episódio anterior (pool fechada agora)*",
                            f"• Fees do episódio: `{fees_episode_usd:.4f} USD`",
                            f"• Posição média (usd): `{total_position_usd:.4f}`",
                            f"• APR diário: `{apr_daily_pct:.4f}%`",
                            f"• APR anualizado: `{apr_annualy_pct:.4f}%`",
                            f"• fees_this_episode_usd: `{fees_this_episode_usd:.4f}%`",
                            f"• qty_candles: `{qty_candles}%`",
                            f"• total_candle_out: `{total_candle_out}%`",
                            
                            "",
                            f"ID episódio anterior: `{last_episode_id}`",
                        ]
                    )

                    text = "\n".join(msg_lines)

                    try:
                        await self._notifier.send_message(text)
                    except Exception as notify_exc:
                        self._logger.warning(
                            "Failed to send Telegram notification for episode %s: %s",
                            last_episode_id,
                            notify_exc,
                        )
        except:
            pass
        
        return True
