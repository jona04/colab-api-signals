import asyncio
import json
import logging
from math import sqrt
from typing import Dict, List, Optional, Tuple

from pydantic import Field

from adapters.external.notify.telegram_notifier import TelegramNotifier
from core.common.utils import sanitize_for_bson
from core.domain.entities.signal_entity import SignalEntity
from core.repositories.strategy_episode_repository import StrategyEpisodeRepository
from core.services.idempotency_key_service import IdempotencyKeyService

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
        idempotency_service: Optional[IdempotencyKeyService] = None,
        max_parallel: int = 4,
    ):
        self._signal_repo = signal_repo
        self._episode_repo = episode_repo
        self._lp = lp_client
        self._logger = logger or logging.getLogger(self.__class__.__name__)
        self._max_retries = max_retries
        self._base_backoff = base_backoff_sec
        self._notifier = notifier
        self._idempotency = idempotency_service or IdempotencyKeyService()
        self._locks: dict[str, asyncio.Lock] = Field(default_factory=dict)

        self._locks_lock = asyncio.Lock()  # pra criar locks de forma segura
        self._max_parallel = max_parallel
        self._semaphore = asyncio.Semaphore(self._max_parallel)
        self.EPS_POS = 1e-12  # usado para clamps de raiz e separação Pa<P<Pb
    
    def _build_idempotency_key(self, signal: SignalEntity, action: str) -> str:
        return self._idempotency.build_for_signal_step(signal, action)
    
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
        pending: List[SignalEntity] = await self._signal_repo.list_pending(limit=50)
        if not pending:
            return
        
        tasks = []
        for sig in pending:
            tasks.append(self._run_single_with_locks(sig))
        
        # executa tudo em paralelo, respeitando semaphore global
        await asyncio.gather(*tasks, return_exceptions=False)
    
    async def _run_single_with_locks(self, sig: SignalEntity) -> None:
        """
        Envolve _process_single_signal com:
          - semaphore global (max_parallel)
          - lock por vault (dex+alias)
        """
        episode = sig.episode
        dex = episode.dex
        alias = episode.alias

        vault_lock = await self._get_vault_lock(dex, alias)

        async with self._semaphore:
            async with vault_lock:
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
                        sig_id = sig.id

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
    
    async def _get_vault_lock(self, dex: Optional[str], alias: Optional[str]) -> asyncio.Lock:
        """
        Retorna um asyncio.Lock específico pra combinação (dex, alias).

        - Garante que nunca existam duas pipelines concorrentes pro MESMO vault.
        - Sinais de vaults diferentes rodam em paralelo (ETH vs BTC, etc.).
        - Se dex/alias vierem None, usamos um placeholder "?" só pra ter chave estável.
        """
        key = f"{dex or '?'}:{alias or '?'}"

        # garante criação thread-safe/async-safe do lock
        async with self._locks_lock:
            lock = self._locks.get(key)
            if lock is None:
                lock = asyncio.Lock()
                self._locks[key] = lock
            return lock

    async def _append_log(
        self,
        episode_id: Optional[str],
        base: Dict,
    ) -> None:
        """
        Helper: push a log line into the episode doc, if we have an episode_id.

        IMPORTANT:
        - Mongo only supports int64. Onchain payloads often contain huge ints (sqrtPriceX96 etc).
        - sanitize_for_bson must be applied to avoid BSON int overflow.
        """
        if not episode_id:
            return
        try:
            payload = sanitize_for_bson(base)
            await self._episode_repo.append_execution_log(episode_id, payload)
        except Exception as log_exc:
            self._logger.warning(
                "Failed to append_execution_log for %s: %s",
                episode_id,
                log_exc,
            )


    async def _process_single_signal(self, sig: SignalEntity) -> bool:
        """
        Executes a single signal's steps sequentially.
        Returns True on full success, False if FAILED.
        """
        steps = [s.model_dump(mode="python") for s in sig.steps]
        
        last_episode = sig.last_episode.model_dump(mode="python") if sig.last_episode else {}
        last_episode_id = last_episode.get("id") or last_episode.get("_id")

        episode = sig.episode.model_dump(mode="python")
        episode_id = episode.get("id") or episode.get("_id")

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
            
            idem_key = self._build_idempotency_key(sig, action)
            
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
                        bal_in_tokens = amt1 if (token_in_addr and token_in_addr.lower() == (t1_addr or "").lower()) else amt0
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
                            idempotency_key=idem_key,
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
                            res = await self._lp.post_unstake(dex, alias, idempotency_key=idem_key,)
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
                            res = await self._lp.post_collect(dex, alias, idempotency_key=idem_key,)
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
                        st = await self._lp.get_status(dex, alias, idempotency_key=idem_key)
                        if not st:
                            raise RuntimeError("status_unavailable_before_swap")
                        
                        position_location = st.get("position_location", None)
                        if position_location == "pool":
                            # always withdraw mode "pool" to bring capital back idle
                            res = await self._lp.post_withdraw(dex, alias, mode="pool", idempotency_key=idem_key,)
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
                        st = await self._lp.get_status(dex, alias, idempotency_key=idem_key)
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
                                convert_gauge_to_usdc=True,
                                idempotency_key=idem_key,
                            )
                                
                            await self._append_log(
                                episode_id,
                                {
                                    "step": action,
                                    "phase": "swap_call",
                                    "attempt": attempt + 1,
                                    "request": {
                                        "token_in": reward_token,
                                        "token_out": token_out_addr,
                                        "amount_in": reward_in_vault_to_swap,
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
                        st = await self._lp.get_status(dex, alias, idempotency_key=idem_key)
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
                            req_amount_usd = req_amount_usd - 0.00001
                            
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
                                    pool_override="WETH_USDC" if dex == "pancake" else None,
                                    idempotency_key=idem_key,
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
                            idempotency_key=idem_key,
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
                        st = await self._lp.get_status(dex, alias, idempotency_key=idem_key)
                        if not st:
                            raise RuntimeError("status_unavailable_before_stake")

                        if bool(st.get("has_gauge")) and (st.get("position_location") != "gauge"):
                            # token_id é opcional; o provider pode resolver internamente
                            token_id = step.get("payload", {}).get("token_id")
                            res = await self._lp.post_stake(dex, alias, token_id=token_id, idempotency_key=idem_key)
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
                    sig_id = sig.id
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
        
        # ==========================
        #  BLOCO DE MÉTRICAS + TELEGRAM
        # ==========================
        try:
            if batch_res and last_episode_id and last_episode:
                after = batch_res.get("after") or {}
                before = batch_res.get("before") or {}
                snapshot = after  # estado após unstake/exit/swap/open

                # --------- snapshots de estado ---------
                totals = snapshot.get("totals") or {}
                vault_idle = snapshot.get("vault_idle") or {}
                in_position = snapshot.get("in_position") or {}

                # CAKE: sempre usar BEFORE (como você já fazia)
                gauge_rewards = before.get("gauge_rewards") or {}
                # rewards e fees: usar AFTER
                rewards_collected_cum = snapshot.get("rewards_collected_cum") or {}
                fees_uncollected = snapshot.get("fees_uncollected") or {}
                fees_collected_cum = snapshot.get("fees_collected_cum") or {}
                gauge_reward_balances = snapshot.get("gauge_reward_balances") or {}
                
                fees_uncollected_st = {}
                fees_uncollected_st_usd = 0
                gauge_rewards_st_usd = 0
                if st:
                    fees_uncollected_st = st.get("fees_uncollected") or {}
                    fees_uncollected_st_usd = fees_uncollected_st.get("usd", 0)
                    gauge_rewards_st = st.get("gauge_rewards") or {}
                    gauge_rewards_st_usd = gauge_rewards_st.get("pending_usd_est", 0)
                
                
                # -------------------------
                # 1) Lifetime atual (fechamento da pool anterior) em tokens
                # -------------------------
                pending_cake = float(gauge_rewards.get("pending_amount") or 0.0)
                pending_cake_usd_est = float(gauge_rewards.get("pending_usd_est") or 0.0)

                # -------------------------
                # 5) Conversão dos deltas -> USD (incluindo CAKE)
                # -------------------------
                prices = (snapshot.get("prices") or {})
                p_t1_t0 = float(prices.get("p_t1_t0") or 0.0)
                p_t0_t1 = float(prices.get("p_t0_t1") or (0.0 if p_t1_t0 == 0.0 else 1.0 / p_t1_t0))

                # busca status completo para descobrir quais tokens são USD-like
                st_for_prices = await self._lp.get_status(dex, alias)
                holdings_full = (st_for_prices or {}).get("holdings", {}) or {}
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

                # CAKE -> USD: usa pending_usd_est / pending_amount como preço de referência (BEFORE)
                price_cake_usd = 0.0
                if pending_cake > 0.0 and pending_cake_usd_est > 0.0:
                    price_cake_usd = pending_cake_usd_est / pending_cake

                # -------------------------
                # 6) APR (sempre numérico, APR em fração + em %)
                # -------------------------
                total_position_usd = float(in_position.get("usd") or 0.0)

                qty_candles = int(last_episode.get("last_event_bar") or 0)
                out_above_streak_total = int(last_episode.get("out_above_streak_total") or 0)
                out_below_streak_total = int(last_episode.get("out_below_streak_total") or 0)
                total_candle_out = out_above_streak_total + out_below_streak_total

                qty_candles_in_formula = float(qty_candles - total_candle_out)
                if qty_candles_in_formula <= 0.0:
                    qty_candles_in_formula = 1.0

                APR_daily = 0.0
                APR_annualy = 0.0
                percentage_fee_vs_position = 0.0

                fees_this_episode_usd=fees_uncollected_st_usd+gauge_rewards_st_usd
                if total_position_usd > 0.0 and fees_this_episode_usd > 0.0:
                    
                    percentage_fee_vs_position = fees_this_episode_usd / total_position_usd
                    APR_daily = (1440.0 / qty_candles_in_formula) * percentage_fee_vs_position
                    APR_annualy = APR_daily * 365.0
                    
                APR_daily_pct = APR_daily * 100.0
                APR_annualy_pct = APR_annualy * 100.0
                
                
                # -------------------------
                # 7) Metadados de episódio (anterior x atual)
                # -------------------------
                prev_open_ts = (
                    last_episode.get("open_time_iso")
                    or last_episode.get("created_at_iso")
                )
                prev_close_ts = (
                    last_episode.get("close_time_iso")
                    or last_episode.get("close_time")
                )

                cur_open_ts = (
                    episode.get("open_time_iso")
                    or episode.get("created_at_iso")
                )

                # ranges anteriores e atuais (Pa/Pb são o range real do episódio)
                prev_lower = last_episode.get("Pa")
                prev_upper = last_episode.get("Pb")

                cur_lower = episode.get("Pa") or episode.get("lower_price") or episode.get("range_lower")
                cur_upper = episode.get("Pb") or episode.get("upper_price") or episode.get("range_upper")

                # labels reais do teu modelo (standard/high_vol/tier, up/down, majority)
                prev_labels = {
                    "pool_type": last_episode.get("pool_type"),
                    "mode_on_open": last_episode.get("mode_on_open"),
                    "majority_on_open": last_episode.get("majority_on_open"),
                    "target_major_pct": last_episode.get("target_major_pct"),
                    "target_minor_pct": last_episode.get("target_minor_pct"),
                    "open_price": (last_episode.get("open_price") or 0.0)
                }

                cur_labels = {
                    "pool_type": episode.get("pool_type"),
                    "mode_on_open": episode.get("mode_on_open"),
                    "majority_on_open": episode.get("majority_on_open"),
                    "target_major_pct": episode.get("target_major_pct"),
                    "target_minor_pct": episode.get("target_minor_pct"),
                    "open_price": (episode.get("open_price") or 0.0)
                }

                episode_meta = {
                    "dex": dex,
                    "alias": alias,
                    "episode_id": episode_id,
                    "last_episode_id": last_episode_id,
                    "prev_open_ts": prev_open_ts,
                    "prev_close_ts": prev_close_ts,
                    "cur_open_ts": cur_open_ts,
                    "prev_range": {
                        "Pa": prev_lower,
                        "Pb": prev_upper,
                    },
                    "cur_range": {
                        "Pa": cur_lower,
                        "Pb": cur_upper,
                    },
                    "prev_labels": prev_labels,
                    "cur_labels": cur_labels,
                }
       
                # -------------------------
                # 8) Monta métricas completas
                # -------------------------
                metrics = {
                    "episode_meta": episode_meta,

                    "totals": totals,
                    "vault_idle": vault_idle,
                    "in_position": in_position,
                    "gauge_rewards": gauge_rewards,
                    "rewards_collected_cum": rewards_collected_cum,
                    "gauge_reward_balances": gauge_reward_balances,
                    "fees_uncollected": fees_uncollected,
                    "fees_collected_cum": fees_collected_cum,

                    "fees_uncollected_st_usd": fees_uncollected_st_usd,
                    "gauge_rewards_st_usd": gauge_rewards_st_usd,
                    "fees_this_episode_usd": fees_this_episode_usd,
                    "price_cake_usd_ref": price_cake_usd,
                    
                    "qty_candles": qty_candles,
                    "total_candle_out": total_candle_out,
                    "qty_candles_in_formula": qty_candles_in_formula,
                    "percentage_fee_vs_position": percentage_fee_vs_position,
                    "APR_daily": APR_daily,
                    "APR_annualy": APR_annualy,
                    "APR_daily_pct": APR_daily_pct,
                    "APR_annualy_pct": APR_annualy_pct,
                }

                metrics_safe = sanitize_for_bson(metrics)
                
                # persiste métricas no episódio anterior (fechado)
                await self._episode_repo.update_partial(
                    last_episode_id,
                    {
                        "metrics": metrics_safe
                    },
                )

                # -------------------------
                # 9) Notificação Telegram
                # -------------------------
                if getattr(self, "_notifier", None) is not None:
                    lines: List[str] = []

                    # HEADER
                    lines.append("")
                    lines.append("")
                    lines.append("**LP episode fechado e nova posição aberta**")
                    lines.append(f"Dex/Alias: {dex}/{alias}")
                    lines.append(f"Open Episódio anterior: {prev_labels.get('open_price')}")
                    lines.append(f"Open Episódio atual: {cur_labels.get('open_price')}")
                    lines.append("")

                    # =========================
                    # EPISÓDIO ANTERIOR
                    # =========================
                    lines.append("**📌 Episódio anterior (pool fechada)**")
                    lines.append(f"Abertura: {prev_open_ts}")
                    lines.append(f"Fechamento: {prev_close_ts}")
                    lines.append(f"Range preços: Pa={prev_lower}, Pb={prev_upper}")
                    lines.append("**Configuração da pool:**")
                    lines.append(f"• Tipo de pool: {prev_labels.get('pool_type')}")
                    lines.append(f"• Direção (tendência): {prev_labels.get('mode_on_open')}")
                    lines.append("")

                    # =========================
                    # NOVO EPISÓDIO
                    # =========================
                    lines.append("**📌 Novo episódio (pool aberta)**")
                    lines.append(f"Abertura: {cur_open_ts}")
                    lines.append(f"Range preços: Pa={cur_lower}, Pb={cur_upper}")
                    lines.append("**Configuração da pool:**")
                    lines.append(f"• Tipo de pool: {cur_labels.get('pool_type')}")
                    lines.append(f"• Direção (tendência): {cur_labels.get('mode_on_open')}")
                    lines.append("")

                    # =========================
                    # MÉTRICAS DA POOL ANTERIOR
                    # =========================
                    lines.append("**📈 Métricas – Episódio encerrado**")
                    lines.append(f"• Fees LP uncollected: {fees_uncollected_st_usd:.8f}")
                    lines.append(f"• Rewards em USDC: {gauge_rewards_st_usd:.8f}")
                    lines.append(f"• Total fees do episódio (USD): {fees_this_episode_usd:.6f}")
                    lines.append(f"• APR diário aproximado: {APR_daily_pct:.4f}%")
                    lines.append(f"• APR anualizado aproximado: {APR_annualy_pct:.4f}%")
                    lines.append("")

                    # =========================
                    # PAINEL APR / CANDLES
                    # =========================
                    lines.append("**🧮 Painel APR (inputs)**")
                    lines.append(f"• Posição USD no fechamento: {total_position_usd:.6f}")
                    lines.append(f"• Nº total de candles: {qty_candles}")
                    lines.append(f"• Candles fora da pool: {total_candle_out}")
                    lines.append(f"• Candles válidos p/ APR: {qty_candles_in_formula:.2f}")
                    lines.append(f"• Percentual fees/posição: {(percentage_fee_vs_position * 100):.4f}%")
                    lines.append("")

                    # envia
                    text = "\n".join(lines)
                    await self._notifier.send_message(text)


        except Exception as exc:
            # nunca quebrar o fluxo por causa de métrica/telegram
            self._logger.warning("Falha ao calcular métricas ou enviar Telegram: %s", exc)

        return True