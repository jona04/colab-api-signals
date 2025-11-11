import asyncio
import logging
from math import sqrt
from typing import Dict, List, Optional, Tuple

from core.repositories.strategy_episode_repository import StrategyEpisodeRepository

from ..repositories.signal_repository import SignalRepository
from adapters.external.pipeline.pipeline_http_client import PipelineHttpClient


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
        max_retries: int = 3,
        base_backoff_sec: float = 1.0,
    ):
        self._signal_repo = signal_repo
        self._episode_repo = episode_repo
        self._lp = lp_client
        self._logger = logger or logging.getLogger(self.__class__.__name__)
        self._max_retries = max_retries
        self._base_backoff = base_backoff_sec
        self.EPS_POS = 1e-12  # usado para clamps de raiz e separação Pa<P<Pb
        
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
                await self._signal_repo.mark_failure(sig, f"UNEXPECTED: {exc}")

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
                                    "request_open": {
                                        "lower_price": lower_price,
                                        "upper_price": upper_price,
                                    },
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
                                batch_res = await self._lp.post_pancake_batch_unstake_exit_swap_open(
                                    alias=alias,
                                    token_in=token_in_addr,
                                    token_out=token_out_addr,
                                    amount_in_usd=req_amount_usd if direction == "WETH->USDC" else None,
                                    amount_in=req_amount_usd if direction == "USDC->WETH" else None,
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
                                            "amount_in_usd": req_amount_usd,
                                        },
                                        "request_open": {
                                            "lower_price": lower_price,
                                            "upper_price": upper_price,
                                        },
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
                                token_out=token1_addr,
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
                    
                    # incremental backoff
                    await asyncio.sleep(self._base_backoff * (attempt + 1))

            if not success:
                # hard fail -> mark FAILED and stop this signal
                await self._signal_repo.mark_failure(sig, last_err or f"{action} failed")
                
                await self._append_log(
                    episode_id,
                    {
                        "step": action,
                        "phase": "hard_fail",
                        "error": last_err or f"{action} failed",
                    },
                )
                
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
                before = batch_res.get("before") or {}
                totals = before.get("totals") or {}
                vault_idle = before.get("vault_idle") or {}
                in_position = before.get("in_position") or {}
                total_position_usd = float(in_position.get("usd") or 0.0)
                
                gauge_rewards = before.get("gauge_rewards") or {}
                rewards_collected_cum = before.get("rewards_collected_cum") or {}
                fees_uncollected = before.get("fees_uncollected") or {}
                fees_collected_cum = before.get("fees_collected_cum") or {}

                gauge_rewards_uncollected_usd = float(gauge_rewards.get("pending_usd_est") or 0.0)
                fees_uncollected_usd = float(fees_uncollected.get("usd") or 0.0)
                total_fees = gauge_rewards_uncollected_usd + fees_uncollected_usd

                qty_candles = int(last_episode.get("last_event_bar") or 0)

                percentage_uncollected_fee = (
                    (total_fees / total_position_usd)
                    if total_position_usd > 0
                    else None
                )


                APR_daily = None
                APR_annualy = None
                if total_position_usd > 0 and total_fees > 0 and qty_candles > 0:
                    # 1440 / qty_candles ≈ number of bars per day
                    APR_daily = (1440.0 / float(qty_candles)) * (total_fees / total_position_usd)
                    # simple 12*30 ~ 360d approximation
                    APR_annualy = APR_daily * 12.0 * 30.0

                metrics = {
                    "totals": totals,
                    "vault_idle": vault_idle,
                    "in_position": in_position,
                    "gauge_rewards": gauge_rewards,
                    "rewards_collected_cum": rewards_collected_cum,
                    "fees_uncollected": fees_uncollected,
                    "fees_collected_cum": fees_collected_cum,
                    "total_fees": total_fees,
                    "qty_candles": qty_candles,
                    "percentage_uncollected_fee": percentage_uncollected_fee,
                    "APR_daily": APR_daily * 100 if APR_daily is not None else None,
                    "APR_annualy": APR_annualy * 100 if APR_annualy is not None else None,
                }

                await self._episode_repo.update_partial(
                    last_episode_id,
                    {
                        "metrics": {
                            **metrics,
                        }
                    },
                )
        except:
            pass
        
        return True
