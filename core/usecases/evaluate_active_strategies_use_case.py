from datetime import datetime, time, timezone
import logging
from typing import Dict, List, Optional, Tuple

from adapters.external.pipeline.pipeline_http_client import PipelineHttpClient
from core.domain.entities.signal_entity import SignalEntity, SignalStep
from core.domain.entities.strategy_entity import StrategyEntity
from core.domain.entities.strategy_episode_entity import StrategyEpisodeEntity
from core.domain.enums.signal_enums import SignalStatus, SignalType

from ..services.strategy_reconciler_service import StrategyReconcilerService

from ..repositories.strategy_repository import StrategyRepository
from ..repositories.strategy_episode_repository import StrategyEpisodeRepository
from ..repositories.signal_repository import SignalRepository
from ..repositories.indicator_set_repository import IndicatorSetRepository

# LOW-VOL WINDOWS (hardcoded por enquanto; futuramente virá de Strategy.params["low_vol_keys"])
LOW_VOL_KEYS_DEFAULT = {
    (2, time(11, 0)),
    (5, time(2, 0)),
    (5, time(3, 0)),
    (5, time(4, 0)),
    (5, time(5, 0)),
    (5, time(6, 0)),
    (5, time(7, 0)),
    (5, time(8, 0)),
    (5, time(9, 0)),
    (5, time(10, 0)),
    (5, time(11, 0)),
    (5, time(12, 0)),
    (5, time(18, 0)),
    (5, time(19, 0)),
    (5, time(20, 0)),
    (5, time(21, 0)),
    (5, time(22, 0)),
    (5, time(23, 0)),
    (6, time(0, 0)),
    (6, time(1, 0)),
    (6, time(2, 0)),
    (6, time(3, 0)),
    (6, time(4, 0)),
    (6, time(5, 0)),
    (6, time(6, 0)),
    (6, time(7, 0)),
    (6, time(8, 0)),
    (6, time(9, 0)),
    (6, time(10, 0)),
    (6, time(11, 0)),
    (6, time(12, 0)),
    (6, time(20, 0)),
    (6, time(21, 0)),
}


class EvaluateActiveStrategiesUseCase:
    """
    Evaluates all ACTIVE strategies tied to a given indicator set when a new snapshot arrives.
    Applies gates similar to the backtest (breakout, high-vol, tiers + cooldown)
    and reconciles desired episode with Liquidity Provider by emitting signals (PENDING).
    """

    def __init__(
        self,
        strategy_repo: StrategyRepository,
        episode_repo: StrategyEpisodeRepository,
        signal_repo: SignalRepository,
        reconciling_service: StrategyReconcilerService,
        lp_client: PipelineHttpClient,
        logger: Optional[logging.Logger] = None,
    ):
        self._strategy_repo = strategy_repo
        self._episode_repo = episode_repo
        self._signal_repo = signal_repo
        self._reconciler = reconciling_service
        self._lp_client = lp_client
        self._logger = logger or logging.getLogger(self.__class__.__name__)

    @staticmethod
    def _trend_at(ema_fast_val: float, ema_slow_val: float) -> str:
        return "up" if ema_fast_val > ema_slow_val else "down"

    @staticmethod
    def _gate_high_vol(atr_pct: Optional[float], threshold: Optional[float]) -> bool:
        return (atr_pct is not None) and (threshold is not None) and (atr_pct > threshold)

    @staticmethod
    def _get_low_vol_keys(params: Dict, gating_enabled: bool) -> Optional[set]:
        """
        Retorna o set de (dow, time) considerado janela low-vol.

        Por enquanto:
        - se não houver gating habilitado -> None (sem efeito).
        - se houver 'low_vol_keys' em params, tentar parsear (futuro).
        - senão usa LOW_VOL_KEYS_DEFAULT (hardcoded).
        """
        if not gating_enabled:
            return None

        keys_param = params.get("low_vol_keys")
        if keys_param:
            parsed = set()
            try:
                # Espera algo como [[dow, "HH:MM"], ...] ou [[dow, hour, minute], ...]
                for item in keys_param:
                    if isinstance(item, (list, tuple)):
                        if len(item) == 2:
                            dow, t = item
                            if isinstance(t, str):
                                hh, mm = map(int, t.split(":")[:2])
                                parsed.add((int(dow), time(hh, mm)))
                        elif len(item) == 3:
                            dow, hh, mm = item
                            parsed.add((int(dow), time(int(hh), int(mm))))
                if parsed:
                    return parsed
            except Exception:
                # se der erro, cai no default
                pass

        return LOW_VOL_KEYS_DEFAULT

    @staticmethod
    def _is_in_low_vol_window(
        dt_now: datetime,
        low_vol_keys: Optional[set],
    ) -> bool:
        """
        True se (dia da semana, hora arredondada p/ hora cheia) estiver na low_vol_keys.
        """
        if not low_vol_keys:
            return False
        bucket_time = dt_now.replace(minute=0, second=0, microsecond=0)
        key = (bucket_time.weekday(), bucket_time.time())
        return key in low_vol_keys

    @staticmethod
    def _is_effectively_low_vol(
        dt_now: datetime,
        atr_pct: Optional[float],
        params: Dict,
        low_vol_keys: Optional[set],
        gating_enabled: bool,
    ) -> bool:
        """
        Considera low-vol se:
          - gating_enabled == True E
          - (estiver em uma janela estável (low_vol_keys) OU
             (low_vol_threshold definido E atr_pct < 0.0007))

        Se gating_enabled == False, sempre retorna True (comportamento antigo).
        """
        if not gating_enabled:
            return True

        in_bucket = False
        if low_vol_keys:
            in_bucket = EvaluateActiveStrategiesUseCase._is_in_low_vol_window(
                dt_now, low_vol_keys
            )

        low_vol_threshold_flag = params.get("low_vol_threshold")
        by_atr = (
            low_vol_threshold_flag is not None
            and atr_pct is not None
            and atr_pct < low_vol_threshold_flag
        )

        return in_bucket or by_atr
    
    # === Helpers de banda (clamps e largura total) ===
    @staticmethod
    def _ensure_valid_band(Pa: float, Pb: float, P: float) -> Tuple[float, float]:
        EPS_POS = 1e-12
        Pa = max(EPS_POS, float(Pa))
        Pb = max(Pa + EPS_POS, float(Pb))
        mid_pad = EPS_POS * max(1.0, float(P))
        Pa = min(P - mid_pad, Pa)
        Pb = max(P + mid_pad, Pb)
        if not (Pa < Pb):
            Pa = P - mid_pad
            Pb = P + mid_pad
        return Pa, Pb

    @staticmethod
    def _scale_to_total_width(pct_below_base: float, pct_above_base: float, total_width_pct: float) -> Tuple[float, float]:
        base_sum = pct_below_base + pct_above_base
        if base_sum <= 0:
            half = max(1e-12, total_width_pct / 2.0)
            return half, half
        scale = total_width_pct / base_sum
        return pct_below_base * scale, pct_above_base * scale

    @staticmethod
    def _is_in_range(P: float, Pa: float, Pb: float, eps: float) -> bool:
        return (P > Pa * (1.0 + eps)) and (P < Pb * (1.0 - eps))

    @staticmethod
    def _parse_pool_status(st: Dict) -> Tuple[Optional[float], Optional[float], Optional[float]]:
        """
        Returns (P_pool, Pa_pool, Pb_pool) when available/valid, else None(s).
        """
        prices = (st.get("prices", {}) or {})

        # --- current price ---
        P_pool: Optional[float] = None
        cur = prices.get("current", {}) or {}
        p_raw = cur.get("p_t1_t0", None)
        if p_raw is not None:
            p = float(p_raw)
            if p > 0.0:
                P_pool = p

        # --- range ---
        Pa_pool: Optional[float] = None
        Pb_pool: Optional[float] = None

        t_lower_raw = st.get("lower", None)
        t_upper_raw = st.get("upper", None)

        p_low_blk = prices.get("lower", {}) or {}
        p_up_blk = prices.get("upper", {}) or {}

        t_low_blk = p_low_blk.get("tick", None)
        t_up_blk = p_up_blk.get("tick", None)

        tA = t_lower_raw if t_lower_raw is not None else t_low_blk
        tB = t_upper_raw if t_upper_raw is not None else t_up_blk

        if tA is not None and tB is not None:
            tA = int(tA)
            tB = int(tB)
            t_low = min(tA, tB)
            t_up  = max(tA, tB)

            if t_low_blk is not None and int(t_low_blk) == t_low:
                pa_raw = p_low_blk.get("p_t1_t0", None)
                pb_raw = p_up_blk.get("p_t1_t0", None)
            elif t_up_blk is not None and int(t_up_blk) == t_low:
                pa_raw = p_up_blk.get("p_t1_t0", None)
                pb_raw = p_low_blk.get("p_t1_t0", None)
            else:
                pa_raw = p_low_blk.get("p_t1_t0", None)
                pb_raw = p_up_blk.get("p_t1_t0", None)

            if pa_raw is not None and pb_raw is not None:
                pa = float(pa_raw)
                pb = float(pb_raw)
                if pa > 0.0 and pb > 0.0:
                    Pa_pool, Pb_pool = (pa, pb) if pa < pb else (pb, pa)

        return P_pool, Pa_pool, Pb_pool

    async def _try_override_from_pool(
        self,
        *,
        params: Dict,
        P: float,
        Pa: float,
        Pb: float,
    ) -> Tuple[float, float, float, bool]:
        """
        Attempts to override (P,Pa,Pb) from pool status.
        Returns (P_out, Pa_out, Pb_out, overridden_flag).
        """
        dex = params.get("dex", "") or ""
        alias = params.get("alias", "") or ""

        try:
            st = await self._lp_client.get_status(dex, alias)
        except Exception as exc:
            self._logger.warning(
                "lp_client.get_status failed; keeping Binance/episode values. dex=%s alias=%s err=%s",
                dex,
                alias,
                exc,
            )
            return P, Pa, Pb, False

        if not st:
            return P, Pa, Pb, False

        try:
            p_pool, pa_pool, pb_pool = self._parse_pool_status(st)
            if p_pool is not None:
                P = p_pool
            if pa_pool is not None and pb_pool is not None:
                Pa, Pb = pa_pool, pb_pool
            return P, Pa, Pb, True
        except Exception as exc:
            self._logger.warning(
                "invalid status payload; keeping Binance/episode values. dex=%s alias=%s err=%s status=%s",
                dex,
                alias,
                exc,
                st,
            )
            return P, Pa, Pb, False
        
    async def _pick_band_for_trend_totalwidth(
        self,
        P: float,
        trend: str,
        params: Dict,
        atr_pct_now: Optional[float],
        total_width_override: Optional[float] = None,
        pool_type: Optional[str] = None,
    ) -> Tuple[float, float, str, str, bool, float, float]:
        """
        Gera (Pa,Pb) assumindo que 'max_major_side_pct' e afins são LARGURA TOTAL do range.
        """
        tiers: List[Dict] = list(params.get("tiers", []))
        
        # skew base
        if pool_type == "high_vol":
            if trend == "down":
                majority = "token1"; mode = "trend_down"
                pct_below_base = float(0.09)   # largo abaixo
                pct_above_base = float(0.01)  # curto acima
            else:
                majority = "token2"; mode = "trend_up"
                pct_below_base = float(0.01)  # curto abaixo
                pct_above_base = float(0.09)   # largo acima
        elif pool_type in [t["name"] for t in tiers]:
            # tiers usam banda simétrica no runtime, como no backtest simplificado
            if trend == "down":
                majority = "token1"; mode = "trend_down"
            else:
                majority = "token2"; mode = "trend_up"
            pct_below_base = 0.05
            pct_above_base = 0.05
        else:
            if trend == "down":
                majority = "token1"; mode = "trend_down"
                pct_below_base = float(params.get("skew_low_pct", 0.075))   # largo abaixo
                pct_above_base = float(params.get("skew_high_pct", 0.025))  # curto acima
            else:
                majority = "token2"; mode = "trend_up"
                pct_below_base = float(params.get("skew_high_pct", 0.025))  # curto abaixo
                pct_above_base = float(params.get("skew_low_pct", 0.075))   # largo acima
                
        # regime de vol (flag informativa, com threshold separado p/ tendência de baixa)
        vol_th = params.get("vol_high_threshold_pct")
        vol_th_down = params.get("vol_high_threshold_pct_down")

        if trend == "down" and vol_th_down is not None:
            high_vol = (atr_pct_now is not None) and (atr_pct_now > float(vol_th_down))
        else:
            high_vol = (atr_pct_now is not None) and (vol_th is not None) and (atr_pct_now > float(vol_th))

        # total width
        if total_width_override is not None:
            total_width_pct = float(total_width_override)
        elif pool_type == "high_vol":
            total_width_pct = float(params.get("high_vol_max_major_side_pct", 2.0))
        elif pool_type == "standard" or pool_type is None:
            total_width_pct = float(params.get("standard_max_major_side_pct", 0.05))
        elif params.get("max_major_side_pct") is not None:
            total_width_pct = float(params["max_major_side_pct"])
        else:
            total_width_pct = pct_below_base + pct_above_base

        total_width_pct = max(float(total_width_pct), 2e-6)
        pct_below, pct_above = self._scale_to_total_width(pct_below_base, pct_above_base, total_width_pct)

        Pa = P * (1.0 - pct_below)
        Pb = P * (1.0 + pct_above)
        Pa, Pb = self._ensure_valid_band(Pa, Pb, P)
        return Pa, Pb, mode, majority, high_vol, pct_below_base, pct_above_base

    # === Breakout com confirmação por streak no episódio ===
    @staticmethod
    def _update_breakout_streaks(P: float, Pa: float, Pb: float, eps: float,
                                 out_above_streak: int, out_below_streak: int,
                                 out_above_streak_total: int, out_below_streak_total: int) -> Tuple[int, int, int, int]:
        above = P > Pb * (1.0 + eps)
        below = P < Pa * (1.0 - eps)
        if above:
            return out_above_streak + 1, 0, out_above_streak_total + 1, out_below_streak_total
        if below:
            return 0, out_below_streak + 1, out_above_streak_total, out_below_streak_total + 1
        # voltou para dentro: zera só o streak, mantém os totais acumulados
        return 0, 0, out_above_streak_total, out_below_streak_total

    # ===== execute =====
    async def execute_for_indicator_snapshot(self, indicator_set: Dict, indicator_snapshot: Dict) -> None:
        symbol = indicator_snapshot["symbol"]
        P = float(indicator_snapshot["close"])
        ema_f = float(indicator_snapshot["ema_fast"])
        ema_s = float(indicator_snapshot["ema_slow"])
        atr_pct = float(indicator_snapshot["atr_pct"])
        ts = int(indicator_snapshot["ts"])
        
        created_at_iso = indicator_snapshot.get("created_at_iso")
        if created_at_iso:
            # trata o 'Z' do final como UTC
            if created_at_iso.endswith("Z"):
                created_at_iso = created_at_iso.replace("Z", "+00:00")
            dt_now = datetime.fromisoformat(created_at_iso)
        else:
            # fallback: usa ts (lembrando que está em ms)
            dt_now = datetime.fromtimestamp(ts / 1000.0, tz=timezone.utc)
            
        strategies: List[StrategyEntity] = await self._strategy_repo.get_active_by_indicator_set(
            indicator_set_id=indicator_set["cfg_hash"]
        )
        if not strategies:
            return

        for strat in strategies:
            params = strat.params
            eps = float(params.get("eps", 1e-6))
            cooloff = int(params.get("cooloff_bars", 1))
            breakout_confirm = int(params.get("breakout_confirm_bars", 1))
            inrange_mode = params.get("inrange_resize_mode", "skew_swap")
            gauge_flow_enabled = bool(params.get("gauge_flow_enabled", False))
            trend_now = self._trend_at(ema_f, ema_s)
            
            # ===== Config de LOW-VOL (janela segura) =====
            low_vol_threshold_flag = params.get("low_vol_threshold")
            # gating habilitado se qualquer uma das configs de low-vol estiver presente
            gating_enabled = bool(
                low_vol_threshold_flag is not None
            )
            low_vol_keys = self._get_low_vol_keys(params, gating_enabled)
            
            # 1) episódio atual
            strat_id = strat.name
            current = await self._episode_repo.get_open_by_strategy(strat_id)
            if current is None:
                # decide se estamos em low-vol efetivo agora
                is_low_vol_now = self._is_effectively_low_vol(
                    dt_now,
                    atr_pct,
                    params,
                    low_vol_keys,
                    gating_enabled,
                )

                if is_low_vol_now:
                    # comportamento original: abre STANDARD pela tendência
                    initial_pool_type = "standard"
                    trend_for_pick = trend_now
                    width_override = params.get("standard_max_major_side_pct")
                else:
                    # fora de low-vol: força abrir HIGH_VOL com viés de queda
                    initial_pool_type = "high_vol"
                    trend_for_pick = "down"
                    width_override = params.get(
                        "high_vol_max_major_side_pct",
                        2,
                    )
                    
                # abre primeira banda centrada pela tendência
                Pa, Pb, mode, majority, _, pct_below_base, pct_above_base = await self._pick_band_for_trend_totalwidth(
                    P, 
                    trend_for_pick, 
                    params, 
                    atr_pct,
                    total_width_override=width_override,
                    pool_type=initial_pool_type,
                )

                if majority == "token1":
                    major_pct = pct_below_base * 10
                    minor_pct = pct_above_base * 10
                else:
                    major_pct = pct_above_base * 10
                    minor_pct = pct_below_base * 10
                
                new_ep = StrategyEpisodeEntity(
                    id=f"ep_{strat_id}_{ts}",
                    strategy_id=strat_id,
                    symbol=symbol,
                    pool_type=initial_pool_type,
                    mode_on_open=mode,
                    majority_on_open=majority,
                    target_major_pct=major_pct,
                    target_minor_pct=minor_pct,
                    open_time=ts,
                    open_time_iso=indicator_snapshot.get("created_at_iso"),
                    open_price=P,
                    Pa=Pa,
                    Pb=Pb,
                    last_event_bar=0,
                    atr_streak={tier["name"]: 0 for tier in params.get("tiers", [])},
                    out_above_streak=0,
                    out_below_streak=0,
                    out_above_streak_total=0,
                    out_below_streak_total=0,
                    dex=params.get("dex"),
                    alias=params.get("alias"),
                    token0_address=params.get("token0_address"),
                    token1_address=params.get("token1_address"),
                    gauge_flow_enabled=gauge_flow_enabled,
                )
                new_ep = await self._episode_repo.open_new(new_ep)
                signal_plan = await self._reconciler.reconcile(strat_id, new_ep, symbol)
                if signal_plan:
                    signal = SignalEntity(
                        strategy_id=strat_id,
                        indicator_set_id=indicator_set["cfg_hash"],
                        cfg_hash=indicator_set["cfg_hash"],
                        symbol=symbol,
                        ts=ts,
                        signal_type=SignalType(signal_plan["signal_type"]),
                        status=SignalStatus.PENDING,
                        attempts=0,
                        steps=[
                            SignalStep(**step) for step in signal_plan["steps"]
                        ],
                        episode=new_ep,
                        last_episode=None,
                    )
                    await self._signal_repo.upsert_signal(signal)
                continue

            # defaults de campos antigos
            Pa_cur = float(current.Pa)
            Pb_cur = float(current.Pb)
            pool_type_cur = current.pool_type
            mode_on_open_cur = current.mode_on_open
            majority_on_open_cur = current.majority_on_open
            
            # ===== Guard: se estamos em high_vol trend_down FORA do low-vol window,
            # não deixamos QUALQUER sinal fechar/reabrir a pool.
            forced_high_vol_down_locked = (
                pool_type_cur == "high_vol"
                and mode_on_open_cur == "trend_down"
                and not self._is_effectively_low_vol(
                    dt_now,
                    atr_pct,
                    params,
                    low_vol_keys,
                    gating_enabled,
                )
            )
            
            i_since_open = int(current.last_event_bar) + 1
            out_above_streak = int(current.out_above_streak)
            out_below_streak = int(current.out_below_streak)
            out_above_streak_total = int(current.out_above_streak_total)
            out_below_streak_total = int(current.out_below_streak_total)
            atr_streaks: Dict = dict(current.atr_streak)
            
            trigger: Optional[str] = None
            trigger_from_periodic_pool_check = False
            
            # 2) primeiro: decide se vale a pena consultar o pool
            in_range_now = self._is_in_range(P, Pa_cur, Pb_cur, eps)

            # se Binance acusou fora do range, tenta confirmar com status da pool
            if not in_range_now:
                P, Pa_cur, Pb_cur, _ = await self._try_override_from_pool(
                    params=params,
                    P=P,
                    Pa=Pa_cur,
                    Pb=Pb_cur,
                )

                # reavalia range com os valores possivelmente corrigidos pela pool
                in_range_now = self._is_in_range(P, Pa_cur, Pb_cur, eps)
                
           # 2b) agora sim: atualiza streaks usando P/Pa/Pb "definitivos" (pool quando necessário)
            out_above_streak, out_below_streak, out_above_streak_total, out_below_streak_total = self._update_breakout_streaks(
                P, Pa_cur, Pb_cur, eps,
                out_above_streak, out_below_streak,
                out_above_streak_total, out_below_streak_total,
            )

            # Pperiodic pool truth check using i_since_open + breakout_confirm ---
            # Rule: if i_since_open % breakout_confirm == 0 -> check pool and if pool is out, trigger breakout.
            # IMPORTANT: do NOT do it when locked (forced_high_vol_down_locked).
            do_periodic_pool_check = (
                (not forced_high_vol_down_locked)
                and breakout_confirm > 0
                and (i_since_open % breakout_confirm == 0)
            )
            
            if do_periodic_pool_check:
                P_pool, Pa_pool, Pb_pool, got_pool = await self._try_override_from_pool(
                    params=params,
                    P=P,
                    Pa=Pa_cur,
                    Pb=Pb_cur,
                )
                if got_pool:
                    # If pool says OUT, behave like breakout (cross_min/cross_max)
                    if P_pool > Pb_pool * (1.0 + eps):
                        trigger = "cross_max"
                        trigger_from_periodic_pool_check = True
                        P, Pa_cur, Pb_cur = P_pool, Pa_pool, Pb_pool
                    elif P_pool < Pa_pool * (1.0 - eps):
                        trigger = "cross_min"
                        trigger_from_periodic_pool_check = True
                        P, Pa_cur, Pb_cur = P_pool, Pa_pool, Pb_pool

                    # refresh in_range_now after pool truth (for tiers logic)
                    in_range_now = self._is_in_range(P, Pa_cur, Pb_cur, eps)
                    
            # persiste os contadores mesmo sem evento
            await self._episode_repo.update_partial(current.id, {
                "out_above_streak": out_above_streak,
                "out_below_streak": out_below_streak,
                "out_above_streak_total": out_above_streak_total,
                "out_below_streak_total": out_below_streak_total,
                "last_event_bar": i_since_open
            })

            # se estiver travado em high_vol_down fora de low-vol, NÃO gera trigger nenhum
            if not forced_high_vol_down_locked:
                
                # evita que sobrescreva o trigger do do_periodic_pool_check
                if not trigger:
                    if out_above_streak >= breakout_confirm:
                        trigger = "cross_max"
                    elif out_below_streak >= breakout_confirm:
                        trigger = "cross_min"

                # 3) gate high vol (evita reabrir se já high_vol)
                if not trigger:
                    vol_th = params.get("vol_high_threshold_pct")
                    vol_th_down = params.get("vol_high_threshold_pct_down")
                    
                    # escolhe o threshold de acordo com a tendência
                    chosen_th: Optional[float] = None
                    if trend_now == "down" and vol_th_down is not None:
                        chosen_th = float(vol_th_down)
                    elif vol_th is not None:
                        chosen_th = float(vol_th)
                        
                    if (
                        atr_pct is not None
                        and chosen_th is not None
                        and atr_pct > chosen_th
                        and pool_type_cur != "high_vol"
                    ):
                        trigger = "high_vol"
                
                # flip de direção dentro de high_vol
                if not trigger and pool_type_cur == "high_vol":
                    ema_f_s_percentage = ((ema_f/ema_s)-1)*100
                    if mode_on_open_cur == "trend_down" and ema_f_s_percentage > 0.3:
                        trigger = "high_vol"
                    elif mode_on_open_cur == "trend_up" and ema_f_s_percentage < -0.3:
                        trigger = "high_vol"
            
                # 4) tiers — apenas se in-range e sem trigger ainda
                if (
                    not trigger
                    and in_range_now
                    and i_since_open >= cooloff
                ):
                    tiers_cfg: List[Dict] = list(params.get("tiers", []))
                    chosen_tier = None
                    for tier in reversed(tiers_cfg):
                        name = tier["name"]
                        allowed_from = tier.get("allowed_from", []) or []
                        if pool_type_cur == name:
                            break  # já estamos neste estreitamento
                        if pool_type_cur not in allowed_from:
                            continue
                        
                        thr_up = tier.get("atr_pct_threshold")
                        thr_down = tier.get("atr_pct_threshold_down")
                        
                        # escolhe threshold conforme tendência (igual backtest)
                        if trend_now == "down" and thr_down is not None:
                            thr = float(thr_down)
                        else:
                            thr = float(thr_up)
                            
                        bars_req = int(tier["bars_required"])
                        
                        if atr_pct is not None and atr_pct <= thr:
                            atr_streaks[name] = int(atr_streaks.get(name, 0)) + 1
                        else:
                            atr_streaks[name] = 0
                        
                        if atr_streaks[name] >= bars_req:
                            chosen_tier = tier
                            break
                        
                    await self._episode_repo.update_partial(current.id, {
                        "atr_streak": atr_streaks,
                    })

                    if chosen_tier:
                        trigger = f"tighten_{chosen_tier['name']}"

            else:
                # travado: literalmente nada de gatilho neste candle
                pass
            
            # 5) sem gatilho -> segue
            if not trigger:
                continue
            
            # override P + (Pa_cur, Pb_cur) with real price/range from pool status (best-effort; fallback to episode/Binance)
            P, Pa_cur, Pb_cur, p_from_pool = await self._try_override_from_pool(
                params=params,
                P=P,
                Pa=Pa_cur,
                Pb=Pb_cur,
            )

            # Cancel trigger only if cross range and in range inside of the position yet.
            if (
                (not trigger_from_periodic_pool_check)
                and trigger in ("cross_min", "cross_max") 
                and p_from_pool 
                and self._is_in_range(P, Pa_cur, Pb_cur, eps)
            ):
                continue

            # 6) fechar episódio atual
            now_iso = datetime.utcnow().replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
            await self._episode_repo.close_episode(
                current.id,
                {
                    "close_time": ts,
                    "close_time_iso": now_iso,
                    "close_reason": trigger,
                    "close_price": P,
                },
            )

            # garante que o current que vai em last_episode já tenha esses campos
            current.close_time = ts
            current.close_time_iso = now_iso
            current.close_reason = trigger
            current.close_price = P
            
            # helper para abrir com "total width"; aplica preserve quando aplicável
            async def _open_with_width(next_pool_type: str, total_width_override: Optional[float]) -> StrategyEpisodeEntity:
                # decide se é low-vol efetivo neste snapshot
                is_low_vol_now = self._is_effectively_low_vol(
                    dt_now,
                    atr_pct,
                    params,
                    low_vol_keys,
                    gating_enabled,
                )
                
                # se NÃO for low-vol => força abrir high_vol trend_down (igual backtest)
                if not is_low_vol_now:
                    desired_type = "high_vol"
                    trend_for_pick = "down"
                    width_key_default = "high_vol_max_major_side_pct"
                    
                    total_width_override = params.get(
                        "high_vol_max_major_side_pct",
                        params.get("standard_max_major_side_pct", 0.10),
                    )
                else:
                    desired_type = next_pool_type
                    trend_for_pick = trend_now
                    if next_pool_type == "high_vol":
                        width_key_default = "high_vol_max_major_side_pct"
                    else:
                        width_key_default = "standard_max_major_side_pct"
                        
                if total_width_override is not None:
                    total_width_pct = float(total_width_override)
                else:
                    total_width_pct = float(
                        params.get(
                            width_key_default,
                            params.get("standard_max_major_side_pct", 0.05),
                        )
                    )

                total_width_pct = max(total_width_pct, 2e-6)
                
                Pa_new, Pb_new, mode_now, majority_now, _, pct_below_base, pct_above_base = \
                    await self._pick_band_for_trend_totalwidth(
                        P,
                        trend_for_pick,
                        params,
                        atr_pct,
                        total_width_override=total_width_pct,
                        pool_type=desired_type,
                    )
                    
                if majority_now == "token1":
                    major_pct = pct_below_base*10
                    minor_pct = pct_above_base*10
                    
                else:  # majority == "token2"
                    major_pct = pct_above_base*10
                    minor_pct = pct_below_base*10
                
                return StrategyEpisodeEntity(
                    id=f"ep_{strat_id}_{ts}",
                    strategy_id=strat_id,
                    symbol=symbol,
                    pool_type=desired_type,
                    mode_on_open=mode_now,
                    majority_on_open=majority_now,
                    target_major_pct=major_pct,
                    target_minor_pct=minor_pct,
                    open_time=ts,
                    open_time_iso=indicator_snapshot.get("created_at_iso"),
                    open_price=P,
                    Pa=Pa_new,
                    Pb=Pb_new,
                    last_event_bar=0,
                    atr_streak={tier["name"]: 0 for tier in params.get("tiers", [])},
                    out_above_streak=0,
                    out_below_streak=0,
                    out_above_streak_total=0,
                    out_below_streak_total=0,
                    dex=params.get("dex"),
                    alias=params.get("alias"),
                    token0_address=params.get("token0_address"),
                    token1_address=params.get("token1_address"),
                    gauge_flow_enabled=gauge_flow_enabled,
                )

            # 7) escolher próxima pool
            new_ep: Optional[StrategyEpisodeEntity] = None
            tiers_cfg: List[Dict] = list(params.get("tiers", []))
            
            if trigger in ("cross_min", "cross_max"):
                chosen_tier = None
                for tier in reversed(tiers_cfg):
                    name = tier["name"]
                    allowed_from = tier.get("allowed_from", []) or []
                    if pool_type_cur not in allowed_from:
                        continue
                    
                    thr_up = tier.get("atr_pct_threshold")
                    thr_down = tier.get("atr_pct_threshold_down")
                    
                    if trend_now == "down" and thr_down is not None:
                        thr = float(thr_down)
                    else:
                        thr = float(thr_up)
                        
                    bars_req = int(tier["bars_required"])
                    # usa ATR do snapshot atual para confirmar
                    if atr_pct is not None and atr_pct <= thr:
                        chosen_tier = tier
                        break
                        
                if chosen_tier:
                    new_ep = await _open_with_width(
                        chosen_tier["name"],
                        float(chosen_tier["max_major_side_pct"])
                    )
                else:
                    new_ep =await  _open_with_width(
                        "standard",
                        float(params.get("standard_max_major_side_pct", 0.05))
                    )
                    
            elif trigger == "high_vol":
                new_ep = await _open_with_width("high_vol", float(params.get("high_vol_max_major_side_pct", 0.10)))
            elif trigger.startswith("tighten_"):
                chosen_tier = None
                for tier in reversed(tiers_cfg):
                    name = tier["name"]
                    allowed_from = tier.get("allowed_from", []) or []
                    if pool_type_cur not in allowed_from:
                        continue
                    
                    thr_up = tier.get("atr_pct_threshold")
                    thr_down = tier.get("atr_pct_threshold_down")
                    
                    if trend_now == "down" and thr_down is not None:
                        thr = float(thr_down)
                    else:
                        thr = float(thr_up)
                        
                    bars_req = int(tier["bars_required"])
                    if atr_pct is not None and atr_pct <= thr:
                        chosen_tier = tier
                        break
                if chosen_tier:
                    new_ep = await _open_with_width(
                        chosen_tier["name"],
                        float(chosen_tier["max_major_side_pct"])
                    )
                else:
                    new_ep = await _open_with_width(
                        "standard",
                        float(params.get("standard_max_major_side_pct", 0.05))
                    )

            if not new_ep:
                continue
            
            await self._episode_repo.open_new(new_ep)
            signal_plan = await self._reconciler.reconcile(strat_id, new_ep, symbol)
            if signal_plan:
                signal = SignalEntity(
                    strategy_id=strat_id,
                    indicator_set_id=indicator_set["cfg_hash"],
                    cfg_hash=indicator_set["cfg_hash"],
                    symbol=symbol,
                    ts=ts,
                    signal_type=SignalType(signal_plan["signal_type"]),
                    status=SignalStatus.PENDING,
                    attempts=0,
                    steps=[SignalStep(**step) for step in signal_plan["steps"]],
                    episode=new_ep,
                    last_episode=current,
                )
                await self._signal_repo.upsert_signal(signal)