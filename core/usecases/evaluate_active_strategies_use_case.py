import logging
from typing import Dict, List, Optional, Tuple

from ..services.strategy_reconciler_service import StrategyReconcilerService

from ..repositories.strategy_repository import StrategyRepository
from ..repositories.strategy_episode_repository import StrategyEpisodeRepository
from ..repositories.signal_repository import SignalRepository
from ..repositories.indicator_set_repository import IndicatorSetRepository


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
        logger: Optional[logging.Logger] = None,
    ):
        self._strategy_repo = strategy_repo
        self._episode_repo = episode_repo
        self._signal_repo = signal_repo
        self._reconciler = reconciling_service
        self._logger = logger or logging.getLogger(self.__class__.__name__)

    @staticmethod
    def _trend_at(ema_fast_val: float, ema_slow_val: float) -> str:
        return "up" if ema_fast_val > ema_slow_val else "down"

    @staticmethod
    def _gate_high_vol(atr_pct: Optional[float], threshold: Optional[float]) -> bool:
        return (atr_pct is not None) and (threshold is not None) and (atr_pct > threshold)

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

    def _pick_band_for_trend_totalwidth(
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
        last_tier = tiers[-1].get("name")
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
        elif pool_type in [t['name'] for t in tiers]:
            if trend == "down":
                majority = "token1"; mode = "trend_down"
                pct_below_base = float(0.05)   # largo abaixo
                pct_above_base = float(0.05)  # curto acima
            else:
                majority = "token2"; mode = "trend_up"
                pct_below_base = float(0.05)  # curto abaixo
                pct_above_base = float(0.05)   # largo acima
        else:
            if trend == "down":
                majority = "token1"; mode = "trend_down"
                pct_below_base = float(params.get("skew_low_pct", 0.075))   # largo abaixo
                pct_above_base = float(params.get("skew_high_pct", 0.025))  # curto acima
            else:
                majority = "token2"; mode = "trend_up"
                pct_below_base = float(params.get("skew_high_pct", 0.025))  # curto abaixo
                pct_above_base = float(params.get("skew_low_pct", 0.075))   # largo acima
                
        # regime de vol (flag informativa)
        vol_th = params.get("vol_high_threshold_pct")
        high_vol = (atr_pct_now is not None and vol_th is not None and atr_pct_now > float(vol_th))

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
                                 out_above_streak: int, out_below_streak: int) -> Tuple[int, int]:
        above = P > Pb * (1.0 + eps)
        below = P < Pa * (1.0 - eps)
        if above:
            return out_above_streak + 1, 0
        if below:
            return 0, out_below_streak + 1
        # voltou para dentro
        return 0, 0

    # ===== execute =====
    async def execute_for_snapshot(self, indicator_set: Dict, snapshot: Dict) -> None:
        symbol = snapshot["symbol"]
        P = float(snapshot["close"])
        ema_f = float(snapshot["ema_fast"])
        ema_s = float(snapshot["ema_slow"])
        atr_pct = float(snapshot["atr_pct"])
        ts = int(snapshot["ts"])

        strategies = await self._strategy_repo.get_active_by_indicator_set(indicator_set_id=indicator_set["cfg_hash"])
        if not strategies:
            return

        for strat in strategies:
            params = strat["params"]
            eps = float(params.get("eps", 1e-6))
            cooloff = int(params.get("cooloff_bars", 1))
            breakout_confirm = int(params.get("breakout_confirm_bars", 1))
            inrange_mode = params.get("inrange_resize_mode", "skew_swap")
            gauge_flow_enabled = bool(params.get("gauge_flow_enabled", False))
            
            # 1) episódio atual
            strat_id = strat["name"]
            current = await self._episode_repo.get_open_by_strategy(strat_id)
            if current is None:
                # abre primeira banda centrada pela tendência
                Pa, Pb, mode, majority, _, pct_below_base, pct_above_base = self._pick_band_for_trend_totalwidth(
                    P, self._trend_at(ema_f, ema_s), params, atr_pct, total_width_override=params.get("standard_max_major_side_pct"), pool_type="standard"
                )
                
                if majority == "token1":
                    major_pct = pct_below_base*10
                    minor_pct = pct_above_base*10
                    
                else:  # majority == "token2"
                    major_pct = pct_above_base*10
                    minor_pct = pct_below_base*10
                
                new_ep = {
                    "_id": f"ep_{strat_id}_{ts}",
                    "strategy_id": strat_id,
                    "symbol": symbol,
                    "pool_type": "standard",
                    "mode_on_open": mode,
                    "majority_on_open": majority,
                    "target_major_pct": major_pct,  # ex: 0.90 ou 0.75
                    "target_minor_pct": minor_pct,
                    "open_time": ts,
                    "open_time_iso": snapshot.get("created_at_iso", None),
                    "open_price": P,
                    "Pa": Pa, "Pb": Pb,
                    "last_event_bar": 0,
                    "atr_streak": {tier["name"]: 0 for tier in params.get("tiers", [])},
                    "out_above_streak": 0,
                    "out_below_streak": 0,
                    "dex": params.get("dex"),
                    "alias": params.get("alias"),
                    "token0_address": params.get("token0_address"),
                    "token1_address": params.get("token1_address"),
                    "gauge_flow_enabled": gauge_flow_enabled
                }
                await self._episode_repo.open_new(new_ep)
                signal_plan = await self._reconciler.reconcile(strat_id, new_ep, symbol)
                if signal_plan:
                    await self._signal_repo.upsert_signal({
                        "strategy_id": strat_id,
                        "indicator_set_id": indicator_set["cfg_hash"],
                        "cfg_hash": indicator_set["cfg_hash"],
                        "symbol": symbol,
                        "ts": ts,
                        "signal_type": signal_plan["signal_type"],
                        "steps": signal_plan["steps"],
                        "episode": signal_plan["episode"], 
                        "status": "PENDING",
                        "attempts": 0,
                    })
                continue

            # defaults de campos antigos
            Pa_cur = float(current.get("Pa"))
            Pb_cur = float(current.get("Pb"))
            i_since_open = int(current.get("last_event_bar", 0)) + 1
            out_above_streak = int(current.get("out_above_streak", 0))
            out_below_streak = int(current.get("out_below_streak", 0))
            pool_type_cur = current.get("pool_type", "standard")
            mode_on_open_cur = current.get("mode_on_open", "")
            
            trigger: Optional[str] = None

            # 2) atualiza streaks de breakout e verifica confirmação
            out_above_streak, out_below_streak = self._update_breakout_streaks(
                P, Pa_cur, Pb_cur, eps, out_above_streak, out_below_streak
            )
            # persiste os contadores mesmo sem evento
            await self._episode_repo.update_partial(current["_id"], {
                "out_above_streak": out_above_streak,
                "out_below_streak": out_below_streak,
                "last_event_bar": i_since_open
            })

            if (i_since_open >= cooloff) and (
                out_above_streak >= breakout_confirm or out_below_streak >= breakout_confirm
            ):
                trigger = "cross_max" if out_above_streak >= breakout_confirm else "cross_min"

            # 3) gate high vol (evita reabrir se já high_vol)
            if not trigger and (i_since_open >= cooloff):
                vol_th = params.get("vol_high_threshold_pct")
                if (atr_pct is not None and vol_th is not None and atr_pct > float(vol_th)) and pool_type_cur != "high_vol":
                    trigger = "high_vol"
            
            # 3.1) Reabre high vol do lado certo
            if pool_type_cur == "high_vol" and mode_on_open_cur == "trend_down" and ema_f - ema_s > 10:
                trigger = "high_vol"
            if pool_type_cur == "high_vol" and mode_on_open_cur == "trend_up" and ema_f - ema_s < -10:
                trigger = "high_vol"
            
            # 4) tiers — apenas se in-range e sem trigger ainda
            if not trigger and (Pa_cur < P < Pb_cur) and (i_since_open >= cooloff):
                tiers: List[Dict] = list(params.get("tiers", []))
                tiers.sort(key=lambda t: t["atr_pct_threshold"])
                streaks = current.get("atr_streak", {})
                chosen = None
                for tier in tiers:
                    if pool_type_cur == tier["name"]:
                        break
                    if pool_type_cur not in tier.get("allowed_from", []) and pool_type_cur != tier["name"]:
                        continue
                    # atualiza streak
                    thr = float(tier["atr_pct_threshold"])
                    name = tier["name"]
                    
                    streaks[name] = int(streaks.get(name, 0)) + 1 if (atr_pct is not None and atr_pct <= thr) else 0
                    if streaks[name] >= int(tier["bars_required"]):
                        chosen = tier
                        break
                if chosen:
                    trigger = f"tighten_{chosen['name']}"
                # persiste streaks (mesmo sem trigger)
                await self._episode_repo.update_partial(current["_id"], {"atr_streak": streaks})

            # 5) sem gatilho → segue
            if not trigger:
                continue

            # 6) fechar episódio atual
            await self._episode_repo.close_episode(
                current["_id"],
                {
                    "close_time": ts,
                    "close_time_iso": snapshot.get("created_at_iso", None),
                    "close_reason": trigger,
                    "close_price": P,
                },
            )

            trend_now = self._trend_at(ema_f, ema_s)

            # helper para abrir com "total width"; aplica preserve quando aplicável
            def _open_with_width(next_pool_type: str, total_width_override: Optional[float]):
                # decide total width alvo
                total_width_pct = (
                    float(total_width_override) if total_width_override is not None
                    else (float(params.get("high_vol_max_major_side_pct")) if next_pool_type == "high_vol"
                          else float(params.get("standard_max_major_side_pct")))
                )
                Pa_new, Pb_new, mode_now, majority_now, _, pct_below_base, pct_above_base = self._pick_band_for_trend_totalwidth(
                    P, trend_now, params, atr_pct, total_width_override=total_width_pct, pool_type=next_pool_type
                )
                    
                if majority_now == "token1":
                    major_pct = pct_below_base*10
                    minor_pct = pct_above_base*10
                    
                else:  # majority == "token2"
                    major_pct = pct_above_base*10
                    minor_pct = pct_below_base*10
                
                return {
                    "_id": f"ep_{strat_id}_{ts}",
                    "strategy_id": strat_id,
                    "symbol": symbol,
                    "pool_type": next_pool_type,
                    "mode_on_open": mode_now,
                    "majority_on_open": majority_now,
                    "target_major_pct": major_pct,  # ex: 0.90 ou 0.75
                    "target_minor_pct": minor_pct,
                    "open_time": ts,
                    "open_time_iso": snapshot.get("created_at_iso", None),
                    "open_price": P,
                    "Pa": Pa_new, "Pb": Pb_new,
                    "last_event_bar": 0,
                    "atr_streak": {tier["name"]: 0 for tier in params.get("tiers", [])},
                    "out_above_streak": 0,
                    "out_below_streak": 0,
                    "dex": params.get("dex"),
                    "alias": params.get("alias"),
                    "token0_address": params.get("token0_address"),
                    "token1_address": params.get("token1_address"),
                    "gauge_flow_enabled": gauge_flow_enabled
                }

            # 7) escolher próxima pool
            new_ep = None
            if trigger in ("cross_min", "cross_max"):
                tiers = params.get("tiers", [])
                if tiers:
                    tiers_sorted = sorted(tiers, key=lambda t: t["atr_pct_threshold"])
                    for tier in reversed(tiers_sorted):  # mais estreito primeiro
                        new_ep = _open_with_width(tier["name"], float(tier["max_major_side_pct"]))
                        break
                if new_ep is None:
                    new_ep = _open_with_width("standard", float(params.get("standard_max_major_side_pct", 0.05)))
            elif trigger == "high_vol":
                new_ep = _open_with_width("high_vol", float(params.get("high_vol_max_major_side_pct", 0.10)))
            elif trigger.startswith("tighten_"):
                tier_name = trigger.split("_", 1)[1]
                tier = next((t for t in params.get("tiers", []) if t["name"] == tier_name), None)
                width = float(tier["max_major_side_pct"]) if tier else float(params.get("standard_max_major_side_pct", 0.05))
                new_ep = _open_with_width(tier_name if tier else "standard", width)

            if new_ep:
                await self._episode_repo.open_new(new_ep)
                signal_plan = await self._reconciler.reconcile(strat_id, new_ep, symbol)
                if signal_plan:
                    await self._signal_repo.upsert_signal({
                        "strategy_id": strat_id,
                        "indicator_set_id": indicator_set["cfg_hash"],
                        "cfg_hash": indicator_set["cfg_hash"],
                        "symbol": symbol,
                        "ts": ts,
                        "signal_type": signal_plan["signal_type"],
                        "steps": signal_plan["steps"],
                        "episode": signal_plan["episode"], 
                        "status": "PENDING",
                        "attempts": 0,
                    })