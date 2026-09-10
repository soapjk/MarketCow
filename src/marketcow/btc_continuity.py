"""Fixed-size Binance stream continuity metadata; gaps never self-heal.

Raw evidence remains archived even when an update must not advance live state.
An observed trade-ID run is not proof that a disconnected interval was recovered.
"""
from decimal import Decimal, InvalidOperation
from copy import deepcopy


def integer(value):
    if type(value) is not int or value < 0:
        raise ValueError("invalid_source_integer")
    return value


def number(value, *, positive=False):
    if not isinstance(value, str):
        raise ValueError("decimal_string_required")
    try:
        result = Decimal(value)
    except InvalidOperation as exc:
        raise ValueError("invalid_decimal") from exc
    if not result.is_finite() or result < 0 or (positive and result == 0):
        raise ValueError("invalid_decimal")
    return result


class Continuity:
    def __init__(self):
        self.states = {}  # At most trade, aggTrade, 1m, 1h.

    def checkpoint(self):
        return {"schema_version": "marketcow.binance-continuity.v1",
                "states": deepcopy(self.states)}

    def restore(self, checkpoint):
        if set(checkpoint) != {"schema_version", "states"} or checkpoint["schema_version"] != "marketcow.binance-continuity.v1":
            raise ValueError("invalid_continuity_checkpoint")
        states = deepcopy(checkpoint["states"])
        if not isinstance(states, dict) or not set(states) <= {"trade", "aggTrade", "1m", "1h"}:
            raise ValueError("invalid_continuity_entities")
        for entity, state in states.items():
            if set(state) != {"version", "position", "final", "missing", "gap"}:
                raise ValueError("invalid_continuity_state")
            version = state["version"]
            if not isinstance(version, (list, tuple)) or len(version) != (2 if entity in ("1m", "1h") else 1):
                raise ValueError("invalid_continuity_version")
            state["version"] = tuple(integer(value) for value in version)
            if integer(state["position"]) != state["version"][0] or type(state["final"]) is not bool or type(state["missing"]) is not bool:
                raise ValueError("invalid_continuity_state")
            gap = state["gap"]
            if state["missing"] != (gap is not None):
                raise ValueError("invalid_continuity_gap")
            if gap is not None and (set(gap) != {"start", "end_exclusive"} or integer(gap["start"]) >= integer(gap["end_exclusive"])):
                raise ValueError("invalid_continuity_gap")
        self.states = states

    def repair_completed(self, entity, start, end):
        state = self.states.get(entity)
        gap = state.get("gap") if state else None
        if entity not in ("1m", "1h") or gap is None:
            return False
        if start <= gap["start"] and end >= gap["end_exclusive"]:
            state["missing"], state["gap"] = False, None
            return True
        return False

    def observe(self, data):
        event = data["e"]
        event_at = integer(data["E"])
        if data["s"] != "BTCUSDT":
            raise ValueError("wrong_instrument")
        if event in ("trade", "aggTrade"):
            entity = event
            position = integer(data["t" if event == "trade" else "a"])
            number(data["p"], positive=True)
            number(data["q"], positive=True)
            integer(data["T"])
            if type(data["m"]) is not bool:
                raise ValueError("invalid_maker_flag")
            version = (position,)
            final = False
            step = 1
        elif event == "kline":
            k = data["k"]
            entity = k["i"]
            if entity not in ("1m", "1h") or k["s"] != "BTCUSDT" or type(k["x"]) is not bool:
                raise ValueError("invalid_kline_identity")
            step = 60000 if entity == "1m" else 3600000
            position = integer(k["t"])
            if position % step or integer(k["T"]) != position + step - 1:
                raise ValueError("invalid_kline_window")
            o, h, low, c = (number(k[key], positive=True) for key in ("o", "h", "l", "c"))
            if not low <= min(o, c) <= max(o, c) <= h:
                raise ValueError("invalid_ohlc")
            for key in ("v", "q", "V", "Q"):
                number(k[key])
            integer(k["n"])
            final = k["x"]
            if final and event_at < position + step:
                raise ValueError("premature_final_kline")
            version = (position, event_at)
        else:
            raise ValueError("unsupported_entity")
        previous = self.states.get(entity)
        gap = None
        missing = previous["missing"] if previous else False
        apply = True
        reason = "initial_observation"
        if previous:
            if version <= previous["version"] or (event == "kline" and position == previous["position"] and previous["final"]):
                apply, reason = False, "duplicate_or_superseded"
            elif position > previous["position"] + step:
                first = previous["position"] if event == "kline" and not previous["final"] else previous["position"] + step
                gap = {"start": first, "end_exclusive": position}
                missing, reason = True, "source_gap"
            elif event == "kline" and position > previous["position"] and not previous["final"]:
                gap = {"start": previous["position"], "end_exclusive": position}
                missing, reason = True, "missing_final_bar"
            else:
                reason = "ordered_observation"
        if apply:
            accumulated = previous.get("gap") if previous else None
            if gap is not None:
                accumulated = {"start": min(gap["start"], accumulated["start"]) if accumulated else gap["start"],
                               "end_exclusive": gap["end_exclusive"]}
            self.states[entity] = dict(version=version, position=position, final=final, missing=missing, gap=accumulated)
        return {"entity": entity, "apply_to_live": apply, "reason": reason,
                "gap": self.states[entity]["gap"],
                "continuity": "gap_unresolved" if missing else "observed_segment_only"}
