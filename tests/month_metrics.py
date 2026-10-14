"""Inventory observations shared by the two deterministic month regressions."""
from decimal import Decimal


def inventory_metrics(trace, started_at, cfg, target_tolerance):
    def hours(at):
        return Decimal(str((at-started_at).total_seconds())) / Decimal("3600")

    recovered = {}
    rounds, active = [], None
    below_floor = 0
    for point in trace:
        at, balance, state = point["at"], point["contract_bnb"], point["state"]
        for key, threshold in (("25", cfg.thresholds.b_low), ("28", cfg.thresholds.b_alert),
                               ("target", cfg.thresholds.b_target-target_tolerance)):
            if key not in recovered and (balance > threshold if key == "target" else balance >= threshold):
                recovered[key] = hours(at)
        below_floor += "25" in recovered and balance < cfg.thresholds.b_low
        if state != "IDLE" and active is None:
            active = at
        elif state == "IDLE" and active is not None:
            rounds.append({"started_hours": hours(active), "ended_hours": hours(at),
                           "duration_hours": hours(at)-hours(active), "contract_bnb": balance})
            active = None
    return {
        "first_observed_recovery_hours": recovered,
        "completed_rounds": rounds,
        "unfinished_round": None if active is None else {
            "started_hours": hours(active), "duration_hours": hours(trace[-1]["at"])-hours(active),
            "contract_bnb": trace[-1]["contract_bnb"],
        },
        "below_floor_observations_after_initial_recovery": below_floor,
        "observation_interval_minutes": cfg.scheduler.t_check_minutes,
    }
