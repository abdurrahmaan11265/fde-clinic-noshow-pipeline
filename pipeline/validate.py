"""Step 2: validate. Profile the raw data, apply business rules, make only safe fixes.

Hard checks stop the run: if they fail, any metric would be wrong. Soft rules move bad rows
to quarantine (kept on disk, never silently dropped) or add a flag, and every one is logged.
"""
import json
import logging

import pandas as pd

from . import config

log = logging.getLogger("pipeline.validate")


class ValidationError(RuntimeError):
    pass


def _check(results, rule, passed, detail, hard):
    results.append({"rule": rule, "passed": bool(passed), "detail": detail,
                    "severity": "hard" if hard else "soft"})
    level = logging.INFO if passed else (logging.ERROR if hard else logging.WARNING)
    log.log(level, "[%s] %s: %s", "PASS" if passed else "FAIL", rule, detail)


def profile(raw: pd.DataFrame) -> dict:
    return {
        "rows": len(raw),
        "columns": list(raw.columns),
        "nulls": {c: int(n) for c, n in raw.isna().sum().items() if n},
        "distinct": {c: int(raw[c].nunique()) for c in raw.columns},
        "no_show_values": raw["No-show"].value_counts().to_dict(),
        "handcap_values": {int(k): int(v) for k, v in raw["Handcap"].value_counts().items()},
        "age_min": int(raw["Age"].min()), "age_max": int(raw["Age"].max()),
        "appointment_time_nonzero": int((pd.to_datetime(raw["AppointmentDay"]).dt.strftime("%H:%M:%S")
                                         != "00:00:00").sum()),
    }


def validate_appointments(raw: pd.DataFrame):
    checks = []

    # ---- hard checks: structure and completeness -------------------------------------
    missing = [c for c in config.REQUIRED_COLUMNS if c not in raw.columns]
    _check(checks, "required_columns", not missing, f"missing={missing}", hard=True)
    if missing:
        raise ValidationError(f"missing columns {missing}")

    exp = config.APPOINTMENTS["expected_rows"]
    _check(checks, "row_count_matches_source", len(raw) == exp, f"{len(raw)} rows, expected {exp}", hard=True)
    dups = int(raw["AppointmentID"].duplicated().sum())
    _check(checks, "appointment_id_unique", dups == 0, f"{dups} duplicate AppointmentID", hard=True)
    bad_outcome = sorted(set(raw["No-show"]) - {"Yes", "No"})
    _check(checks, "outcome_values_known", not bad_outcome, f"unexpected No-show values={bad_outcome}", hard=True)
    failed = [c["rule"] for c in checks if c["severity"] == "hard" and not c["passed"]]
    if failed:
        raise ValidationError(f"hard checks failed: {failed}")

    # ---- safe fixes: rename, type, and state the semantics explicitly --------------------
    df = pd.DataFrame({
        "appointment_id": raw["AppointmentID"].astype("int64"),
        "patient_id": raw["PatientId"].map(lambda v: f"{int(v)}"),   # float-looking IDs -> exact string
        "gender": raw["Gender"],
        "age": raw["Age"].astype(int),
        "neighbourhood": raw["Neighbourhood"].str.strip().str.upper(),
        "scholarship": raw["Scholarship"].astype(int),        # Bolsa Família welfare enrolment
        "hypertension": raw["Hipertension"].astype(int),      # source column is misspelled
        "diabetes": raw["Diabetes"].astype(int),
        "alcoholism": raw["Alcoholism"].astype(int),
        "handicap_count": raw["Handcap"].astype(int),         # 0-4 is a count, not a yes/no flag
        "sms_received": raw["SMS_received"].astype(int),
        "no_show": (raw["No-show"] == "Yes").astype(int),     # "No" in the source means the patient came
    })
    scheduled = pd.to_datetime(raw["ScheduledDay"], utc=True)
    appt = pd.to_datetime(raw["AppointmentDay"], utc=True)
    df["scheduled_at"] = scheduled.dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    df["appointment_date"] = appt.dt.strftime("%Y-%m-%d")
    # AppointmentDay carries no time of day, so lead time is measured in calendar days.
    df["lead_days"] = (appt.dt.normalize() - scheduled.dt.normalize()).dt.days

    # ---- soft rules: quarantine impossible rows, flag doubtful ones ----------------------
    reasons = pd.Series("", index=df.index)
    neg_lead = df["lead_days"] < 0
    reasons[neg_lead] += "appointment_before_booking;"
    bad_age = ~df["age"].between(config.AGE_MIN, config.AGE_MAX)
    reasons[bad_age] += "age_out_of_range;"
    _check(checks, "appointment_not_before_booking", not neg_lead.any(),
           f"{int(neg_lead.sum())} rows quarantined", hard=False)
    _check(checks, "age_in_0_110", not bad_age.any(),
           f"{int(bad_age.sum())} rows quarantined (ages {sorted(df.loc[bad_age, 'age'].unique().tolist())})",
           hard=False)

    df["flag_multi_same_day"] = df.duplicated(["patient_id", "appointment_date"], keep=False).astype(int)
    _check(checks, "one_appointment_per_patient_per_day", not df["flag_multi_same_day"].any(),
           f"{int(df['flag_multi_same_day'].sum())} rows share patient+day; kept and flagged "
           "(could be two specialties or a double booking: unknown)", hard=False)
    weekend = pd.to_datetime(df["appointment_date"]).dt.dayofweek >= 5
    _check(checks, "appointment_on_weekday", not weekend.any(),
           f"{int(weekend.sum())} Saturday appointments; kept", hard=False)
    _check(checks, "appointment_has_time_of_day", False,
           "AppointmentDay is date-only in every row; time-of-day analysis impossible", hard=False)

    quarantined = df[reasons != ""].assign(quarantine_reason=reasons[reasons != ""])
    clean = df[reasons == ""].copy()
    log.info("clean rows=%d, quarantined=%d", len(clean), len(quarantined))
    return clean, quarantined, checks


def validate_calendar(appointments: pd.DataFrame, holidays: list, weather: dict):
    """Cross-source completeness: every weekday in the window should have appointments,
    unless a holiday explains the gap. Weather must cover every appointment date."""
    checks = []
    dates = pd.to_datetime(appointments["appointment_date"])
    window = pd.date_range(dates.min(), dates.max(), freq="D")
    present = set(dates.dt.strftime("%Y-%m-%d"))
    hol = {h["date"]: h["name"] for h in holidays if h.get("global", True)}
    weekdays_missing = [d.strftime("%Y-%m-%d") for d in window if d.dayofweek < 5
                        and d.strftime("%Y-%m-%d") not in present]
    explained = {d: hol[d] for d in weekdays_missing if d in hol}
    unexplained = [d for d in weekdays_missing if d not in hol]
    _check(checks, "weekday_gaps_explained_by_holidays", not unexplained,
           f"missing weekdays={weekdays_missing}; holiday explains {explained}; unexplained={unexplained}",
           hard=False)

    wdates = set(weather["daily"]["time"])
    uncovered = sorted(present - wdates)
    _check(checks, "weather_covers_all_dates", not uncovered, f"uncovered={uncovered}", hard=True)
    if uncovered:
        raise ValidationError(f"weather missing for {uncovered}")
    return {"weekdays_missing": weekdays_missing, "explained_by_holiday": explained,
            "unexplained": unexplained}, checks


def write_quality_report(path, prof, checks, calendar):
    path.write_text(json.dumps({"profile": prof, "checks": checks, "calendar": calendar}, indent=2))
