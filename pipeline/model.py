"""Step 3: model. Turn clean rows into business entities in SQLite.

    patient 1──* appointment *──1 calendar_day
                     │
                     ├──1 reminder   (intervention: was an SMS recorded?)
                     └──1 outcome    (did the patient attend?)

The source has one row per appointment with everything mixed together. Splitting it makes the
workflow explicit: a booking happens, a reminder may or may not follow, then an outcome.
"""
import logging
import sqlite3

import pandas as pd

from . import config

log = logging.getLogger("pipeline.model")

SCHEMA = """
DROP VIEW IF EXISTS v_appointment;
DROP VIEW IF EXISTS v_reminder_day;
DROP TABLE IF EXISTS outcome;
DROP TABLE IF EXISTS reminder;
DROP TABLE IF EXISTS appointment;
DROP TABLE IF EXISTS calendar_day;
DROP TABLE IF EXISTS patient;

CREATE TABLE patient (
    patient_id   TEXT PRIMARY KEY,
    gender       TEXT NOT NULL,
    scholarship  INTEGER NOT NULL,
    hypertension INTEGER NOT NULL,
    diabetes     INTEGER NOT NULL,
    alcoholism   INTEGER NOT NULL
);
CREATE TABLE calendar_day (
    date             TEXT PRIMARY KEY,
    weekday          INTEGER NOT NULL,     -- 0 = Monday
    holiday_name     TEXT,
    precipitation_mm REAL,
    temp_max_c       REAL
);
CREATE TABLE appointment (
    appointment_id      INTEGER PRIMARY KEY,
    patient_id          TEXT NOT NULL REFERENCES patient(patient_id),
    appointment_date    TEXT NOT NULL REFERENCES calendar_day(date),
    scheduled_at        TEXT NOT NULL,
    lead_days           INTEGER NOT NULL CHECK (lead_days >= 0),
    neighbourhood       TEXT NOT NULL,     -- where the clinic is
    age                 INTEGER NOT NULL,
    handicap_count      INTEGER NOT NULL,
    flag_multi_same_day INTEGER NOT NULL
);
CREATE TABLE reminder (
    appointment_id INTEGER PRIMARY KEY REFERENCES appointment(appointment_id),
    eligible       INTEGER NOT NULL,       -- booked far enough ahead to be reminded
    sms_received   INTEGER NOT NULL        -- only a flag: no send time, no delivery status
);
CREATE TABLE outcome (
    appointment_id INTEGER PRIMARY KEY REFERENCES appointment(appointment_id),
    no_show        INTEGER NOT NULL CHECK (no_show IN (0, 1))
);
CREATE INDEX ix_appt_date ON appointment(appointment_date);
CREATE INDEX ix_appt_patient ON appointment(patient_id);

-- Share of eligible appointments on each day that had a reminder recorded.
CREATE VIEW v_reminder_day AS
SELECT a.appointment_date,
       COUNT(*)                         AS eligible_appointments,
       AVG(r.sms_received)              AS coverage,
       AVG(r.sms_received) < {outage}   AS is_outage
FROM appointment a JOIN reminder r USING (appointment_id)
WHERE r.eligible = 1
GROUP BY a.appointment_date;

CREATE VIEW v_appointment AS
SELECT a.*, r.eligible, r.sms_received, o.no_show,
       c.weekday, c.holiday_name, c.precipitation_mm,
       COALESCE(d.is_outage, 0) AS outage_day,
       CASE WHEN a.lead_days = 0  THEN '0 same day'
            WHEN a.lead_days <= 2 THEN '1-2 days'
            WHEN a.lead_days <= 7 THEN '3-7 days'
            WHEN a.lead_days <= 14 THEN '8-14 days'
            WHEN a.lead_days <= 30 THEN '15-30 days'
            ELSE '31+ days' END AS lead_bucket
FROM appointment a
JOIN reminder r     USING (appointment_id)
JOIN outcome o      USING (appointment_id)
JOIN calendar_day c ON c.date = a.appointment_date
LEFT JOIN v_reminder_day d USING (appointment_date);
""".format(outage=config.OUTAGE_COVERAGE_THRESHOLD)


def build_calendar(appointments, holidays, weather) -> pd.DataFrame:
    dates = pd.to_datetime(appointments["appointment_date"])
    days = pd.DataFrame({"date": pd.date_range(dates.min(), dates.max(), freq="D")})
    hol = {h["date"]: h["name"] for h in holidays if h.get("global", True)}
    w = pd.DataFrame(weather["daily"]).rename(columns={
        "time": "date", "precipitation_sum": "precipitation_mm", "temperature_2m_max": "temp_max_c"})
    days["weekday"] = days["date"].dt.dayofweek
    days["date"] = days["date"].dt.strftime("%Y-%m-%d")
    days["holiday_name"] = days["date"].map(hol)
    return days.merge(w, on="date", how="left")


def build_warehouse(clean: pd.DataFrame, calendar: pd.DataFrame, db_path=config.WAREHOUSE) -> dict:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    patients = (clean.sort_values("scheduled_at")
                .groupby("patient_id", as_index=False)
                .agg(gender=("gender", "last"), scholarship=("scholarship", "max"),
                     hypertension=("hypertension", "max"), diabetes=("diabetes", "max"),
                     alcoholism=("alcoholism", "max")))
    appt_cols = ["appointment_id", "patient_id", "appointment_date", "scheduled_at", "lead_days",
                 "neighbourhood", "age", "handicap_count", "flag_multi_same_day"]
    reminder = clean[["appointment_id", "sms_received"]].assign(
        eligible=(clean["lead_days"] >= config.REMINDER_MIN_LEAD_DAYS).astype(int))

    with sqlite3.connect(db_path) as con:
        con.execute("PRAGMA foreign_keys = ON")
        con.executescript(SCHEMA)
        for name, frame in [("patient", patients), ("calendar_day", calendar),
                            ("appointment", clean[appt_cols]),
                            ("reminder", reminder[["appointment_id", "eligible", "sms_received"]]),
                            ("outcome", clean[["appointment_id", "no_show"]])]:
            frame.to_sql(name, con, if_exists="append", index=False)
        counts = {t: con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
                  for t in ["patient", "calendar_day", "appointment", "reminder", "outcome"]}
        orphans = con.execute("PRAGMA foreign_key_check").fetchall()
    log.info("warehouse tables: %s; FK violations=%d", counts, len(orphans))
    if orphans or counts["appointment"] != len(clean) or counts["outcome"] != len(clean):
        raise RuntimeError(f"warehouse integrity failed: counts={counts}, fk_violations={len(orphans)}")
    return counts
