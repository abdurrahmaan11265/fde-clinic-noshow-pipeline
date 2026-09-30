"""Step 4: metrics. Every number in the brief comes from a named SQL query below.

Definitions (stated before calculating, as they change the answer):
  no-show            patient did not attend a booked appointment (source "No-show" = "Yes")
  eligible           booked 3+ calendar days ahead; nobody booked sooner ever got an SMS
  reminder coverage  share of eligible appointments with sms_received = 1
  outage day         appointment date where coverage among eligible appointments is below 5%
"""
import csv
import json
import logging
import sqlite3

from . import config

log = logging.getLogger("pipeline.metrics")

QUERIES = {
    # Project KPI (lagging): no-show rate for advance bookings, the group reminders exist for.
    "kpi_advance_no_show": """
        SELECT COUNT(*) AS appointments, SUM(no_show) AS no_shows, AVG(no_show) AS rate
        FROM v_appointment WHERE eligible = 1""",

    "overall": """
        SELECT COUNT(*) AS appointments, SUM(no_show) AS no_shows, AVG(no_show) AS rate,
               COUNT(DISTINCT patient_id) AS patients, COUNT(DISTINCT appointment_date) AS days,
               COUNT(DISTINCT neighbourhood) AS clinics,
               MIN(appointment_date) AS first_day, MAX(appointment_date) AS last_day
        FROM v_appointment""",

    # Guardrail: same-day bookings never get reminders, so any change there is not our doing.
    "guardrail_same_day": """
        SELECT COUNT(*) AS appointments, AVG(no_show) AS rate
        FROM v_appointment WHERE lead_days = 0""",

    "by_lead_bucket": """
        SELECT lead_bucket, COUNT(*) AS appointments, AVG(no_show) AS no_show_rate,
               AVG(sms_received) AS sms_rate
        FROM v_appointment GROUP BY lead_bucket ORDER BY MIN(lead_days)""",

    # The misleading headline number: compares groups with very different lead times.
    "naive_sms": """
        SELECT sms_received, COUNT(*) AS appointments, AVG(no_show) AS no_show_rate
        FROM v_appointment GROUP BY sms_received""",

    # Same comparison within lead-time bands, normal days only.
    "sms_within_lead": """
        SELECT lead_bucket, sms_received, COUNT(*) AS appointments, AVG(no_show) AS no_show_rate
        FROM v_appointment WHERE eligible = 1 AND outage_day = 0
        GROUP BY lead_bucket, sms_received ORDER BY MIN(lead_days), sms_received""",

    "coverage": """
        SELECT COUNT(*) AS eligible, SUM(sms_received) AS reminded, AVG(sms_received) AS coverage,
               SUM(CASE WHEN sms_received = 0 THEN 1 ELSE 0 END) AS not_reminded,
               SUM(CASE WHEN outage_day = 1 THEN 1 ELSE 0 END) AS on_outage_days,
               SUM(CASE WHEN outage_day = 0 AND sms_received = 0 THEN 1 ELSE 0 END) AS missed_normal_days,
               AVG(CASE WHEN outage_day = 0 THEN sms_received END) AS coverage_normal_days
        FROM v_appointment WHERE eligible = 1""",

    "coverage_by_day": """
        SELECT d.appointment_date, d.eligible_appointments, d.coverage, d.is_outage, c.weekday
        FROM v_reminder_day d JOIN calendar_day c ON c.date = d.appointment_date
        ORDER BY d.appointment_date""",

    # Natural experiment: outage days vs normal days, eligible vs never-eligible (control).
    "outage_did": """
        SELECT eligible, outage_day, COUNT(*) AS appointments, AVG(no_show) AS no_show_rate
        FROM v_appointment GROUP BY eligible, outage_day ORDER BY eligible, outage_day""",

    "outage_by_lead": """
        SELECT lead_bucket, outage_day, COUNT(*) AS appointments, AVG(no_show) AS no_show_rate
        FROM v_appointment GROUP BY lead_bucket, outage_day ORDER BY MIN(lead_days), outage_day""",

    # Claim from the front desk: "rain keeps people away".
    "by_rain": """
        SELECT CASE WHEN precipitation_mm < 1 THEN '1 dry (<1mm)'
                    WHEN precipitation_mm < 10 THEN '2 light (1-10mm)'
                    ELSE '3 heavy (10mm+)' END AS rain, COUNT(DISTINCT appointment_date) AS days,
               COUNT(*) AS appointments, AVG(no_show) AS no_show_rate
        FROM v_appointment GROUP BY rain ORDER BY rain""",

    "repeat_no_show_patients": """
        WITH p AS (SELECT patient_id, COUNT(*) AS appts, SUM(no_show) AS ns
                   FROM v_appointment GROUP BY patient_id)
        SELECT COUNT(*) AS patients,
               SUM(CASE WHEN ns >= 2 THEN 1 ELSE 0 END) AS patients_2plus_no_shows,
               1.0 * SUM(CASE WHEN ns >= 2 THEN ns ELSE 0 END) / SUM(ns) AS share_of_no_shows
        FROM p""",

    "slots_lost_per_week": """
        SELECT strftime('%W', appointment_date) AS week, COUNT(*) AS appointments, SUM(no_show) AS no_shows
        FROM v_appointment GROUP BY week ORDER BY week""",
}


def _rows(con, sql):
    cur = con.execute(sql)
    cols = [c[0] for c in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


def compute(db_path=config.WAREHOUSE) -> dict:
    with sqlite3.connect(db_path) as con:
        res = {name: _rows(con, sql) for name, sql in QUERIES.items()}

    did = {(r["eligible"], r["outage_day"]): r["no_show_rate"] for r in res["outage_did"]}
    effect = (did[(1, 1)] - did[(1, 0)]) - (did[(0, 1)] - did[(0, 0)])
    cov = res["coverage"][0]
    derived = {
        "did_effect_pp": effect * 100,
        "extra_no_shows_from_outages": effect * cov["on_outage_days"],
        "outage_days": sum(r["is_outage"] for r in res["coverage_by_day"]),
        "appointment_days": len(res["coverage_by_day"]),
        "no_shows_per_appointment_day": res["overall"][0]["no_shows"] / res["overall"][0]["days"],
    }

    # Sanity checks on the outputs themselves: a metric outside its range means a bug upstream.
    rates = [r["no_show_rate"] for r in res["by_lead_bucket"]] + [cov["coverage"]]
    if not all(0 <= r <= 1 for r in rates):
        raise RuntimeError("metric out of [0,1] range")
    if sum(r["appointments"] for r in res["by_lead_bucket"]) != res["overall"][0]["appointments"]:
        raise RuntimeError("lead buckets do not add up to total appointments")

    res["derived"] = derived
    log.info("KPI advance no-show rate=%.3f; coverage=%.3f; outage days=%d; DiD=%+.1fpp",
             res["kpi_advance_no_show"][0]["rate"], cov["coverage"], derived["outage_days"],
             derived["did_effect_pp"])
    return res


def write(res: dict):
    config.OUTPUTS.mkdir(parents=True, exist_ok=True)
    (config.OUTPUTS / "metrics.json").write_text(json.dumps(res, indent=2))
    for name in ["by_lead_bucket", "coverage_by_day", "outage_did", "sms_within_lead", "by_rain"]:
        rows = res[name]
        with open(config.OUTPUTS / f"{name}.csv", "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(rows[0]))
            w.writeheader()
            w.writerows(rows)
