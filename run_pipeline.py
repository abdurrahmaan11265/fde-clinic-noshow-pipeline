"""retrieve -> validate -> model -> metrics, in one repeatable run.

    python run_pipeline.py            # fetch what is missing or stale, then rebuild everything
    python run_pipeline.py --offline  # rebuild from saved raw files only (no network)

Exit codes: 0 ok, 1 retrieval failed, 2 a hard data check failed, 3 model/metric integrity failed.
Every run writes logs/run_<id>.log and outputs/run_manifest.json.
"""
import argparse
import json
import logging
import sys
import time
from datetime import datetime, timezone

import pandas as pd

from pipeline import config, metrics, model, retrieve, validate


def setup_logging(run_id):
    config.LOGS.mkdir(parents=True, exist_ok=True)
    fmt = logging.Formatter("%(asctime)s %(levelname)-7s %(name)-18s %(message)s")
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    for h in (logging.FileHandler(config.LOGS / f"run_{run_id}.log"), logging.StreamHandler(sys.stdout)):
        h.setFormatter(fmt)
        root.addHandler(h)
    return logging.getLogger("pipeline")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--offline", action="store_true", help="use saved raw files only")
    args = parser.parse_args(argv)

    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    log = setup_logging(run_id)
    manifest = {"run_id": run_id, "offline": args.offline, "sources": [], "steps": {}, "status": "running"}
    t0 = time.time()

    def finish(status, code, error=None):
        manifest.update(status=status, error=error, seconds=round(time.time() - t0, 1))
        config.OUTPUTS.mkdir(parents=True, exist_ok=True)
        (config.OUTPUTS / "run_manifest.json").write_text(json.dumps(manifest, indent=2))
        log.info("run %s finished: %s (exit %d)", run_id, status, code)
        return code

    # 1. RETRIEVE ---------------------------------------------------------------------------
    try:
        appt_src = retrieve.fetch_appointments(offline=args.offline)
        raw = pd.read_csv(config.ROOT / appt_src["path"])
        appt_src["rows"] = len(raw)
        dates = pd.to_datetime(raw["AppointmentDay"]).dt.strftime("%Y-%m-%d")
        start, end = dates.min(), dates.max()
        hol_srcs = retrieve.fetch_holidays({int(start[:4]), int(end[:4])}, offline=args.offline)
        wx_src = retrieve.fetch_weather(start, end, offline=args.offline)
        manifest["sources"] = [appt_src, *hol_srcs, wx_src]
    except retrieve.RetrievalError as exc:
        log.error("retrieval failed: %s", exc)
        return finish("failed_retrieval", 1, str(exc))
    holidays = [h for s in hol_srcs for h in json.loads((config.ROOT / s["path"]).read_text())]
    weather = json.loads((config.ROOT / wx_src["path"]).read_text())
    stale = [s["source"] for s in manifest["sources"] if s["status"] == "stale"]
    if stale:
        log.warning("running with stale snapshots for: %s", stale)

    # 2. VALIDATE ---------------------------------------------------------------------------
    try:
        prof = validate.profile(raw)
        clean, quarantined, checks = validate.validate_appointments(raw)
        calendar_gaps, cal_checks = validate.validate_calendar(clean, holidays, weather)
    except validate.ValidationError as exc:
        log.error("hard check failed, no metrics produced: %s", exc)
        return finish("failed_validation", 2, str(exc))
    for folder, frame, name in [(config.STAGED, clean, "appointments_clean.csv"),
                                (config.QUARANTINE, quarantined, "appointments_quarantine.csv")]:
        folder.mkdir(parents=True, exist_ok=True)
        frame.to_csv(folder / name, index=False)
    config.OUTPUTS.mkdir(parents=True, exist_ok=True)
    validate.write_quality_report(config.OUTPUTS / "data_quality.json", prof, checks + cal_checks, calendar_gaps)
    manifest["steps"]["validate"] = {"raw_rows": len(raw), "clean_rows": len(clean),
                                     "quarantined_rows": len(quarantined),
                                     "soft_failures": [c["rule"] for c in checks + cal_checks if not c["passed"]]}

    # 3. MODEL + 4. METRICS -----------------------------------------------------------------
    try:
        cal = model.build_calendar(clean, holidays, weather)
        manifest["steps"]["model"] = model.build_warehouse(clean, cal)
        res = metrics.compute()
        metrics.write(res)
    except Exception as exc:  # integrity failures here mean the numbers cannot be trusted
        log.exception("model/metrics failed")
        return finish("failed_integrity", 3, str(exc))
    manifest["steps"]["metrics"] = {"kpi_advance_no_show": res["kpi_advance_no_show"][0],
                                    "derived": res["derived"]}
    return finish("ok_with_stale_sources" if stale else "ok", 0)


if __name__ == "__main__":
    sys.exit(main())
