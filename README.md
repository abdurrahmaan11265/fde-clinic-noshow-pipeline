# Clinic no-shows: a dependable data pipeline

FDE assignment, Mohammed Abdur Rahman (roll 10130). The 2-page brief is `report/brief.pdf`.

**Finding:** in 110,516 real appointments from Vitória's public clinics (Apr–Jun 2016), the
SMS reminder reached 0% of eligible patients on 7 of 27 appointment days, and nothing flagged
it. On those days, no-shows among patients who should have been reminded rose 3.9 points,
while the control group (bookings under 3 days ahead, never reminded) stayed flat. Before
anyone builds a no-show predictor, the reminder step has to be made dependable and monitored.

## Run it

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
python run_pipeline.py            # fetch what's missing, then rebuild everything
python run_pipeline.py --offline  # rebuild from saved raw files only
pytest -q                         # 6 tests on the quality rules
```

A full run takes about 5 seconds when the raw files are already on disk.

## Sources (at least two source types, all real)

| Source | Type | Where | Used for |
|---|---|---|---|
| Medical Appointment No Shows | CSV file | canonical: kaggle.com/datasets/joniarroba/noshowappointments (CC BY-NC-SA 4.0); fetched from a public GitHub LFS mirror, SHA-256 `9132d3e7…` and 110,527 rows checked | bookings, reminders, outcomes |
| Nager.Date public holidays | JSON API | `https://date.nager.at/api/v3/PublicHolidays/2016/BR` | explaining gaps in the calendar |
| Open-Meteo historical weather | JSON API | `https://archive-api.open-meteo.com/v1/archive` (Vitória, daily precipitation) | testing "rain keeps people away" |

Raw files in `data/raw/` are never edited. The CSV is stored under its hash. API responses are
stored with their retrieval timestamp, so every run can be traced back to the exact bytes it used.

## Pipeline

```
retrieve  ->  validate  ->  model (SQLite)  ->  metrics  ->  outputs + manifest + log
```

| Step | File | What it guarantees |
|---|---|---|
| Retrieve | `pipeline/retrieve.py` | Hash check on the CSV; a mismatch is saved as `REJECTED_*` and the run stops. HTTP 429/5xx and network errors are retried with exponential backoff. If an API stays down, the last good snapshot is used and the run is marked `ok_with_stale_sources`. |
| Validate | `pipeline/validate.py` | **Hard checks stop the run:** required columns, row count, unique AppointmentID, known outcome values, weather covering every date. **Soft rules:** impossible rows go to `data/quarantine/` with a reason; doubtful rows are flagged and kept. Missing weekdays are cross-checked against the holiday API. |
| Model | `pipeline/model.py` | `patient`, `appointment`, `reminder` (the intervention), `outcome`, `calendar_day`, plus views `v_appointment` and `v_reminder_day`. Foreign-key check and row reconciliation. |
| Metrics | `pipeline/metrics.py` | 13 named SQL queries. Every rate must fall in [0,1] and the lead bands must add up to the total, or the run fails. |
| Orchestrator | `run_pipeline.py` | `logs/run_<id>.log`, `outputs/run_manifest.json`. Exit codes: 0 ok, 1 retrieval, 2 hard data check, 3 integrity. |

Anything that uses the outputs should check `outputs/run_manifest.json` → `status` before
trusting `metrics.json`. A failed run leaves the previous metrics on disk, and the manifest says
the latest run failed.

## Data quality: what I found and what I did

| Issue | Rows | Handling |
|---|---|---|
| Booked after the appointment date | 5 | quarantined |
| Age −1 or 115 | 6 | quarantined |
| `No-show` = "No" means the patient attended | all | renamed to `no_show` (1 = absent) |
| `Hipertension`, `Handcap` misspelled; `Handcap` is 0–4, not a yes/no | all | renamed; kept as `handicap_count` |
| `AppointmentDay` has no time of day | all | lead time counted in calendar days; same-day bookings made after midnight get lead 0, not −1 |
| Same patient, same day, more than one appointment | 16,200 | flagged and kept. Double booking or two specialties? Unknown |
| Saturday appointments | 39 | kept |
| Weekdays with no data: 23, 26, 27 May | 3 days | 26 May is Corpus Christi (from the API); 23 and 27 May are unexplained |
| `SMS_received` is a flag, with no send or delivery event | all | the key missing event; documented as a limitation |

## Layout

```
data/raw/          immutable source files (CSV by hash, JSON by timestamp)
data/staged/       cleaned appointments
data/quarantine/   rejected rows with reasons
warehouse/         SQLite model (rebuilt every run)
outputs/           metrics.json, *.csv, data_quality.json, run_manifest.json
logs/              one log per run
report/            brief.pdf (the 2-page brief)
```
