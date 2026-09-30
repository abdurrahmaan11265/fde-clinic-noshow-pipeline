"""Every source, path and threshold the pipeline depends on, in one place."""
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
RAW = ROOT / "data" / "raw"
STAGED = ROOT / "data" / "staged"
QUARANTINE = ROOT / "data" / "quarantine"
WAREHOUSE = ROOT / "warehouse" / "noshow.db"
OUTPUTS = ROOT / "outputs"
LOGS = ROOT / "logs"

# Source 1 (CSV file): real appointment records, Vitória (ES), Brazil, Apr–Jun 2016.
# Canonical page: https://www.kaggle.com/datasets/joniarroba/noshowappointments (CC BY-NC-SA 4.0).
# Kaggle needs a login, so we fetch a public byte-identical mirror and verify the hash.
APPOINTMENTS = {
    "name": "appointments",
    "url": "https://media.githubusercontent.com/media/TokoniK/exploratory-data-analysis/main/"
           "data/Database_No_show_appointments/noshowappointments-kagglev2-may-2016.csv",
    "canonical": "https://www.kaggle.com/datasets/joniarroba/noshowappointments",
    "sha256": "9132d3e7d0246617df9041d3764f20ad6f08e7b0d9f0997fa254fc5e52eda27d",
    "expected_rows": 110_527,
}

# Source 2 (JSON API): national public holidays, to explain gaps in the calendar.
HOLIDAYS = {
    "name": "holidays",
    "url": "https://date.nager.at/api/v3/PublicHolidays/{year}/BR",
}

# Source 3 (JSON API): daily weather in Vitória, to test the "rain keeps people away" claim.
WEATHER = {
    "name": "weather",
    "url": "https://archive-api.open-meteo.com/v1/archive",
    "params": {
        "latitude": -20.3155,
        "longitude": -40.3128,
        "daily": "precipitation_sum,temperature_2m_max",
        "timezone": "America/Sao_Paulo",
    },
}

HTTP_TIMEOUT_S = 30
HTTP_RETRIES = 4            # attempts per request, with exponential backoff
RETRYABLE_STATUS = {429, 500, 502, 503, 504}

REQUIRED_COLUMNS = [
    "PatientId", "AppointmentID", "Gender", "ScheduledDay", "AppointmentDay", "Age",
    "Neighbourhood", "Scholarship", "Hipertension", "Diabetes", "Alcoholism",
    "Handcap", "SMS_received", "No-show",
]

# Business rules
AGE_MIN, AGE_MAX = 0, 110
REMINDER_MIN_LEAD_DAYS = 3        # inferred: no appointment booked <3 days ahead ever got an SMS
OUTAGE_COVERAGE_THRESHOLD = 0.05  # a day where <5% of eligible bookings got an SMS counts as an outage
