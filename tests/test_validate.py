import pandas as pd
import pytest

from pipeline import config, validate


def _raw(**overrides):
    row = {"PatientId": 29872499824296.0, "AppointmentID": 1, "Gender": "F",
           "ScheduledDay": "2016-04-25T10:00:00Z", "AppointmentDay": "2016-04-29T00:00:00Z",
           "Age": 40, "Neighbourhood": " jardim da penha", "Scholarship": 0, "Hipertension": 1,
           "Diabetes": 0, "Alcoholism": 0, "Handcap": 2, "SMS_received": 1, "No-show": "No"}
    row.update(overrides)
    return row


@pytest.fixture(autouse=True)
def small_expected(monkeypatch):
    monkeypatch.setitem(config.APPOINTMENTS, "expected_rows", 3)


def test_safe_fixes_and_semantics():
    raw = pd.DataFrame([_raw(), _raw(AppointmentID=2, **{"No-show": "Yes"}), _raw(AppointmentID=3)])
    clean, quarantined, _ = validate.validate_appointments(raw)
    assert quarantined.empty
    assert clean["no_show"].tolist() == [0, 1, 0]          # "No" means the patient came
    assert clean["patient_id"].iloc[0] == "29872499824296"  # no float formatting in IDs
    assert clean["handicap_count"].iloc[0] == 2             # kept as a count
    assert clean["neighbourhood"].iloc[0] == "JARDIM DA PENHA"
    assert clean["lead_days"].iloc[0] == 4


def test_same_day_booking_after_midnight_slot_is_not_negative():
    # AppointmentDay is date-only, so a same-day booking at 18:00 must give lead 0, not -1.
    raw = pd.DataFrame([_raw(ScheduledDay="2016-04-29T18:38:08Z"), _raw(AppointmentID=2), _raw(AppointmentID=3)])
    clean, quarantined, _ = validate.validate_appointments(raw)
    assert quarantined.empty and clean["lead_days"].iloc[0] == 0


def test_impossible_rows_are_quarantined_not_dropped():
    raw = pd.DataFrame([_raw(), _raw(AppointmentID=2, Age=-1),
                        _raw(AppointmentID=3, ScheduledDay="2016-05-02T09:00:00Z")])
    clean, quarantined, _ = validate.validate_appointments(raw)
    assert len(clean) == 1 and len(quarantined) == 2
    assert set(quarantined["quarantine_reason"]) == {"age_out_of_range;", "appointment_before_booking;"}


@pytest.mark.parametrize("bad", [
    {"AppointmentID": 1},            # duplicate id
    {"No-show": "Maybe"},            # unknown outcome
])
def test_hard_checks_stop_the_run(bad):
    raw = pd.DataFrame([_raw(), _raw(AppointmentID=2), _raw(**{"AppointmentID": 3, **bad})])
    with pytest.raises(validate.ValidationError):
        validate.validate_appointments(raw)


def test_calendar_gap_explained_only_by_holiday():
    appts = pd.DataFrame({"appointment_date": ["2016-05-25", "2016-05-30"]})
    weather = {"daily": {"time": ["2016-05-25", "2016-05-30"]}}
    gaps, _ = validate.validate_calendar(appts, [{"date": "2016-05-26", "name": "Corpus Christi"}], weather)
    assert gaps["explained_by_holiday"] == {"2016-05-26": "Corpus Christi"}
    assert gaps["unexplained"] == ["2016-05-27"]
