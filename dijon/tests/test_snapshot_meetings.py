
from datetime import time, timedelta
from decimal import Decimal

import pytest
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from dijon import crud
from dijon.models import (
    DayOfWeekEnum,
    Meeting,
    MeetingNawsCode,
    RootServer,
    ServiceBody,
    Snapshot,
    VenueTypeEnum,
)
from dijon.snapshot import BmltMeeting, SnapshotCache, save_meetings


@pytest.fixture
def root_server_1(db: Session) -> RootServer:
    return crud.create_root_server(db, "root name", "https://blah/main_server/")


@pytest.fixture
def snapshot_1(db: Session, root_server_1: RootServer) -> Snapshot:
    return crud.create_snapshot(db, root_server_1)


@pytest.fixture
def service_body_1(db: Session, snapshot_1: Snapshot) -> ServiceBody:
    return crud.create_service_body(db, snapshot_1.id, 1, "sb name", "AS")


@pytest.fixture
def cache(db: Session, snapshot_1: Snapshot) -> SnapshotCache:
    return SnapshotCache(db, snapshot_1)


def get_mock_raw_meeting() -> dict[str, str]:
    return {
        "id_bigint": "6102",
        "worldid_mixed": "G00013329",
        "shared_group_id_bigint": "",
        "service_body_bigint": "101",
        "weekday_tinyint": "1",
        "venue_type": "",
        "start_time": "16:00:00",
        "duration_time": "01:00:00",
        "time_zone": "America/New_York",
        "formats": "O,CW,D,TOP,To,TC",
        "lang_enum": "en",
        "longitude": "-82.8381874",
        "latitude": "34.6840723",
        "distance_in_km": "",
        "distance_in_miles": "",
        "email_contact": "",
        "contact_phone_2": "",
        "contact_email_1": "",
        "contact_phone_1": "",
        "contact_email_2": "",
        "contact_name_1": "",
        "contact_name_2": "",
        "comments": "",
        "virtual_meeting_additional_info": "",
        "location_city_subsection": "",
        "virtual_meeting_link": "",
        "phone_meeting_number": "",
        "location_nation": "US",
        "location_postal_code_1": "29631",
        "location_province": "SC",
        "location_sub_province": "Pickens",
        "location_municipality": "Clemson",
        "location_neighborhood": "",
        "location_street": "111 Sloan St.",
        "location_info": "Entrance/parking on Clemson Ave. at rear of church",
        "location_text": "University Lutheran Church",
        "meeting_name": "Pioneers of Change (POC)",
        "bus_lines": "Bus Lines#@-@#On CAT bus line",
        "train_lines": "",
        "published": "0",
        "root_server_uri": "https://bmlt.sezf.org/main_server",
        "format_shared_id_list": "7,8,17,29,83,340"
    }


def get_mock_bmlt_meeting() -> BmltMeeting:
    raw_meeting = get_mock_raw_meeting()
    return BmltMeeting(**raw_meeting)


def test_parse_raw_meeting_id_bigint():
    mock_meeting = get_mock_raw_meeting()
    mock_meeting["id_bigint"] = "123"
    bmlt_meeting = BmltMeeting(**mock_meeting)
    assert bmlt_meeting.id_bigint == 123

    with pytest.raises(ValueError):
        mock_meeting["id_bigint"] = ""
        BmltMeeting(**mock_meeting)


def test_parse_raw_meeting_meeting_name():
    mock_meeting = get_mock_raw_meeting()
    mock_meeting["meeting_name"] = "Living The Program"
    bmlt_meeting = BmltMeeting(**mock_meeting)
    assert bmlt_meeting.meeting_name == "Living The Program"

    with pytest.raises(ValueError):
        mock_meeting["meeting_name"] = ""
        BmltMeeting(**mock_meeting)

    with pytest.raises(ValueError):
        del mock_meeting["meeting_name"]
        BmltMeeting(**mock_meeting)


def test_parse_raw_meeting_format_shared_id_list():
    mock_meeting = get_mock_raw_meeting()
    mock_meeting["format_shared_id_list"] = "7,8,17,29,83,340"
    bmlt_meeting = BmltMeeting(**mock_meeting)
    assert bmlt_meeting.format_shared_id_list == [7, 8, 17, 29, 83, 340]

    mock_meeting["format_shared_id_list"] = ""
    bmlt_meeting = BmltMeeting(**mock_meeting)
    assert bmlt_meeting.format_shared_id_list == []

    del mock_meeting["format_shared_id_list"]
    bmlt_meeting = BmltMeeting(**mock_meeting)
    assert bmlt_meeting.format_shared_id_list == []


def test_parse_raw_meeting_worldid_mixed():
    mock_meeting = get_mock_raw_meeting()
    mock_meeting["worldid_mixed"] = "G00013329"
    bmlt_meeting = BmltMeeting(**mock_meeting)
    assert bmlt_meeting.worldid_mixed == "G00013329"

    mock_meeting["worldid_mixed"] = ""
    bmlt_meeting = BmltMeeting(**mock_meeting)
    assert bmlt_meeting.worldid_mixed is None

    del mock_meeting["worldid_mixed"]
    bmlt_meeting = BmltMeeting(**mock_meeting)
    assert bmlt_meeting.worldid_mixed is None


def test_parse_raw_meeting_service_body_bigint():
    mock_meeting = get_mock_raw_meeting()
    mock_meeting["service_body_bigint"] = "123"
    bmlt_meeting = BmltMeeting(**mock_meeting)
    assert bmlt_meeting.service_body_bigint == 123

    with pytest.raises(ValueError):
        mock_meeting["service_body_bigint"] = ""
        BmltMeeting(**mock_meeting)


def test_parse_raw_meeting_weekday_tinyint():
    mock_meeting = get_mock_raw_meeting()
    for valid_i in range(1, 8):
        mock_meeting["weekday_tinyint"] = str(valid_i)
        bmlt_meeting = BmltMeeting(**mock_meeting)
        assert bmlt_meeting.weekday_tinyint == valid_i

    with pytest.raises(ValueError):
        mock_meeting["weekday_tinyint"] = "0"
        BmltMeeting(**mock_meeting)

    with pytest.raises(ValueError):
        mock_meeting["weekday_tinyint"] = "8"
        BmltMeeting(**mock_meeting)

    with pytest.raises(ValueError):
        mock_meeting["weekday_tinyint"] = ""
        BmltMeeting(**mock_meeting)

    with pytest.raises(ValueError):
        del mock_meeting["weekday_tinyint"]
        BmltMeeting(**mock_meeting)


def test_parse_raw_meeting_venue_type():
    mock_meeting = get_mock_raw_meeting()
    for valid_i in range(1, 4):
        mock_meeting["venue_type"] = str(valid_i)
        bmlt_meeting = BmltMeeting(**mock_meeting)
        assert bmlt_meeting.venue_type == valid_i

    with pytest.raises(ValueError):
        mock_meeting["venue_type"] = "0"
        BmltMeeting(**mock_meeting)

    with pytest.raises(ValueError):
        mock_meeting["venue_type"] = "4"
        BmltMeeting(**mock_meeting)

    mock_meeting["venue_type"] = ""
    bmlt_meeting = BmltMeeting(**mock_meeting)
    assert bmlt_meeting.venue_type is None

    del mock_meeting["venue_type"]
    bmlt_meeting = BmltMeeting(**mock_meeting)
    assert bmlt_meeting.venue_type is None


def test_parse_raw_meeting_start_time():
    mock_meeting = get_mock_raw_meeting()
    mock_meeting["start_time"] = "01:00:00"
    bmlt_meeting = BmltMeeting(**mock_meeting)
    assert bmlt_meeting.start_time.hour == 1
    assert bmlt_meeting.start_time.minute == 0

    mock_meeting["start_time"] = "01:30:00"
    bmlt_meeting = BmltMeeting(**mock_meeting)
    assert bmlt_meeting.start_time.hour == 1
    assert bmlt_meeting.start_time.minute == 30

    mock_meeting["start_time"] = "13:30:00"
    bmlt_meeting = BmltMeeting(**mock_meeting)
    assert bmlt_meeting.start_time.hour == 13
    assert bmlt_meeting.start_time.minute == 30

    mock_meeting["start_time"] = "00:15:00"
    bmlt_meeting = BmltMeeting(**mock_meeting)
    assert bmlt_meeting.start_time.hour == 0
    assert bmlt_meeting.start_time.minute == 15

    mock_meeting["start_time"] = "23:59:00"
    bmlt_meeting = BmltMeeting(**mock_meeting)
    assert bmlt_meeting.start_time.hour == 23
    assert bmlt_meeting.start_time.minute == 59

    with pytest.raises(ValueError):
        mock_meeting["start_time"] = "24:00:00"
        bmlt_meeting = BmltMeeting(**mock_meeting)

    with pytest.raises(ValueError):
        mock_meeting["start_time"] = ""
        bmlt_meeting = BmltMeeting(**mock_meeting)

    with pytest.raises(ValueError):
        del mock_meeting["start_time"]
        bmlt_meeting = BmltMeeting(**mock_meeting)


def test_parse_raw_meeting_duration_time():
    mock_meeting = get_mock_raw_meeting()
    mock_meeting["duration_time"] = "01:00:00"
    bmlt_meeting = BmltMeeting(**mock_meeting)
    assert bmlt_meeting.duration_time == timedelta(hours=1)

    mock_meeting["duration_time"] = "01:30:00"
    bmlt_meeting = BmltMeeting(**mock_meeting)
    assert bmlt_meeting.duration_time == timedelta(hours=1, minutes=30)

    mock_meeting["duration_time"] = "00:15:00"
    bmlt_meeting = BmltMeeting(**mock_meeting)
    assert bmlt_meeting.duration_time == timedelta(minutes=15)

    with pytest.raises(ValueError):
        mock_meeting["duration_time"] = ""
        bmlt_meeting = BmltMeeting(**mock_meeting)

    with pytest.raises(ValueError):
        del mock_meeting["duration_time"]
        bmlt_meeting = BmltMeeting(**mock_meeting)


def test_parse_raw_meeting_time_zone():
    mock_meeting = get_mock_raw_meeting()
    mock_meeting["time_zone"] = "America/New York"
    bmlt_meeting = BmltMeeting(**mock_meeting)
    assert bmlt_meeting.time_zone == "America/New York"

    mock_meeting["time_zone"] = ""
    bmlt_meeting = BmltMeeting(**mock_meeting)
    assert bmlt_meeting.time_zone is None

    del mock_meeting["time_zone"]
    bmlt_meeting = BmltMeeting(**mock_meeting)
    assert bmlt_meeting.time_zone is None


def test_parse_raw_meeting_longitude():
    mock_meeting = get_mock_raw_meeting()
    mock_meeting["longitude"] = "-82.8381874"
    bmlt_meeting = BmltMeeting(**mock_meeting)
    assert bmlt_meeting.longitude == Decimal("-82.8381874")

    mock_meeting["longitude"] = ""
    bmlt_meeting = BmltMeeting(**mock_meeting)
    assert bmlt_meeting.longitude is None

    del mock_meeting["longitude"]
    bmlt_meeting = BmltMeeting(**mock_meeting)
    assert bmlt_meeting.longitude is None


def test_parse_raw_meeting_latitude():
    mock_meeting = get_mock_raw_meeting()
    mock_meeting["latitude"] = "-82.8381874"
    bmlt_meeting = BmltMeeting(**mock_meeting)
    assert bmlt_meeting.latitude == Decimal("-82.8381874")

    mock_meeting["latitude"] = ""
    bmlt_meeting = BmltMeeting(**mock_meeting)
    assert bmlt_meeting.latitude is None

    del mock_meeting["latitude"]
    bmlt_meeting = BmltMeeting(**mock_meeting)
    assert bmlt_meeting.latitude is None


def test_parse_raw_meeting_virtual_meeting_additional_info():
    mock_meeting = get_mock_raw_meeting()
    mock_meeting["virtual_meeting_additional_info"] = "some pretty sweet additional info"
    bmlt_meeting = BmltMeeting(**mock_meeting)
    assert bmlt_meeting.virtual_meeting_additional_info == "some pretty sweet additional info"

    mock_meeting["virtual_meeting_additional_info"] = ""
    bmlt_meeting = BmltMeeting(**mock_meeting)
    assert bmlt_meeting.virtual_meeting_additional_info is None

    del mock_meeting["virtual_meeting_additional_info"]
    bmlt_meeting = BmltMeeting(**mock_meeting)
    assert bmlt_meeting.virtual_meeting_additional_info is None


def test_parse_raw_meeting_location_city_subsection():
    mock_meeting = get_mock_raw_meeting()
    mock_meeting["location_city_subsection"] = "some city subsection"
    bmlt_meeting = BmltMeeting(**mock_meeting)
    assert bmlt_meeting.location_city_subsection == "some city subsection"

    mock_meeting["location_city_subsection"] = ""
    bmlt_meeting = BmltMeeting(**mock_meeting)
    assert bmlt_meeting.location_city_subsection is None

    del mock_meeting["location_city_subsection"]
    bmlt_meeting = BmltMeeting(**mock_meeting)
    assert bmlt_meeting.location_city_subsection is None


def test_parse_raw_meeting_virtual_meeting_link():
    mock_meeting = get_mock_raw_meeting()
    mock_meeting["virtual_meeting_link"] = "https://link"
    bmlt_meeting = BmltMeeting(**mock_meeting)
    assert bmlt_meeting.virtual_meeting_link == "https://link"

    mock_meeting["virtual_meeting_link"] = ""
    bmlt_meeting = BmltMeeting(**mock_meeting)
    assert bmlt_meeting.virtual_meeting_link is None

    del mock_meeting["virtual_meeting_link"]
    bmlt_meeting = BmltMeeting(**mock_meeting)
    assert bmlt_meeting.virtual_meeting_link is None


def test_parse_raw_meeting_phone_meeting_number():
    mock_meeting = get_mock_raw_meeting()
    mock_meeting["phone_meeting_number"] = "5555555555"
    bmlt_meeting = BmltMeeting(**mock_meeting)
    assert bmlt_meeting.phone_meeting_number == "5555555555"

    mock_meeting["phone_meeting_number"] = ""
    bmlt_meeting = BmltMeeting(**mock_meeting)
    assert bmlt_meeting.phone_meeting_number is None

    del mock_meeting["phone_meeting_number"]
    bmlt_meeting = BmltMeeting(**mock_meeting)
    assert bmlt_meeting.phone_meeting_number is None


def test_parse_raw_meeting_location_nation():
    mock_meeting = get_mock_raw_meeting()
    mock_meeting["location_nation"] = "US"
    bmlt_meeting = BmltMeeting(**mock_meeting)
    assert bmlt_meeting.location_nation == "US"

    mock_meeting["location_nation"] = ""
    bmlt_meeting = BmltMeeting(**mock_meeting)
    assert bmlt_meeting.location_nation is None

    del mock_meeting["location_nation"]
    bmlt_meeting = BmltMeeting(**mock_meeting)
    assert bmlt_meeting.location_nation is None


def test_parse_raw_meeting_location_postal_code_1():
    mock_meeting = get_mock_raw_meeting()
    mock_meeting["location_postal_code_1"] = "27205"
    bmlt_meeting = BmltMeeting(**mock_meeting)
    assert bmlt_meeting.location_postal_code_1 == "27205"

    mock_meeting["location_postal_code_1"] = ""
    bmlt_meeting = BmltMeeting(**mock_meeting)
    assert bmlt_meeting.location_postal_code_1 is None

    del mock_meeting["location_postal_code_1"]
    bmlt_meeting = BmltMeeting(**mock_meeting)
    assert bmlt_meeting.location_postal_code_1 is None


def test_parse_raw_meeting_location_province():
    mock_meeting = get_mock_raw_meeting()
    mock_meeting["location_province"] = "SC"
    bmlt_meeting = BmltMeeting(**mock_meeting)
    assert bmlt_meeting.location_province == "SC"

    mock_meeting["location_province"] = ""
    bmlt_meeting = BmltMeeting(**mock_meeting)
    assert bmlt_meeting.location_province is None

    del mock_meeting["location_province"]
    bmlt_meeting = BmltMeeting(**mock_meeting)
    assert bmlt_meeting.location_province is None


def test_parse_raw_meeting_location_sub_province():
    mock_meeting = get_mock_raw_meeting()
    mock_meeting["location_sub_province"] = "Pickens"
    bmlt_meeting = BmltMeeting(**mock_meeting)
    assert bmlt_meeting.location_sub_province == "Pickens"

    mock_meeting["location_sub_province"] = ""
    bmlt_meeting = BmltMeeting(**mock_meeting)
    assert bmlt_meeting.location_sub_province is None

    del mock_meeting["location_sub_province"]
    bmlt_meeting = BmltMeeting(**mock_meeting)
    assert bmlt_meeting.location_sub_province is None


def test_parse_raw_meeting_location_municipality():
    mock_meeting = get_mock_raw_meeting()
    mock_meeting["location_municipality"] = "Clemson"
    bmlt_meeting = BmltMeeting(**mock_meeting)
    assert bmlt_meeting.location_municipality == "Clemson"

    mock_meeting["location_municipality"] = ""
    bmlt_meeting = BmltMeeting(**mock_meeting)
    assert bmlt_meeting.location_municipality is None

    del mock_meeting["location_municipality"]
    bmlt_meeting = BmltMeeting(**mock_meeting)
    assert bmlt_meeting.location_municipality is None


def test_parse_raw_meeting_location_neighborhood():
    mock_meeting = get_mock_raw_meeting()
    mock_meeting["location_neighborhood"] = "Pinecroft Village"
    bmlt_meeting = BmltMeeting(**mock_meeting)
    assert bmlt_meeting.location_neighborhood == "Pinecroft Village"

    mock_meeting["location_neighborhood"] = ""
    bmlt_meeting = BmltMeeting(**mock_meeting)
    assert bmlt_meeting.location_neighborhood is None

    del mock_meeting["location_neighborhood"]
    bmlt_meeting = BmltMeeting(**mock_meeting)
    assert bmlt_meeting.location_neighborhood is None


def test_parse_raw_meeting_location_street():
    mock_meeting = get_mock_raw_meeting()
    mock_meeting["location_street"] = "111 Sloan St."
    bmlt_meeting = BmltMeeting(**mock_meeting)
    assert bmlt_meeting.location_street == "111 Sloan St."

    mock_meeting["location_street"] = ""
    bmlt_meeting = BmltMeeting(**mock_meeting)
    assert bmlt_meeting.location_street is None

    del mock_meeting["location_street"]
    bmlt_meeting = BmltMeeting(**mock_meeting)
    assert bmlt_meeting.location_street is None


def test_parse_raw_meeting_location_info():
    mock_meeting = get_mock_raw_meeting()
    mock_meeting["location_info"] = "Entrance/parking on Clemson Ave. at rear of church"
    bmlt_meeting = BmltMeeting(**mock_meeting)
    assert bmlt_meeting.location_info == "Entrance/parking on Clemson Ave. at rear of church"

    mock_meeting["location_info"] = ""
    bmlt_meeting = BmltMeeting(**mock_meeting)
    assert bmlt_meeting.location_info is None

    del mock_meeting["location_info"]
    bmlt_meeting = BmltMeeting(**mock_meeting)
    assert bmlt_meeting.location_info is None


def test_parse_raw_meeting_location_text():
    mock_meeting = get_mock_raw_meeting()
    mock_meeting["location_text"] = "University Lutheran Church"
    bmlt_meeting = BmltMeeting(**mock_meeting)
    assert bmlt_meeting.location_text == "University Lutheran Church"

    mock_meeting["location_text"] = ""
    bmlt_meeting = BmltMeeting(**mock_meeting)
    assert bmlt_meeting.location_text is None

    del mock_meeting["location_text"]
    bmlt_meeting = BmltMeeting(**mock_meeting)
    assert bmlt_meeting.location_text is None


def test_parse_raw_meeting_bus_lines():
    mock_meeting = get_mock_raw_meeting()
    mock_meeting["bus_lines"] = "some bus line"
    bmlt_meeting = BmltMeeting(**mock_meeting)
    assert bmlt_meeting.bus_lines == "some bus line"

    mock_meeting["bus_lines"] = ""
    bmlt_meeting = BmltMeeting(**mock_meeting)
    assert bmlt_meeting.bus_lines is None

    del mock_meeting["bus_lines"]
    bmlt_meeting = BmltMeeting(**mock_meeting)
    assert bmlt_meeting.bus_lines is None


def test_parse_raw_meeting_train_lines():
    mock_meeting = get_mock_raw_meeting()
    mock_meeting["train_lines"] = "some train line"
    bmlt_meeting = BmltMeeting(**mock_meeting)
    assert bmlt_meeting.train_lines == "some train line"

    mock_meeting["train_lines"] = ""
    bmlt_meeting = BmltMeeting(**mock_meeting)
    assert bmlt_meeting.train_lines is None

    del mock_meeting["train_lines"]
    bmlt_meeting = BmltMeeting(**mock_meeting)
    assert bmlt_meeting.train_lines is None


def test_parse_raw_meeting_published():
    mock_meeting = get_mock_raw_meeting()
    mock_meeting["published"] = "1"
    bmlt_meeting = BmltMeeting(**mock_meeting)
    assert bmlt_meeting.published is True

    mock_meeting["published"] = "0"
    bmlt_meeting = BmltMeeting(**mock_meeting)
    assert bmlt_meeting.published is False

    with pytest.raises(ValueError):
        mock_meeting["published"] = ""
        BmltMeeting(**mock_meeting)

    with pytest.raises(ValueError):
        del mock_meeting["published"]
        BmltMeeting(**mock_meeting)


def test_bmlt_meeting_to_db_bmlt_id(db: Session, cache: SnapshotCache, service_body_1: ServiceBody):
    bmlt_meeting = get_mock_bmlt_meeting()
    bmlt_meeting.service_body_bigint = service_body_1.bmlt_id

    bmlt_meeting.id_bigint = 123
    db_meeting, _ = bmlt_meeting.to_db(db, cache)
    db.add(db_meeting)
    db.flush()
    assert db_meeting.bmlt_id == 123

    with pytest.raises(IntegrityError):
        bmlt_meeting.id_bigint = None
        db_meeting, _ = bmlt_meeting.to_db(db, cache)
        db.add(db_meeting)
        db.flush()


def test_bmlt_meeting_to_db_name(db: Session, cache: SnapshotCache, service_body_1: ServiceBody):
    bmlt_meeting = get_mock_bmlt_meeting()
    bmlt_meeting.service_body_bigint = service_body_1.bmlt_id

    bmlt_meeting.meeting_name = "Living The Program"
    db_meeting, _ = bmlt_meeting.to_db(db, cache)
    db.add(db_meeting)
    db.flush()
    assert db_meeting.name == "Living The Program"

    with pytest.raises(IntegrityError):
        bmlt_meeting.meeting_name = None
        db_meeting, _ = bmlt_meeting.to_db(db, cache)
        db.add(db_meeting)
        db.flush()


def test_bmlt_meeting_to_db_day(db: Session, cache: SnapshotCache, service_body_1: ServiceBody):
    bmlt_meeting = get_mock_bmlt_meeting()
    bmlt_meeting.service_body_bigint = service_body_1.bmlt_id

    bmlt_meeting.weekday_tinyint = 1
    db_meeting, _ = bmlt_meeting.to_db(db, cache)
    db.add(db_meeting)
    db.flush()
    assert db_meeting.day == DayOfWeekEnum.SUNDAY

    with pytest.raises(ValueError):
        bmlt_meeting.weekday_tinyint = None
        db_meeting, _ = bmlt_meeting.to_db(db, cache)
        db.add(db_meeting)
        db.flush()


def test_bmlt_meeting_to_db_service_body_id(db: Session, cache: SnapshotCache, service_body_1: ServiceBody):
    bmlt_meeting = get_mock_bmlt_meeting()
    bmlt_meeting.service_body_bigint = service_body_1.bmlt_id

    db_meeting, _ = bmlt_meeting.to_db(db, cache)
    db.add(db_meeting)
    db.flush()
    assert db_meeting.service_body_id == service_body_1.id
    assert db_meeting.service_body == service_body_1

    with pytest.raises(ValueError):
        bmlt_meeting.service_body_bigint = None
        db_meeting, _ = bmlt_meeting.to_db(db, cache)
        db.add(db_meeting)
        db.flush()


def test_bmlt_meeting_to_db_start_time(db: Session, cache: SnapshotCache, service_body_1: ServiceBody):
    bmlt_meeting = get_mock_bmlt_meeting()
    bmlt_meeting.service_body_bigint = service_body_1.bmlt_id

    bmlt_meeting.start_time = time(hour=1, minute=30)
    db_meeting, _ = bmlt_meeting.to_db(db, cache)
    db.add(db_meeting)
    db.flush()
    assert db_meeting.start_time == time(hour=1, minute=30)

    with pytest.raises(IntegrityError):
        bmlt_meeting.start_time = None
        db_meeting, _ = bmlt_meeting.to_db(db, cache)
        db.add(db_meeting)
        db.flush()


def test_bmlt_meeting_to_db_duration(db: Session, cache: SnapshotCache, service_body_1: ServiceBody):
    bmlt_meeting = get_mock_bmlt_meeting()
    bmlt_meeting.service_body_bigint = service_body_1.bmlt_id

    bmlt_meeting.duration_time = timedelta(hours=1)
    db_meeting, _ = bmlt_meeting.to_db(db, cache)
    db.add(db_meeting)
    db.flush()
    assert db_meeting.duration == timedelta(hours=1)

    with pytest.raises(IntegrityError):
        bmlt_meeting.duration_time = None
        db_meeting, _ = bmlt_meeting.to_db(db, cache)
        db.add(db_meeting)
        db.flush()


def test_bmlt_meeting_to_db_venue_type(db: Session, cache: SnapshotCache, service_body_1: ServiceBody):
    bmlt_meeting = get_mock_bmlt_meeting()
    bmlt_meeting.service_body_bigint = service_body_1.bmlt_id

    bmlt_meeting.venue_type = 1
    db_meeting, _ = bmlt_meeting.to_db(db, cache)
    db.add(db_meeting)
    db.flush()
    assert db_meeting.venue_type == VenueTypeEnum.IN_PERSON

    bmlt_meeting.venue_type = 2
    db_meeting, _ = bmlt_meeting.to_db(db, cache)
    db.add(db_meeting)
    db.flush()
    assert db_meeting.venue_type == VenueTypeEnum.VIRTUAL

    bmlt_meeting.venue_type = 3
    db_meeting, _ = bmlt_meeting.to_db(db, cache)
    db.add(db_meeting)
    db.flush()
    assert db_meeting.venue_type == VenueTypeEnum.HYBRID

    bmlt_meeting.venue_type = None
    db_meeting, _ = bmlt_meeting.to_db(db, cache)
    db.add(db_meeting)
    db.flush()
    assert db_meeting.venue_type == VenueTypeEnum.NONE


def test_bmlt_meeting_to_db_time_zone(db: Session, cache: SnapshotCache, service_body_1: ServiceBody):
    bmlt_meeting = get_mock_bmlt_meeting()
    bmlt_meeting.service_body_bigint = service_body_1.bmlt_id

    bmlt_meeting.time_zone = "America/New_York"
    db_meeting, _ = bmlt_meeting.to_db(db, cache)
    db.add(db_meeting)
    db.flush()
    assert db_meeting.time_zone == "America/New_York"

    bmlt_meeting.time_zone = None
    db_meeting, _ = bmlt_meeting.to_db(db, cache)
    db.add(db_meeting)
    db.flush()


def test_bmlt_meeting_to_db_longitude(db: Session, cache: SnapshotCache, service_body_1: ServiceBody):
    bmlt_meeting = get_mock_bmlt_meeting()
    bmlt_meeting.service_body_bigint = service_body_1.bmlt_id

    bmlt_meeting.longitude = Decimal("34.6840723")
    db_meeting, _ = bmlt_meeting.to_db(db, cache)
    db.add(db_meeting)
    db.flush()
    assert db_meeting.longitude == Decimal("34.6840723")

    bmlt_meeting.longitude = None
    db_meeting, _ = bmlt_meeting.to_db(db, cache)
    db.add(db_meeting)
    db.flush()


def test_bmlt_meeting_to_db_latitude(db: Session, cache: SnapshotCache, service_body_1: ServiceBody):
    bmlt_meeting = get_mock_bmlt_meeting()
    bmlt_meeting.service_body_bigint = service_body_1.bmlt_id

    bmlt_meeting.latitude = Decimal("34.6840723")
    db_meeting, _ = bmlt_meeting.to_db(db, cache)
    db.add(db_meeting)
    db.flush()
    assert db_meeting.latitude == Decimal("34.6840723")

    bmlt_meeting.latitude = None
    db_meeting, _ = bmlt_meeting.to_db(db, cache)
    db.add(db_meeting)
    db.flush()


def test_bmlt_meeting_to_db_comments(db: Session, cache: SnapshotCache, service_body_1: ServiceBody):
    bmlt_meeting = get_mock_bmlt_meeting()
    bmlt_meeting.service_body_bigint = service_body_1.bmlt_id

    bmlt_meeting.comments = "a really cool string"
    db_meeting, _ = bmlt_meeting.to_db(db, cache)
    db.add(db_meeting)
    db.flush()
    assert db_meeting.comments == "a really cool string"

    bmlt_meeting.comments = None
    db_meeting, _ = bmlt_meeting.to_db(db, cache)
    db.add(db_meeting)
    db.flush()


def test_bmlt_meeting_to_db_virtual_meeting_additional_info(db: Session, cache: SnapshotCache, service_body_1: ServiceBody):
    bmlt_meeting = get_mock_bmlt_meeting()
    bmlt_meeting.service_body_bigint = service_body_1.bmlt_id

    bmlt_meeting.virtual_meeting_additional_info = "a really cool string"
    db_meeting, _ = bmlt_meeting.to_db(db, cache)
    db.add(db_meeting)
    db.flush()
    assert db_meeting.virtual_meeting_additional_info == "a really cool string"

    bmlt_meeting.virtual_meeting_additional_info = None
    db_meeting, _ = bmlt_meeting.to_db(db, cache)
    db.add(db_meeting)
    db.flush()


def test_bmlt_meeting_to_db_location_city_subsection(db: Session, cache: SnapshotCache, service_body_1: ServiceBody):
    bmlt_meeting = get_mock_bmlt_meeting()
    bmlt_meeting.service_body_bigint = service_body_1.bmlt_id

    bmlt_meeting.location_city_subsection = "a really cool string"
    db_meeting, _ = bmlt_meeting.to_db(db, cache)
    db.add(db_meeting)
    db.flush()
    assert db_meeting.location_city_subsection == "a really cool string"

    bmlt_meeting.location_city_subsection = None
    db_meeting, _ = bmlt_meeting.to_db(db, cache)
    db.add(db_meeting)
    db.flush()


def test_bmlt_meeting_to_db_virtual_meeting_link(db: Session, cache: SnapshotCache, service_body_1: ServiceBody):
    bmlt_meeting = get_mock_bmlt_meeting()
    bmlt_meeting.service_body_bigint = service_body_1.bmlt_id

    bmlt_meeting.virtual_meeting_link = "a really cool string"
    db_meeting, _ = bmlt_meeting.to_db(db, cache)
    db.add(db_meeting)
    db.flush()
    assert db_meeting.virtual_meeting_link == "a really cool string"

    bmlt_meeting.virtual_meeting_link = None
    db_meeting, _ = bmlt_meeting.to_db(db, cache)
    db.add(db_meeting)
    db.flush()


def test_bmlt_meeting_to_db_phone_meeting_number(db: Session, cache: SnapshotCache, service_body_1: ServiceBody):
    bmlt_meeting = get_mock_bmlt_meeting()
    bmlt_meeting.service_body_bigint = service_body_1.bmlt_id

    bmlt_meeting.phone_meeting_number = "a really cool string"
    db_meeting, _ = bmlt_meeting.to_db(db, cache)
    db.add(db_meeting)
    db.flush()
    assert db_meeting.phone_meeting_number == "a really cool string"

    bmlt_meeting.phone_meeting_number = None
    db_meeting, _ = bmlt_meeting.to_db(db, cache)
    db.add(db_meeting)
    db.flush()


def test_bmlt_meeting_to_db_location_nation(db: Session, cache: SnapshotCache, service_body_1: ServiceBody):
    bmlt_meeting = get_mock_bmlt_meeting()
    bmlt_meeting.service_body_bigint = service_body_1.bmlt_id

    bmlt_meeting.location_nation = "a really cool string"
    db_meeting, _ = bmlt_meeting.to_db(db, cache)
    db.add(db_meeting)
    db.flush()
    assert db_meeting.location_nation == "a really cool string"

    bmlt_meeting.location_nation = None
    db_meeting, _ = bmlt_meeting.to_db(db, cache)
    db.add(db_meeting)
    db.flush()


def test_bmlt_meeting_to_db_location_postal_code_1(db: Session, cache: SnapshotCache, service_body_1: ServiceBody):
    bmlt_meeting = get_mock_bmlt_meeting()
    bmlt_meeting.service_body_bigint = service_body_1.bmlt_id

    bmlt_meeting.location_postal_code_1 = "a really cool string"
    db_meeting, _ = bmlt_meeting.to_db(db, cache)
    db.add(db_meeting)
    db.flush()
    assert db_meeting.location_postal_code_1 == "a really cool string"

    bmlt_meeting.location_postal_code_1 = None
    db_meeting, _ = bmlt_meeting.to_db(db, cache)
    db.add(db_meeting)
    db.flush()


def test_bmlt_meeting_to_db_location_province(db: Session, cache: SnapshotCache, service_body_1: ServiceBody):
    bmlt_meeting = get_mock_bmlt_meeting()
    bmlt_meeting.service_body_bigint = service_body_1.bmlt_id

    bmlt_meeting.location_province = "a really cool string"
    db_meeting, _ = bmlt_meeting.to_db(db, cache)
    db.add(db_meeting)
    db.flush()
    assert db_meeting.location_province == "a really cool string"

    bmlt_meeting.location_province = None
    db_meeting, _ = bmlt_meeting.to_db(db, cache)
    db.add(db_meeting)
    db.flush()


def test_bmlt_meeting_to_db_location_sub_province(db: Session, cache: SnapshotCache, service_body_1: ServiceBody):
    bmlt_meeting = get_mock_bmlt_meeting()
    bmlt_meeting.service_body_bigint = service_body_1.bmlt_id

    bmlt_meeting.location_sub_province = "a really cool string"
    db_meeting, _ = bmlt_meeting.to_db(db, cache)
    db.add(db_meeting)
    db.flush()
    assert db_meeting.location_sub_province == "a really cool string"

    bmlt_meeting.location_sub_province = None
    db_meeting, _ = bmlt_meeting.to_db(db, cache)
    db.add(db_meeting)
    db.flush()


def test_bmlt_meeting_to_db_location_municipality(db: Session, cache: SnapshotCache, service_body_1: ServiceBody):
    bmlt_meeting = get_mock_bmlt_meeting()
    bmlt_meeting.service_body_bigint = service_body_1.bmlt_id

    bmlt_meeting.location_municipality = "a really cool string"
    db_meeting, _ = bmlt_meeting.to_db(db, cache)
    db.add(db_meeting)
    db.flush()
    assert db_meeting.location_municipality == "a really cool string"

    bmlt_meeting.location_municipality = None
    db_meeting, _ = bmlt_meeting.to_db(db, cache)
    db.add(db_meeting)
    db.flush()


def test_bmlt_meeting_to_db_location_neighborhood(db: Session, cache: SnapshotCache, service_body_1: ServiceBody):
    bmlt_meeting = get_mock_bmlt_meeting()
    bmlt_meeting.service_body_bigint = service_body_1.bmlt_id

    bmlt_meeting.location_neighborhood = "a really cool string"
    db_meeting, _ = bmlt_meeting.to_db(db, cache)
    db.add(db_meeting)
    db.flush()
    assert db_meeting.location_neighborhood == "a really cool string"

    bmlt_meeting.location_neighborhood = None
    db_meeting, _ = bmlt_meeting.to_db(db, cache)
    db.add(db_meeting)
    db.flush()


def test_bmlt_meeting_to_db_location_street(db: Session, cache: SnapshotCache, service_body_1: ServiceBody):
    bmlt_meeting = get_mock_bmlt_meeting()
    bmlt_meeting.service_body_bigint = service_body_1.bmlt_id

    bmlt_meeting.location_street = "a really cool string"
    db_meeting, _ = bmlt_meeting.to_db(db, cache)
    db.add(db_meeting)
    db.flush()
    assert db_meeting.location_street == "a really cool string"

    bmlt_meeting.location_street = None
    db_meeting, _ = bmlt_meeting.to_db(db, cache)
    db.add(db_meeting)
    db.flush()


def test_bmlt_meeting_to_db_location_info(db: Session, cache: SnapshotCache, service_body_1: ServiceBody):
    bmlt_meeting = get_mock_bmlt_meeting()
    bmlt_meeting.service_body_bigint = service_body_1.bmlt_id

    bmlt_meeting.location_info = "a really cool string"
    db_meeting, _ = bmlt_meeting.to_db(db, cache)
    db.add(db_meeting)
    db.flush()
    assert db_meeting.location_info == "a really cool string"

    bmlt_meeting.location_info = None
    db_meeting, _ = bmlt_meeting.to_db(db, cache)
    db.add(db_meeting)
    db.flush()


def test_bmlt_meeting_to_db_location_text(db: Session, cache: SnapshotCache, service_body_1: ServiceBody):
    bmlt_meeting = get_mock_bmlt_meeting()
    bmlt_meeting.service_body_bigint = service_body_1.bmlt_id

    bmlt_meeting.location_text = "a really cool string"
    db_meeting, _ = bmlt_meeting.to_db(db, cache)
    db.add(db_meeting)
    db.flush()
    assert db_meeting.location_text == "a really cool string"

    bmlt_meeting.location_text = None
    db_meeting, _ = bmlt_meeting.to_db(db, cache)
    db.add(db_meeting)
    db.flush()


def test_bmlt_meeting_to_db_bus_lines(db: Session, cache: SnapshotCache, service_body_1: ServiceBody):
    bmlt_meeting = get_mock_bmlt_meeting()
    bmlt_meeting.service_body_bigint = service_body_1.bmlt_id

    bmlt_meeting.bus_lines = "a really cool string"
    db_meeting, _ = bmlt_meeting.to_db(db, cache)
    db.add(db_meeting)
    db.flush()
    assert db_meeting.bus_lines == "a really cool string"

    bmlt_meeting.bus_lines = None
    db_meeting, _ = bmlt_meeting.to_db(db, cache)
    db.add(db_meeting)
    db.flush()


def test_bmlt_meeting_to_db_train_lines(db: Session, cache: SnapshotCache, service_body_1: ServiceBody):
    bmlt_meeting = get_mock_bmlt_meeting()
    bmlt_meeting.service_body_bigint = service_body_1.bmlt_id

    bmlt_meeting.train_lines = "a really cool string"
    db_meeting, _ = bmlt_meeting.to_db(db, cache)
    db.add(db_meeting)
    db.flush()
    assert db_meeting.train_lines == "a really cool string"

    bmlt_meeting.train_lines = None
    db_meeting, _ = bmlt_meeting.to_db(db, cache)
    db.add(db_meeting)
    db.flush()


def test_bmlt_meeting_to_db_published(db: Session, cache: SnapshotCache, service_body_1: ServiceBody):
    bmlt_meeting = get_mock_bmlt_meeting()
    bmlt_meeting.service_body_bigint = service_body_1.bmlt_id

    bmlt_meeting.published = True
    db_meeting, _ = bmlt_meeting.to_db(db, cache)
    db.add(db_meeting)
    db.flush()
    assert db_meeting.published is True

    bmlt_meeting.published = False
    db_meeting, _ = bmlt_meeting.to_db(db, cache)
    db.add(db_meeting)
    db.flush()
    assert db_meeting.published is False

    with pytest.raises(IntegrityError):
        bmlt_meeting.published = None
        db_meeting, _ = bmlt_meeting.to_db(db, cache)
        db.add(db_meeting)
        db.flush()


def test_bmlt_meeting_to_db_world_id(db: Session, cache: SnapshotCache, service_body_1: ServiceBody):
    bmlt_meeting = get_mock_bmlt_meeting()
    bmlt_meeting.service_body_bigint = service_body_1.bmlt_id

    bmlt_meeting.worldid_mixed = "G00013329"
    db_meeting, _ = bmlt_meeting.to_db(db, cache)
    db.add(db_meeting)
    db.flush()
    assert db_meeting.world_id == "G00013329"

    bmlt_meeting.worldid_mixed = None
    db_meeting, _ = bmlt_meeting.to_db(db, cache)
    db.add(db_meeting)
    db.flush()


def test_bmlt_meeting_to_db_naws_code(db: Session, cache: SnapshotCache, service_body_1: ServiceBody):
    bmlt_meeting = get_mock_bmlt_meeting()
    bmlt_meeting.service_body_bigint = service_body_1.bmlt_id

    db_meeting, _ = bmlt_meeting.to_db(db, cache)
    bmlt_meeting = get_mock_bmlt_meeting()
    bmlt_meeting.service_body_bigint = service_body_1.bmlt_id
    assert db_meeting.meeting_naws_code_id is None

    naws_code = MeetingNawsCode(root_server_id=cache.snapshot.root_server_id, bmlt_id=bmlt_meeting.id_bigint)
    db.add(naws_code)
    db.flush()
    db.refresh(naws_code)

    cache.clear()
    db_meeting, _ = bmlt_meeting.to_db(db, cache)
    db.add(db_meeting)
    db.flush()
    db.refresh(db_meeting)
    assert db_meeting.meeting_naws_code_id == naws_code.id
    assert db_meeting.naws_code == naws_code


def test_bmlt_meeting_to_db_meeting_formats(db: Session, cache: SnapshotCache, service_body_1: ServiceBody):
    bmlt_meeting = get_mock_bmlt_meeting()
    bmlt_meeting.service_body_bigint = service_body_1.bmlt_id
    db_format_1 = crud.create_format(db, cache.snapshot.id, 1, "O")
    db_format_2 = crud.create_format(db, cache.snapshot.id, 2, "BEG")

    bmlt_meeting.format_shared_id_list = [1, 2, 3]
    db_meeting, db_meeting_formats = bmlt_meeting.to_db(db, cache)
    assert len(db_meeting_formats) == 2
    db.add(db_meeting)
    db.add_all(db_meeting_formats)
    db.flush()
    db.refresh(db_meeting)
    assert len(db_meeting.meeting_formats) == 2
    assert db_meeting.meeting_formats[0].format == db_format_1
    assert db_meeting.meeting_formats[1].format == db_format_2


def test_save_meetings(db: Session, snapshot_1: Snapshot):
    db_sb_1 = crud.create_service_body(db, snapshot_1.id, 1, "sb name", "AS")
    db_format_1 = crud.create_format(db, snapshot_1.id, 1, "O")
    db_format_2 = crud.create_format(db, snapshot_1.id, 2, "BEG")

    bmlt_meeting = get_mock_bmlt_meeting()
    bmlt_meeting.service_body_bigint = db_sb_1.bmlt_id
    bmlt_meeting.format_shared_id_list = [1, 2]
    bmlt_meetings = [bmlt_meeting]

    save_meetings(db, snapshot_1, bmlt_meetings)
    assert db.query(Meeting).filter(Meeting.snapshot == snapshot_1).count() == 1
    db_meeting = db.query(Meeting).first()
    assert db_meeting.meeting_formats[0].meeting == db_meeting
    assert db_meeting.meeting_formats[0].format == db_format_1
    assert db_meeting.meeting_formats[1].meeting == db_meeting
    assert db_meeting.meeting_formats[1].format == db_format_2
