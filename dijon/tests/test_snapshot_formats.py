import pytest
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from dijon import crud
from dijon.models import Format, FormatNawsCode, RootServer, Snapshot
from dijon.snapshot import BmltFormat, save_formats


@pytest.fixture
def root_server_1(db: Session) -> RootServer:
    return crud.create_root_server(db, "root name", "https://blah/main_server/")


@pytest.fixture
def snapshot_1(db: Session, root_server_1: RootServer) -> Snapshot:
    return crud.create_snapshot(db, root_server_1)


def get_mock_raw_format() -> dict[str, str]:
    return {
        "key_string": "BEG",
        "name_string": "Beginners",
        "description_string": "This meeting is focused on the needs of new members of NA.",
        "lang": "en",
        "id": "1",
        "world_id": "BEG",
        "root_server_uri": "https://bmlt.sezf.org/main_server",
        "format_type_enum": "FC3"
    }


def get_mock_bmlt_format() -> BmltFormat:
    raw_format = get_mock_raw_format()
    return BmltFormat(**raw_format)


def test_parse_raw_format_id():
    mock_format = get_mock_raw_format()
    mock_format["id"] = "123"
    bmlt_format = BmltFormat(**mock_format)
    assert bmlt_format.id == 123

    with pytest.raises(ValueError):
        mock_format["id"] = ""
        BmltFormat(**mock_format)

    with pytest.raises(ValueError):
        del mock_format["id"]
        BmltFormat(**mock_format)


def test_parse_raw_format_key_string():
    mock_format = get_mock_raw_format()
    mock_format["key_string"] = "O"
    bmlt_format = BmltFormat(**mock_format)
    assert bmlt_format.key_string == "O"

    with pytest.raises(ValueError):
        mock_format["key_string"] = ""
        BmltFormat(**mock_format)

    with pytest.raises(ValueError):
        del mock_format["key_string"]
        BmltFormat(**mock_format)


def test_parse_raw_format_name_string():
    mock_format = get_mock_raw_format()
    mock_format["name_string"] = "Beginners"
    bmlt_format = BmltFormat(**mock_format)
    assert bmlt_format.name_string == "Beginners"

    mock_format["name_string"] = ""
    bmlt_format = BmltFormat(**mock_format)
    assert bmlt_format.name_string is None

    del mock_format["name_string"]
    bmlt_format = BmltFormat(**mock_format)
    assert bmlt_format.name_string is None


def test_parse_raw_format_world_id():
    mock_format = get_mock_raw_format()
    mock_format["world_id"] = "BEG"
    bmlt_format = BmltFormat(**mock_format)
    assert bmlt_format.world_id == "BEG"

    mock_format["world_id"] = ""
    bmlt_format = BmltFormat(**mock_format)
    assert bmlt_format.world_id is None

    del mock_format["world_id"]
    bmlt_format = BmltFormat(**mock_format)
    assert bmlt_format.world_id is None


def test_bmlt_format_to_db_bmlt_id(db: Session, snapshot_1: Snapshot):
    bmlt_format = get_mock_bmlt_format()
    bmlt_format.id = 123
    db_format = bmlt_format.to_db(db, snapshot_1)
    assert db_format.bmlt_id == 123
    db.add(db_format)
    db.flush()

    with pytest.raises(IntegrityError):
        bmlt_format.id = None
        db_sb = bmlt_format.to_db(db, snapshot_1)
        db.add(db_sb)
        db.flush()


def test_bmlt_format_to_db_key_string(db: Session, snapshot_1: Snapshot):
    bmlt_format = get_mock_bmlt_format()
    bmlt_format.key_string = "O"
    db_format = bmlt_format.to_db(db, snapshot_1)
    assert db_format.key_string == "O"
    db.add(db_format)
    db.flush()

    with pytest.raises(IntegrityError):
        bmlt_format.key_string = None
        db_sb = bmlt_format.to_db(db, snapshot_1)
        db.add(db_sb)
        db.flush()


def test_bmlt_format_to_db_name(db: Session, snapshot_1: Snapshot):
    bmlt_format = get_mock_bmlt_format()
    bmlt_format.name_string = "Open"
    db_format = bmlt_format.to_db(db, snapshot_1)
    assert db_format.name == "Open"
    db.add(db_format)
    db.flush()

    bmlt_format.name_string = None
    db_sb = bmlt_format.to_db(db, snapshot_1)
    db.add(db_sb)
    db.flush()


def test_bmlt_format_to_db_world_id(db: Session, snapshot_1: Snapshot):
    bmlt_format = get_mock_bmlt_format()
    bmlt_format.world_id = "BEG"
    db_format = bmlt_format.to_db(db, snapshot_1)
    assert db_format.world_id == "BEG"
    db.add(db_format)
    db.flush()

    bmlt_format.world_id = None
    db_sb = bmlt_format.to_db(db, snapshot_1)
    db.add(db_sb)
    db.flush()


def test_bmlt_format_to_db_naws_code(db: Session, snapshot_1: Snapshot):
    bmlt_format = get_mock_bmlt_format()
    db_format = bmlt_format.to_db(db, snapshot_1)
    assert db_format.format_naws_code_id is None
    db.add(db_format)
    db.flush()

    naws_code = FormatNawsCode(root_server_id=snapshot_1.root_server_id, bmlt_id=bmlt_format.id)
    db.add(naws_code)
    db.flush()
    db.refresh(naws_code)

    db_sb = bmlt_format.to_db(db, snapshot_1)
    db.add(db_sb)
    db.flush()
    db.refresh(db_sb)
    assert db_sb.format_naws_code_id == naws_code.id
    assert db_sb.naws_code == naws_code


def test_save_formats(db: Session, snapshot_1: Snapshot):
    bmlt_format_1 = get_mock_bmlt_format()
    bmlt_format_1.id = 1
    bmlt_format_1.key_string = "O"
    bmlt_format_1.name_string = "Open"

    bmlt_format_2 = get_mock_bmlt_format()
    bmlt_format_2.id = 2
    bmlt_format_2.key_string = "C"
    bmlt_format_2.name_string = "Closed"

    bmlt_format_3 = get_mock_bmlt_format()
    bmlt_format_3.id = 3
    bmlt_format_3.key_string = "BEG"
    bmlt_format_3.name_string = "Beginners"

    bmlt_formats = [
        bmlt_format_1,
        bmlt_format_2,
        bmlt_format_3
    ]

    save_formats(db, snapshot_1, bmlt_formats)
    assert db.query(Format).filter(Format.snapshot == snapshot_1).count() == 3
