from typing import Optional

import pytest
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from dijon import crud
from dijon.models import RootServer, ServiceBody, ServiceBodyNawsCode, Snapshot
from dijon.snapshot import BmltServiceBody, save_service_bodies


@pytest.fixture
def root_server_1(db: Session) -> RootServer:
    return crud.create_root_server(db, "root name", "https://blah/main_server/")


@pytest.fixture
def snapshot_1(db: Session, root_server_1: RootServer) -> Snapshot:
    return crud.create_snapshot(db, root_server_1)


def get_mock_raw_service_body() -> dict[str, str]:
    return {
        "id": "9",
        "parent_id": "20",
        "name": "Unity Springs Area",
        "description": "Unity Springs Area",
        "type": "AS",
        "url": "http://www.unityspringsna.org",
        "helpline": "(866) 418-1683",
        "world_id": "AR63340"
    }


def get_mock_bmlt_service_body() -> BmltServiceBody:
    raw_sb = get_mock_raw_service_body()
    return BmltServiceBody(**raw_sb)


def test_parse_raw_service_body_id():
    mock_sb = get_mock_raw_service_body()
    mock_sb["id"] = "123"
    bmlt_sb = BmltServiceBody(**mock_sb)
    assert bmlt_sb.id == 123

    with pytest.raises(ValueError):
        mock_sb["id"] = ""
        BmltServiceBody(**mock_sb)

    with pytest.raises(ValueError):
        del mock_sb["id"]
        BmltServiceBody(**mock_sb)


def test_parse_raw_service_body_parent_id():
    mock_sb = get_mock_raw_service_body()
    mock_sb["parent_id"] = "123"
    bmlt_sb = BmltServiceBody(**mock_sb)
    assert bmlt_sb.parent_id == 123

    with pytest.raises(ValueError):
        mock_sb["parent_id"] = ""
        BmltServiceBody(**mock_sb)

    with pytest.raises(ValueError):
        del mock_sb["parent_id"]
        BmltServiceBody(**mock_sb)


def test_parse_raw_service_body_name():
    mock_sb = get_mock_raw_service_body()
    mock_sb["name"] = "Georgia Region"
    bmlt_sb = BmltServiceBody(**mock_sb)
    assert bmlt_sb.name == "Georgia Region"

    with pytest.raises(ValueError):
        mock_sb["name"] = ""
        BmltServiceBody(**mock_sb)

    with pytest.raises(ValueError):
        del mock_sb["name"]
        BmltServiceBody(**mock_sb)


def test_parse_raw_service_body_type():
    mock_sb = get_mock_raw_service_body()
    mock_sb["type"] = "AS"
    bmlt_sb = BmltServiceBody(**mock_sb)
    assert bmlt_sb.type == "AS"

    with pytest.raises(ValueError):
        mock_sb["type"] = ""
        BmltServiceBody(**mock_sb)

    with pytest.raises(ValueError):
        del mock_sb["type"]
        BmltServiceBody(**mock_sb)


def test_parse_raw_service_body_url():
    mock_sb = get_mock_raw_service_body()
    mock_sb["url"] = "https://blah"
    bmlt_sb = BmltServiceBody(**mock_sb)
    assert bmlt_sb.url == "https://blah"

    mock_sb["url"] = ""
    bmlt_sb = BmltServiceBody(**mock_sb)
    assert bmlt_sb.url is None

    del mock_sb["url"]
    bmlt_sb = BmltServiceBody(**mock_sb)
    assert bmlt_sb.url is None


def test_parse_raw_service_body_helpline():
    mock_sb = get_mock_raw_service_body()
    mock_sb["helpline"] = "5555555555"
    bmlt_sb = BmltServiceBody(**mock_sb)
    assert bmlt_sb.helpline == "5555555555"

    mock_sb["helpline"] = ""
    bmlt_sb = BmltServiceBody(**mock_sb)
    assert bmlt_sb.helpline is None

    del mock_sb["helpline"]
    bmlt_sb = BmltServiceBody(**mock_sb)
    assert bmlt_sb.helpline is None


def test_parse_raw_service_body_description():
    mock_sb = get_mock_raw_service_body()
    mock_sb["description"] = "long description"
    bmlt_sb = BmltServiceBody(**mock_sb)
    assert bmlt_sb.description == "long description"

    mock_sb["description"] = ""
    bmlt_sb = BmltServiceBody(**mock_sb)
    assert bmlt_sb.description is None

    del mock_sb["description"]
    bmlt_sb = BmltServiceBody(**mock_sb)
    assert bmlt_sb.description is None


def test_parse_raw_service_body_world_id():
    mock_sb = get_mock_raw_service_body()
    mock_sb["world_id"] = "AR63340"
    bmlt_sb = BmltServiceBody(**mock_sb)
    assert bmlt_sb.world_id == "AR63340"

    mock_sb["world_id"] = ""
    bmlt_sb = BmltServiceBody(**mock_sb)
    assert bmlt_sb.world_id is None

    del mock_sb["world_id"]
    bmlt_sb = BmltServiceBody(**mock_sb)
    assert bmlt_sb.world_id is None


def test_bmlt_service_body_to_db_bmlt_id(db: Session, snapshot_1: Snapshot):
    bmlt_sb = get_mock_bmlt_service_body()
    bmlt_sb.id = 123
    db_sb = bmlt_sb.to_db(db, snapshot_1)
    assert db_sb.bmlt_id == 123
    db.add(db_sb)
    db.flush()

    with pytest.raises(IntegrityError):
        bmlt_sb.id = None
        db_sb = bmlt_sb.to_db(db, snapshot_1)
        db.add(db_sb)
        db.flush()


def test_bmlt_service_body_to_db_bmlt_parent_id(db: Session, snapshot_1: Snapshot):
    bmlt_sb = get_mock_bmlt_service_body()
    db_sb = bmlt_sb.to_db(db, snapshot_1)
    assert db_sb.parent_id is None
    db.add(db_sb)
    db.flush()


def test_bmlt_service_body_to_db_bmlt_name(db: Session, snapshot_1: Snapshot):
    bmlt_sb = get_mock_bmlt_service_body()
    bmlt_sb.name = "cool name"
    db_sb = bmlt_sb.to_db(db, snapshot_1)
    assert db_sb.name == "cool name"
    db.add(db_sb)
    db.flush()

    with pytest.raises(IntegrityError):
        bmlt_sb.name = None
        db_sb = bmlt_sb.to_db(db, snapshot_1)
        db.add(db_sb)
        db.flush()


def test_bmlt_service_body_to_db_type(db: Session, snapshot_1: Snapshot):
    bmlt_sb = get_mock_bmlt_service_body()
    bmlt_sb.type = "AS"
    db_sb = bmlt_sb.to_db(db, snapshot_1)
    assert db_sb.type == "AS"
    db.add(db_sb)
    db.flush()

    with pytest.raises(IntegrityError):
        bmlt_sb.type = None
        db_sb = bmlt_sb.to_db(db, snapshot_1)
        db.add(db_sb)
        db.flush()


def test_bmlt_service_body_to_db_description(db: Session, snapshot_1: Snapshot):
    bmlt_sb = get_mock_bmlt_service_body()
    bmlt_sb.description = "cool desc"
    db_sb = bmlt_sb.to_db(db, snapshot_1)
    assert db_sb.description == "cool desc"
    db.add(db_sb)
    db.flush()

    bmlt_sb.description = None
    db_sb = bmlt_sb.to_db(db, snapshot_1)
    assert db_sb.description is None
    db.add(db_sb)
    db.flush()


def test_bmlt_service_body_to_db_url(db: Session, snapshot_1: Snapshot):
    bmlt_sb = get_mock_bmlt_service_body()
    bmlt_sb.url = "https://blah"
    db_sb = bmlt_sb.to_db(db, snapshot_1)
    assert db_sb.url == "https://blah"
    db.add(db_sb)
    db.flush()

    bmlt_sb.url = None
    db_sb = bmlt_sb.to_db(db, snapshot_1)
    assert db_sb.url is None
    db.add(db_sb)
    db.flush()


def test_bmlt_service_body_to_db_helpline(db: Session, snapshot_1: Snapshot):
    bmlt_sb = get_mock_bmlt_service_body()
    bmlt_sb.helpline = "5555555555"
    db_sb = bmlt_sb.to_db(db, snapshot_1)
    assert db_sb.helpline == "5555555555"
    db.add(db_sb)
    db.flush()

    bmlt_sb.helpline = None
    db_sb = bmlt_sb.to_db(db, snapshot_1)
    assert db_sb.helpline is None
    db.add(db_sb)
    db.flush()


def test_bmlt_service_body_to_db_world_id(db: Session, snapshot_1: Snapshot):
    bmlt_sb = get_mock_bmlt_service_body()
    bmlt_sb.world_id = "AR63340"
    db_sb = bmlt_sb.to_db(db, snapshot_1)
    assert db_sb.world_id == "AR63340"
    db.add(db_sb)
    db.flush()

    bmlt_sb.world_id = None
    db_sb = bmlt_sb.to_db(db, snapshot_1)
    assert db_sb.world_id is None
    db.add(db_sb)
    db.flush()


def test_bmlt_service_body_to_db_naws_code(db: Session, snapshot_1: Snapshot):
    bmlt_sb = get_mock_bmlt_service_body()
    db_sb = bmlt_sb.to_db(db, snapshot_1)
    assert db_sb.service_body_naws_code_id is None
    db.add(db_sb)
    db.flush()

    naws_code = ServiceBodyNawsCode(root_server_id=snapshot_1.root_server_id, bmlt_id=bmlt_sb.id)
    db.add(naws_code)
    db.flush()
    db.refresh(naws_code)

    db_sb = bmlt_sb.to_db(db, snapshot_1)
    db.add(db_sb)
    db.flush()
    db.refresh(db_sb)
    assert db_sb.service_body_naws_code_id == naws_code.id
    assert db_sb.naws_code == naws_code


def test_save_service_bodies(db: Session, snapshot_1: Snapshot):
    bmlt_sb_1 = get_mock_bmlt_service_body()
    bmlt_sb_1.id = 1
    bmlt_sb_1.parent_id = 0

    bmlt_sb_2 = get_mock_bmlt_service_body()
    bmlt_sb_2.id = 2
    bmlt_sb_2.parent_id = 1

    bmlt_sb_3 = get_mock_bmlt_service_body()
    bmlt_sb_3.id = 3
    bmlt_sb_3.parent_id = 1

    bmlt_sb_4 = get_mock_bmlt_service_body()
    bmlt_sb_4.id = 4
    bmlt_sb_4.parent_id = 3

    bmlt_sb_5 = get_mock_bmlt_service_body()
    bmlt_sb_5.id = 5
    bmlt_sb_5.parent_id = 0

    bmlt_sb_6 = get_mock_bmlt_service_body()
    bmlt_sb_6.id = 6
    bmlt_sb_6.parent_id = 5

    bmlt_service_bodies = [
        bmlt_sb_1,
        bmlt_sb_2,
        bmlt_sb_5,
        bmlt_sb_3,
        bmlt_sb_4,
        bmlt_sb_6,
    ]

    save_service_bodies(db, snapshot_1, bmlt_service_bodies)

    def get_sb(bmlt_id: int) -> Optional[ServiceBody]:
        return db.query(ServiceBody).filter(ServiceBody.snapshot_id == snapshot_1.id, ServiceBody.bmlt_id == bmlt_id).first()

    db_sb_1 = get_sb(bmlt_sb_1.id)
    assert db_sb_1.parent_id is None
    assert db_sb_1.parent is None

    db_sb_2 = get_sb(bmlt_sb_2.id)
    assert db_sb_2.parent_id == db_sb_1.id
    assert db_sb_2.parent == db_sb_1

    db_sb_3 = get_sb(bmlt_sb_3.id)
    assert db_sb_3.parent_id == db_sb_1.id
    assert db_sb_3.parent == db_sb_1

    db_sb_4 = get_sb(bmlt_sb_4.id)
    assert db_sb_4.parent_id == db_sb_3.id
    assert db_sb_4.parent == db_sb_3

    db_sb_5 = get_sb(bmlt_sb_5.id)
    assert db_sb_5.parent_id is None
    assert db_sb_5.parent is None

    db_sb_6 = get_sb(bmlt_sb_6.id)
    assert db_sb_6.parent_id == db_sb_5.id
    assert db_sb_6.parent == db_sb_5
