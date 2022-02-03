from dijon import crud, schemas
from dijon.conftest import Ctx


def test_list_root_servers(ctx: Ctx):
    response = ctx.client.get("/rootservers")
    assert response.status_code == 200
    data = response.json()
    assert data == []

    rs_1 = crud.create_root_server(ctx.db, "root 1", "https://1/main_server/")
    rs_2 = crud.create_root_server(ctx.db, "root 2", "https://2/main_server/")

    response = ctx.client.get("/rootservers")
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 2
    assert data[0]["id"] == rs_1.id
    assert data[0]["name"] == rs_1.name
    assert data[0]["url"] == rs_1.url
    assert data[1]["id"] == rs_2.id
    assert data[1]["name"] == rs_2.name
    assert data[1]["url"] == rs_2.url


def test_get_root_server(ctx: Ctx):
    rs_1 = crud.create_root_server(ctx.db, "root 1", "https://1/main_server/")

    response = ctx.client.get(f"/rootservers/{rs_1.id}")
    assert response.status_code == 200
    data = response.json()
    assert data["id"] == rs_1.id
    assert data["name"] == rs_1.name
    assert data["url"] == rs_1.url

    response = ctx.client.get(f"/rootservers/{rs_1.id + 1}")
    assert response.status_code == 404


def test_create_root_server(ctx: Ctx):
    response = ctx.client.post(
        "/rootservers",
        json=schemas.RootServerCreate(
            name="SEZF",
            url="https://reallygoodurl/main_server"
        ).dict()
    )
    assert response.status_code == 201
    data = response.json()
    assert isinstance(data["id"], int)
    assert data["name"] == "SEZF"
    assert data["url"] == "https://reallygoodurl/main_server/"


def test_delete_root_server(ctx: Ctx):
    rs_1 = crud.create_root_server(ctx.db, "root 1", "https://1/main_server/")

    response = ctx.client.delete(f"/rootservers/{rs_1.id}")
    assert response.status_code == 204
    response = ctx.client.delete(f"/rootservers/{rs_1.id}")
    assert response.status_code == 404
