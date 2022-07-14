import datetime
import json
import time
from pathlib import Path
from typing import List, Union

import pytest

from stantic.models import Datastream, Sensor, Thing
from stantic.server import check_if_observations_exist, Server


# @pytest.mark.usefixtures("server")
def test_server_is_alive(server: Server):
    assert server.is_alive is True


def test_check_if_observations_exist(server_with_data: Server):
    valid_date = datetime.datetime(2022, 4, 6, 0, 3, 0)
    invalid_date = datetime.datetime(1922, 1, 1, 0, 10, 0)

    datastream = server_with_data.get(Datastream, id=6)
    test_url = server_with_data._get_endpoint_url(datastream) + "/Observations"

    existing_ids = check_if_observations_exist(test_url, dt=valid_date)
    assert len(existing_ids) == 1
    assert all([type(x) is int for x in existing_ids]) is True

    existing_ids = check_if_observations_exist(test_url, dt=invalid_date)
    assert len(existing_ids) == 0


def test_get_endpoint_url_with_id(server_with_data: Server):
    url = server_with_data._get_endpoint_url(Datastream, id=6)
    assert url == "http://0.0.0.0:8080/FROST-Server/v1.1/Datastreams(6)"


def test_get_endpoint_url_without_id(server_with_data: Server):
    url = server_with_data._get_endpoint_url(Datastream)
    assert url == "http://0.0.0.0:8080/FROST-Server/v1.1/Datastreams"


def test_get_id_from_url(server_with_data: Server):
    valid_url = server_with_data._get_endpoint_url(Datastream, id=6)
    invalid_url = valid_url[:-3]

    id = server_with_data._get_id_from_url(valid_url)
    assert id == 6

    with pytest.raises(ValueError):
        server_with_data._get_id_from_url(invalid_url)


def test_extract_ids_from_navlinks(server_with_data: Server):
    datastream = server_with_data.get(Datastream, id=6)

    data_json_with_ids = server_with_data._extract_ids_from_navlinks(datastream.json())

    # associated thing id should be 2 (datastream with id 6 is from graswang)
    # TODO: make this more robust
    assert json.loads(data_json_with_ids)["Thing"]["id"] == 2


@pytest.mark.parametrize("command", ("test -f db.backup", ["test", "-f", "db.backup"]))
def test_call_container(server_with_cleandata: Server, command: Union[str, List[str]]):

    stdout, stderr, status = server_with_cleandata._call_container(command)
    assert status == 0
    assert stderr == ""
    assert stdout == ""


@pytest.mark.parametrize("command", ("test -f db.backup", ["test", "-f", "db.backup"]))
def test_call_container_without_dockercompose_link(
    server_with_cleandata: Server, command: Union[str, List[str]]
):

    # keep initial status
    status = server_with_cleandata._docker_compose

    server_with_cleandata._docker_compose = None
    with pytest.raises(NotImplementedError):
        server_with_cleandata._call_container(command)

    # revert to initial status
    server_with_cleandata._docker_compose = status


def test_dump_db(server_with_cleandata: Server):
    server_with_cleandata.dump_db(Path("dummy_db.backup"))

    # test that file exists and is of size > 0
    _, _, status = server_with_cleandata._call_container("test -s dummy_db.backup")
    assert status == 0


def test_dump_db_invalid_destination(server_with_cleandata: Server):
    with pytest.raises(FileNotFoundError):
        server_with_cleandata.dump_db(Path("/invalid/path/dummy_db.backup"))


def test_restore_db(server_with_cleandata: Server):
    server_with_cleandata.restore_db(Path("db.backup"))


def test_restore_db_invalid_source(server_with_cleandata: Server):
    with pytest.raises(FileNotFoundError):
        server_with_cleandata.restore_db(Path("/invalid/path/db.backup"))


def test_restore_db_corrupt_source(server_with_cleandata: Server):
    server_with_cleandata._call_container("touch corrupt.backup")
    with pytest.raises(FileNotFoundError):
        server_with_cleandata.restore_db(Path("corrupt.backup"))
