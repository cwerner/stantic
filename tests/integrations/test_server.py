import datetime
import json
import time

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


# ============================================================================
# test get functionality

# def test_server_get_thing_total_count(server_with_data: Server):
#     things = server_with_data.get(Thing)
#     assert len(things) == 2
#     assert all([isinstance(t, Thing) for t in things])


# def test_server_get_thing_specific_id(server_with_data: Server):
#     thing = server_with_data.get(Thing, id=2)

#     assert type(thing) is not list
#     assert isinstance(thing, Thing)


# def test_server_get_thing_search(server_with_data: Server):
#     thing = server_with_data.get(Thing, search="Graswang")

#     assert type(thing) is not list
#     assert isinstance(thing, Thing)


# def test_server_get_thing_partial_search(server_with_data: Server):
#     thing = server_with_data.get(Thing, search="Gras")

#     assert type(thing) is not list
#     assert isinstance(thing, Thing)


# @pytest.mark.usefixtures("server_with_data")
# def test_server_add_data(server_with_data: Server):
#     print("\nWasting time...")
#     for i in range(4):
#         print(i)
#         time.sleep(2)

#     things = server_with_data.get(Thing)


#     streams = server_with_data.get(Datastream)
