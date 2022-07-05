import time

import pytest

from stantic.models import Datastream, Sensor, Thing
from stantic.server import Server


# @pytest.mark.usefixtures("server")
def test_server_is_alive(server: Server):
    assert server.is_alive is True


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
