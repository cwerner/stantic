import time
from typing import Any

import pytest

from stantic.models import Datastream, ObservedProperty, Sensor, Thing
from stantic.server import Server

# ============================================================================
# test get functionality


@pytest.mark.parametrize("entity", (Datastream, Thing, Sensor))
def test_server_get_entities(server_with_data: Server, entity: Any):
    res = server_with_data.get(entity)
    res = [res] if not isinstance(res, list) else res
    assert all([isinstance(r, entity) for r in res])


def test_server_check_datastream_backlinks(server_with_data: Server):
    things = server_with_data.get(Thing)
    sensors = server_with_data.get(Sensor)
    observed_properties = server_with_data.get(ObservedProperty)

    for datastream in server_with_data.get(Datastream):
        assert (datastream.Thing.id in [t.id for t in things]) is True
        assert (datastream.Sensor.id in [s.id for s in sensors]) is True
        assert (
            datastream.ObservedProperty.id in [op.id for op in observed_properties]
        ) is True


def test_server_get_thing_total_count(server_with_data: Server):
    things = server_with_data.get(Thing)

    assert len(things) == 2
    assert all([isinstance(t, Thing) for t in things])


def test_server_get_thing_specific_id(server_with_data: Server):
    thing = server_with_data.get(Thing, id=2)

    assert isinstance(thing, Thing)
    assert thing.id == 2


@pytest.mark.parametrize("searchterm", ("Graswang", "Gras"))
def test_server_get_thing_search(server_with_data: Server, searchterm: str):
    thing = server_with_data.get(Thing, search=searchterm)

    assert isinstance(thing, Thing)
    assert thing.name == "Graswang"


def test_server_get_thing_search_no_hits(server_with_data: Server):
    thing_id = server_with_data.get(Thing, id=999)  # search="ImaginarySite")
    thing_search = server_with_data.get(Thing, search="ImaginarySite")

    assert len(thing_id) == 0
    assert len(thing_search) == 0
