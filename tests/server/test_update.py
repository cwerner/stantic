import time
from typing import Any

import pytest

from stantic.models import Datastream, ObservedProperty, Sensor, Thing
from stantic.server import Server

# ============================================================================
# test update functionality


def test_server_update_thing(server_with_data: Server):
    thing = server_with_data.get(Thing, id=2)
    thing.description = thing.description + " (updated)"
    server_with_data.update(thing)
    thing_updated = server_with_data.get(Thing, id=2)
    assert "(updated)" in thing_updated.description


def test_server_update_thing_without_id(server_with_data: Server):
    thing = server_with_data.get(Thing, id=2)
    thing.id = None
    thing.description = thing.description + " (updated)"
    with pytest.raises(ValueError):
        server_with_data.update(thing)


def test_server_update_thing_with_field(server_with_data: Server):
    thing = server_with_data.get(Thing, id=2)
    server_with_data.update_field(
        Thing, id=2, payload={"description": thing.description + " (updated again)"}
    )
    thing_updated = server_with_data.get(Thing, id=2)
    assert "(updated again)" in thing_updated.description


def test_server_update_thing_with_invalid_field(server_with_data: Server):
    with pytest.raises(ValueError):
        server_with_data.update_field(Thing, id=2, payload={"invalid_field": "invalid"})
