import time
from typing import Any

import pytest

from stantic.models import (
    Datastream,
    ObservedProperty,
    Person,
    Sensor,
    Thing,
    ThingProperty,
)
from stantic.server import Server

# ============================================================================
# test update functionality


@pytest.fixture
def thing():
    return Thing(
        name="random",
        description="A new field station called Random",
        properties=ThingProperty(
            id="random",
            project="unknown",
            station_id="RAN",
            tech_person=Person(
                name="John Doe", email="j.doe@inc.com", affiliation="INC.com"
            ),
            science_person=Person(
                name="John Doe", email="j.doe@inc.com", affiliation="INC.com"
            ),
            data_person=Person(
                name="John Doe", email="j.doe@inc.com", affiliation="INC.com"
            ),
            qaqc_person=Person(
                name="John Doe", email="j.doe@inc.com", affiliation="INC.com"
            ),
        ),
    )


def test_server_post_thing(server_with_data: Server, thing: Thing):
    thing = server_with_data.post(thing)
    server_with_data.get(Thing, search="random")


def test_server_post_thing_strict_off(server_with_data: Server, thing: Thing):
    thing = server_with_data.post(thing, strict=False)
    thing_from_server = server_with_data.get(Thing, search="random")
    assert thing_from_server[0].id != thing.id


def test_server_post_thing_strict_on(server_with_data: Server, thing: Thing):
    with pytest.raises(ValueError):
        thing = server_with_data.post(thing, strict=True)


def test_server_delete_thing(server_with_data: Server):
    thing = server_with_data.get(Thing, search="random")
    server_with_data.delete(Thing, id=thing[0].id)


@pytest.mark.skip("This fails with a 'POST only allowed for Collections' 400 error?")
def test_server_delete_things_with_search(server_with_data: Server, thing: Thing):
    server_with_data.post(thing, strict=False)
    server_with_data.post(thing, strict=False)
    server_with_data.post(thing, strict=False)
    server_with_data.delete(Thing, search="random")
