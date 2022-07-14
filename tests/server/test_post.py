import time
from typing import Any

import pytest

from stantic.models import (
    Datastream,
    ObservedProperty,
    Person,
    Sensor,
    SensorProperty,
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


@pytest.fixture
def sensor():
    return Sensor(
        name="random",
        description="A new sensor called Random",
        encodingType="defaultencoding",
        metadata="some_metadata",
        properties=SensorProperty(
            pid="1234",
            sn="ABC1234",
            offset=0.0,
        ),
    )


def test_server_post_thing(server_with_cleandata: Server, thing: Thing):
    server_with_cleandata.post(thing)
    thing = server_with_cleandata.get(Thing, search="random")
    assert thing is not None


def test_server_post_sensor(server_with_cleandata: Server, sensor: Sensor):
    server_with_cleandata.post(sensor)
    sensor = server_with_cleandata.get(Sensor, search="random")
    assert sensor is not None


def test_server_post_thing_strict_off(server_with_cleandata: Server, thing: Thing):
    server_with_cleandata.post(thing, strict=False)
    thing.id = None
    server_with_cleandata.post(thing, strict=False)
    things = server_with_cleandata.get(Thing, search="random")
    assert len(set([thing.id for thing in things])) > 1


def test_server_post_thing_strict_on(server_with_cleandata: Server, thing: Thing):
    server_with_cleandata.post(thing, strict=True)
    with pytest.raises(ValueError):
        server_with_cleandata.post(thing, strict=True)
