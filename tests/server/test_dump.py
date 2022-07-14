import datetime
import time
from pathlib import Path
from typing import Any

import pandas as pd
import pytest

from stantic.models import (
    Datastream,
    Location,
    ObservedProperty,
    Person,
    Sensor,
    Thing,
    ThingProperty,
)
from stantic.server import Server
from stantic.tools import load_data

# ============================================================================
# test data dump functionality


def test_server_dump_all(server_with_cleandata: Server):
    dump = server_with_cleandata.dump()
    valid_entities = [Sensor, Thing, Location, Datastream]

    assert set(valid_entities) == set(dump.keys())
    assert all([len(v) > 0 for _, v in dump.items()]) is True


def test_server_dump_things(server_with_cleandata: Server):
    dump = server_with_cleandata.dump(Thing)
    valid_entities = [Thing]

    assert set(valid_entities) == set(dump.keys())
    assert all([len(v) > 0 for _, v in dump.items()]) is True


def test_server_dump_datastreams_and_sensors(server_with_cleandata: Server):
    dump = server_with_cleandata.dump([Datastream, Sensor])
    valid_entities = [Datastream, Sensor]

    assert set(valid_entities) == set(dump.keys())
    assert all([len(v) > 0 for _, v in dump.items()]) is True
