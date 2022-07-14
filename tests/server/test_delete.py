import copy

import pytest

from stantic.models import Person, Thing, ThingProperty
from stantic.server import Server


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


def test_server_delete_thing(server_with_cleandata: Server, thing: Thing):
    thing = server_with_cleandata.post(thing)
    server_with_cleandata.delete(Thing, id=thing.id)

    things_after_delete = server_with_cleandata.get(Thing, search="random")
    assert bool(things_after_delete) is False


# @pytest.mark.skip("This fails with a 'POST only allowed for Collections' 400 error?")
def test_server_delete_all_things(server_with_cleandata: Server):
    server_with_cleandata.delete(Thing)
    things_after_delete = server_with_cleandata.get(Thing)
    assert bool(things_after_delete) is False
