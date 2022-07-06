import datetime
import os

import pandas as pd
from dotenv import load_dotenv
from pydantic import EmailStr

from stantic.models import *
from stantic.server import Server
from stantic.tools import pretty_print

load_dotenv()

FROST_URL = os.environ.get("FROST_URL", "http://localhost:8093")


# some people
p1 = Person(
    name="Max Mustermann",
    email=EmailStr("max.mustermann@abc.de"),
    affiliation="ABC GmbH",
)

p2 = Person(
    name="Elfriede Ebert",
    email=EmailStr("elfi@hotmail.com"),
    affiliation="-",
)


def clean(server: Server):
    """wipe all (tagged) entries from server"""
    server.delete(Thing)
    server.delete(Sensor)
    server.delete(ObservedProperty)
    server.delete(Location)
    server.delete(Datastream)


def create_basic_layout(server: Server):

    clean(server)

    t1 = Thing(
        name="cw_test_thing",
        description="a test thing",
        properties=ThingProperty(
            id="my_ifu_id",
            project="TERENO",
            station_id="Fendt",
            tech_person=p1,
            science_person=p1,
            data_person=p1,
            qaqc_person=p2,
        ),
    )

    s1 = Sensor(
        name="cw_test_sensor",
        description="a test sensor",
        properties=SensorProperty(
            pid="my_sensor_pid",
            sn="1234567890",
            offset=0,
        ),
        encodingType="???",
        metadata="???",
    )

    op1 = ObservedProperty(
        name="cw_test_observedproperty",
        description="an observed property",
        properties=Property(),
        definition="???",
    )

    # post these entities so they get their ref ids
    t1 = server.post(t1)
    s1 = server.post(s1)
    op1 = server.post(op1)

    # Location -- linked to a thing
    l1 = Location(
        name="cw_my_location",
        description="jada jda jada",
        properties=LocationProperty(),
        location=LocationGeo(coordinates=(13, 45)),
        Things=[Link(id=t1.id)],
    )

    l1 = server.post(l1)

    # Datastream -- linked to a thing, sensor, observedproperty
    d1 = Datastream(
        name="cw_test_datastream",
        description="jada jada jada",
        properties={},
        observationType="http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement",
        unitOfMeasurement=Unit(
            name="percent saturation", symbol=r"%", definition="ucum:percent"
        ),
        Thing=Link(id=t1.id),
        Sensor=Link(id=s1.id),
        ObservedProperty=Link(id=op1.id),
    )

    d1 = server.post(d1)


if __name__ == "__main__":

    server = Server(url=FROST_URL)

    print(f"URL: {server.url}")
    print(f"Alive? {server.is_alive}")

    # create some entities (turned off since this ran already)
    # create_basic_layout(server)

    # get a specific datastream (we know th id)
    d1 = server.get(Datastream, id=118)
    pretty_print(d1)
    print("\n-------------\n")

    # get all datastreams
    ds = server.get(Datastream)
    pretty_print(ds)
    print("\n-------------\n")

    # push observations
    data = pd.DataFrame(
        {"result": [10, 11, 9, 8, 7, 9]},
        index=pd.date_range(start="2/1/2022", periods=6),
    )
    server.push_data(d1, data)

    # pull observations (optionally limit timeperiod)
    df = server.pull_data(
        d1, dt_min=datetime.datetime(2022, 2, 1), dt_max=datetime.datetime(2022, 2, 3)
    )
    print(df)
    print("\n-------------\n")

    # dump all datastreams
    dump = server.dump(Datastream)
    pretty_print(dump)
    print("\n-------------\n")

    # dump everything (except obervations)
    dump = server.dump()
    pretty_print(dump)
