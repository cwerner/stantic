import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd
import pytest
from testcontainers.compose import DockerCompose

from stantic.models import (
    Datastream,
    Link,
    Location,
    LocationGeo,
    LocationProperty,
    ObservedProperty,
    Person,
    Sensor,
    SensorProperty,
    Thing,
    ThingProperty,
    Unit,
)
from stantic.server import Server

COMPOSE_PATH = "."  # the folder containing docker-compose.yml


# TODO : Check where we store our single source of truth for units.
#        Seems it's not part of the datamodel? Maybe source it from a json/ yaml file in S3?
def define_units() -> Dict[str, Unit]:
    print("Defining units...")
    units = {
        "degree_celsius": Unit(
            name="degree_celsius", symbol="°C", definition="degree celsius"
        ),
        "mm": Unit(
            name="mm",
            symbol="mm",
            definition="cumulative precip in mm (usually per day)",
        ),
        "percent": Unit(
            name="percent", symbol="mm d-1", definition="percent (0...100)"
        ),
        "m_per_sec": Unit(
            name="m_per_sec", symbol="m s-1", definition="meter per second"
        ),
        "degree": Unit(name="degree", symbol="deg", definition="direction in degrees"),
        "watt_m2": Unit(name="watt_m2", symbol="W m-2", definition="watts per m2"),
    }
    return units


def setup_datastreams(server: Server) -> None:
    print("Setting up datastreams...")

    units = define_units()
    stream_mapping = {
        "airtemp_avg": ("MET_000_Temperature_raw", units["degree_celsius"]),
        "relhumidity_avg": ("MET_000_RelativeHumidity_raw", units["percent"]),
        "wdavg": ("MET_000_WindDirection_raw", units["degree"]),
        "ramount_avg": ("MET_000_PrecipitationIntensity_raw", units["mm"]),
        "total_avg": ("MET_000_RadiationGlobal_raw", units["watt_m2"]),
    }

    # retrieve observed properties (joined by stations in this case)
    observed_properties = setup_observed_properties(server)
    sensors = setup_sensors(server)

    # retrieve sensors

    # thing.property.station_id = 'FEN'
    # thing.property.id = 'fendt'
    things = server.get(Thing)

    for thing in things:
        site = thing.properties.id
        for _, (abbrev, (name, unit)) in enumerate(stream_mapping.items()):
            stream = Datastream(
                observationType="???",
                unitOfMeasurement=unit,
                name=f"{thing.properties.station_id}_{name}",
                description=f"{abbrev.capitalize()} data for TERENO field station data for {thing.name}",
                Thing=Link(id=thing.id),
                Sensor=Link(id=sensors[site][abbrev].id),
                ObservedProperty=Link(id=observed_properties[abbrev].id),
            )
            stream = server.post(stream)


def setup_sensors(server: Server) -> Dict[str, Dict[str, Sensor]]:
    print("Setting up sensors...")

    sensor_mapping = {
        "airtemp_avg": ("tempsens", "tempsens", "12547273", "SN127AB656"),
        "relhumidity_avg": ("rhprobe", "rhprobe", "12547274", "SNAB645454"),
        "wdavg": ("wind-ac", "wind-ac", "12547275", "SN1234/5656A3"),
        "ramount_avg": ("rainee", "rainee", "12547276", "unknown"),
        "total_avg": ("rad1a", "rad1a", "12547277", "unknown"),
    }

    sensors = {"fendt": {}, "graswang": {}}

    for site in ["fendt", "graswang"]:
        for _, (abbrev, (name, description, pid, sn)) in enumerate(
            sensor_mapping.items()
        ):
            # make pid/ sn different for 2nd site
            if site == "graswang":
                pid = "2" + pid[1:]
                if sn != "unknown":
                    sn = sn[:-1] + str(int(sn[-1]) + 1)

            sensor = Sensor(
                name=name,
                description=description,
                encodingType="???",
                metadata="-",
                properties=SensorProperty(pid=pid, sn=sn, offset=0),
            )
            sensor = server.post(sensor)
            sensors[site][abbrev] = sensor
    return sensors


def setup_observed_properties(server: Server) -> Dict[str, ObservedProperty]:
    print("Setting up observed_properties...")
    observed_property_mapping = {
        "airtemp_avg": ("air temperature", "air temperature", "temp 2m above ground"),
        "relhumidity_avg": ("relative humidity", "relative humidity", "unknown"),
        "wdavg": ("wind direction", "wind direction", "direction in 0-360 deg"),
        "ramount_avg": (
            "cummulative rainfall",
            "cummulative rainfall",
            "total rainfall in specified interval",
        ),
        "total_avg": (
            "incoming global radiation",
            "incoming global radiation",
            "unknown",
        ),
    }

    observed_properties = {}
    for _, (abbrev, (name, description, definition)) in enumerate(
        observed_property_mapping.items()
    ):
        observed_property = ObservedProperty(
            name=name,
            description=description,
            definition=definition,
            properties={},
        )
        observed_property = server.post(observed_property)
        observed_properties[abbrev] = observed_property
    return observed_properties


@dataclass
class Site:
    name: str
    coords: Tuple[float, float]


def setup_system(server: Server) -> None:
    """setup field sites, and all associated entities"""
    print("Setting up system...")

    setup_observed_properties(server)

    sites = [
        Site(name="fendt", coords=(11.06068, 47.83287)),
        Site(name="graswang", coords=(11.03189, 47.5703)),
    ]

    for site in sites:

        abbrev = site.name

        station = Thing(
            name=abbrev.capitalize(),
            description=f"TERENO field station {abbrev.capitalize()}",
            properties=ThingProperty(
                id=abbrev,
                project="Tereno",
                station_id=abbrev.upper()[:3],
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
        station = server.post(station)

        location = Location(
            name=abbrev.capitalize(),
            description=f"TERENO site {abbrev.capitalize()}",
            properties=LocationProperty(),
            location=LocationGeo(coordinates=site.coords),
            Things=[Link(id=station.id)],
        )
        location = server.post(location)

        del station

    setup_datastreams(server)


def load_data(site: str) -> pd.DataFrame:

    data_src = Path("tests/integrations/data")

    # read header info
    colnames = open(data_src / f"{site}_M_header.csv", "r").readline()[:-1].split(",")
    colnames = [
        cname.lower().replace(" ", "_").replace("(", "").replace(")", "")
        for cname in colnames
    ]

    var_subset = ["airtemp_avg", "ramount"]

    files = data_src.glob(f"{site}_*.dat.gz")
    files = sorted(files)

    dfs = []
    for file in files:
        df = pd.read_csv(
            file,
            names=colnames,
            header=None,
            na_values="NAN",
            parse_dates=["timestamp"],
        )
        df = df.set_index("timestamp")
        dfs.append(df)
    df = pd.concat(dfs)
    return df.loc[:, var_subset]


@pytest.fixture(scope="session")
def server():
    frost = DockerCompose(
        COMPOSE_PATH, compose_file_name="docker-compose.yaml", pull=True
    )
    frost.start()

    service_host = frost.get_service_host("web", 8080)
    service_port = frost.get_service_port("web", 8080)

    url = f"http://{service_host}:{service_port}/FROST-Server/v1.1"

    # hack
    retries = 10
    for i in range(retries):
        print(f"Waiting for system to come alive. Try: {i+1}")
        time.sleep(3)
        stdout, _ = frost.get_logs()
        if "database system is ready to accept connections" in stdout.decode(
            "unicode_escape"
        ):
            break

    server = Server(url)

    assert server.is_alive is True
    # store state
    print("We have a frost server")
    yield server

    # restore state
    frost.stop()


@pytest.fixture(scope="session")
def server_with_schema(server: Server):

    setup_system(server)

    yield server
    print("\nRemoving schema? Maybe not even desirable...?")


@pytest.fixture(scope="session")
def server_with_data(server_with_schema: Server):
    fendt_data = load_data("Fen")
    graswang_data = load_data("Gra")

    # dump = server_with_schema.dump(Datastream)
    # print(f"DUMP: {dump}")
    fendt_ds = server_with_schema.get(Datastream, search="FEN")
    graswang_ds = server_with_schema.get(Datastream, search="GRA")

    for site_data, site_ds in [(fendt_data, fendt_ds), (graswang_data, graswang_ds)]:
        var_query = "Temperature"
        var = "airtemp_avg"
        data = site_data[[var]]
        server_with_schema.push_data(
            [ds for ds in site_ds if var_query in ds.name][0],
            data.rename(columns={var: "result"}),
            batch_mode=True,
        )

        var_query = "PrecipitationIntensity"
        var = "ramount"
        data = site_data[[var]].rename(columns={var: "result"})
        server_with_schema.push_data(
            [ds for ds in site_ds if var_query in ds.name][0],
            data.rename(columns={var: "result"}),
            batch_mode=True,
        )

    yield server_with_schema
    # reset to initial state
    print("\nRemoving (observation) data again ...?")
