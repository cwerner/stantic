import datetime
from pathlib import Path

import pandas as pd
import pytest

from stantic.models import Datastream
from stantic.server import Server
from stantic.tools import load_data

# ============================================================================
# test data push/ pull functionality


@pytest.fixture
def data(sample_data: Path):
    return load_data("Fen", data_src=sample_data)


def test_server_push_data(server_with_data: Server, data: pd.DataFrame):
    future_temp_data = (
        data[["airtemp_avg"]].rename(columns={"airtemp_avg": "result"}).head(100)
    )
    future_temp_data1 = future_temp_data.set_index(
        future_temp_data.index + datetime.timedelta(days=10)
    )
    future_temp_data2 = future_temp_data.set_index(
        future_temp_data.index + datetime.timedelta(days=20)
    )

    datastreams = server_with_data.get(Datastream, search="FEN")
    datastream = [d for d in datastreams if "Temperature" in d.name][0]

    server_with_data.push_data(datastream, future_temp_data1, batch_mode=False)

    server_with_data.push_data(datastream, future_temp_data2, batch_mode=True)

    # push data again (old data should be deleted)
    server_with_data.push_data(datastream, future_temp_data2, batch_mode=False)


@pytest.fixture
def temp_datastream(server_with_data: Server):
    datastreams = server_with_data.get(Datastream, search="FEN")
    return [d for d in datastreams if "Temperature" in d.name][0]


def test_server_pull_data(server_with_data: Server, temp_datastream: Datastream):
    data = server_with_data.pull_data(temp_datastream)
    print(data.head())
    assert len(data) > 0


def test_server_pull_data_with_dtmin(
    server_with_data: Server, temp_datastream: Datastream
):
    min_date = datetime.datetime(2022, 1, 1, tzinfo=datetime.timezone.utc)
    data = server_with_data.pull_data(temp_datastream, dt_min=min_date)
    assert data.index.min() >= min_date


def test_server_pull_data_with_dtmax(
    server_with_data: Server, temp_datastream: Datastream
):
    max_date = datetime.datetime(2022, 12, 31, tzinfo=datetime.timezone.utc)
    data = server_with_data.pull_data(temp_datastream, dt_max=max_date)
    print(data)
    assert data.index.max() <= max_date


def test_server_pull_data_with_invalid_dtmin_dtmax(
    server_with_data: Server, temp_datastream: Datastream
):
    min_date = datetime.datetime(2022, 12, 31, tzinfo=datetime.timezone.utc)
    max_date = datetime.datetime(2022, 1, 31, tzinfo=datetime.timezone.utc)

    with pytest.raises(ValueError):
        server_with_data.pull_data(temp_datastream, dt_min=min_date, dt_max=max_date)
