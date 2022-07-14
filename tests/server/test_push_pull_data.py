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


@pytest.fixture
def future_temp_data(data: pd.DataFrame):
    future_temp_data = (
        data[["airtemp_avg"]].rename(columns={"airtemp_avg": "result"}).head(100)
    )
    return future_temp_data.set_index(
        future_temp_data.index + datetime.timedelta(days=10)
    )


@pytest.fixture
def fendt_temp_datastream(server_with_cleandata: Server):
    datastreams = server_with_cleandata.get(Datastream, search="FEN")
    datastream = [d for d in datastreams if "Temperature" in d.name][0]
    return datastream


def test_server_push_data_not_batched(
    server_with_cleandata: Server,
    future_temp_data: pd.DataFrame,
    fendt_temp_datastream: Datastream,
):
    server_with_cleandata.push_data(
        fendt_temp_datastream, future_temp_data, batch_mode=False
    )


def test_server_push_data_batched(
    server_with_cleandata: Server,
    future_temp_data: pd.DataFrame,
    fendt_temp_datastream: Datastream,
):
    server_with_cleandata.push_data(
        fendt_temp_datastream, future_temp_data, batch_mode=True
    )


def test_server_push_data_not_batched_overwrite(
    server_with_cleandata: Server,
    future_temp_data: pd.DataFrame,
    fendt_temp_datastream: Datastream,
):
    server_with_cleandata.push_data(
        fendt_temp_datastream, future_temp_data, batch_mode=False
    )
    server_with_cleandata.push_data(
        fendt_temp_datastream, future_temp_data, batch_mode=False
    )


def test_server_push_data_invalid_dataframe(
    server_with_cleandata: Server, fendt_temp_datastream: Datastream
):
    bad_df = pd.DataFrame(columns={"var": [1, 2]})
    with pytest.raises(ValueError):
        server_with_cleandata.push_data(fendt_temp_datastream, bad_df)


# def test_server_push_data_batched_overwrite(server_with_cleandata: Server, future_temp_data: pd.DataFrame, fendt_temp_datastream: Datastream):
#     server_with_cleandata.push_data(fendt_temp_datastream, future_temp_data, batch_mode=True)
#     with pytest.raises(ValueError):
#         server_with_cleandata.push_data(fendt_temp_datastream, future_temp_data, batch_mode=True)


def test_server_pull_data(
    server_with_cleandata: Server, fendt_temp_datastream: Datastream
):
    data = server_with_cleandata.pull_data(fendt_temp_datastream)
    assert len(data) > 0


def test_server_pull_data_with_dtmin(
    server_with_cleandata: Server, fendt_temp_datastream: Datastream
):
    min_date = datetime.datetime(2022, 1, 1, tzinfo=datetime.timezone.utc)
    data = server_with_cleandata.pull_data(fendt_temp_datastream, dt_min=min_date)
    assert data.index.min() >= min_date


def test_server_pull_data_with_dtmax(
    server_with_cleandata: Server, fendt_temp_datastream: Datastream
):
    max_date = datetime.datetime(2022, 12, 31, tzinfo=datetime.timezone.utc)
    data = server_with_cleandata.pull_data(fendt_temp_datastream, dt_max=max_date)
    assert data.index.max() <= max_date


def test_server_pull_data_trigger_data_truncate(
    server_with_cleandata: Server, fendt_temp_datastream: Datastream
):
    data_complete = server_with_cleandata.pull_data(fendt_temp_datastream)
    data_truncated = server_with_cleandata.pull_data(
        fendt_temp_datastream, max_requests=2
    )
    assert len(data_complete) > len(data_truncated)


def test_server_pull_data_with_dtmin_and_dtmax(
    server_with_cleandata: Server, fendt_temp_datastream: Datastream
):
    min_date = datetime.datetime(2022, 1, 1, tzinfo=datetime.timezone.utc)
    max_date = datetime.datetime(2022, 12, 31, tzinfo=datetime.timezone.utc)
    data = server_with_cleandata.pull_data(
        fendt_temp_datastream, dt_min=min_date, dt_max=max_date
    )
    assert data.index.max() <= max_date and data.index.min() >= min_date


def test_server_pull_data_with_invalid_dtmin_dtmax(
    server_with_cleandata: Server, fendt_temp_datastream: Datastream
):
    min_date = datetime.datetime(2022, 12, 31, tzinfo=datetime.timezone.utc)
    max_date = datetime.datetime(2022, 1, 31, tzinfo=datetime.timezone.utc)

    with pytest.raises(ValueError):
        server_with_cleandata.pull_data(
            fendt_temp_datastream, dt_min=min_date, dt_max=max_date
        )
