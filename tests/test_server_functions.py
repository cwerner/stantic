import pandas as pd
import pytest

from stantic.models import ObservedProperty, Person, Thing, ThingProperty
from stantic.server import (
    convert_df_to_jsonable_array,
    expected_cols_and_dtypes,
    get_endpoint_from_entity,
)


def test_expected_cols_and_dtypes():
    dt_index = pd.date_range(start="2010-01-01", end="2010-01-05")
    df_good = pd.DataFrame(
        data={"result": [1, 2, 3, 4, 5]},
        index=dt_index,
    )
    df_bad_index = pd.DataFrame(data={"result": [1, 2, 3, 4, 5]})
    df_bad_multiple_cols = pd.DataFrame(
        data={"result": [1, 2, 3, 4, 5], "result2": [6, 7, 8, 9, 10]},
        index=dt_index,
    )
    df_bad_colname = pd.DataFrame(data={"var1": [1, 2, 3, 4, 5]}, index=dt_index)
    assert expected_cols_and_dtypes(df_good) is True
    assert expected_cols_and_dtypes(df_bad_index) is False
    assert expected_cols_and_dtypes(df_bad_multiple_cols) is False
    assert expected_cols_and_dtypes(df_bad_colname) is False


def test_convert_df_to_jsonable_array():
    df = pd.DataFrame(
        data={"result": [1, 2, 3]}, index=pd.date_range("2010-01-01", "2010-01-03")
    )
    target = [
        ("2010-01-01T00:00:00", 1),
        ("2010-01-02T00:00:00", 2),
        ("2010-01-03T00:00:00", 3),
    ]
    assert convert_df_to_jsonable_array(df) == target


def test_get_endpoint_from_entity_invalid_entity():
    with pytest.raises(NotImplementedError):
        get_endpoint_from_entity("ThisShouldNotWork")


def test_get_endpoint_from_entity_invalid_entity_type():
    with pytest.raises(NotImplementedError):
        get_endpoint_from_entity(12)


def test_get_endpoint_from_entity_using_str():
    assert get_endpoint_from_entity("Thing") == "Things"
    assert get_endpoint_from_entity("ObservedProperty") == "ObservedProperties"


def test_get_endpoint_from_entity_using_classes():
    assert get_endpoint_from_entity(Thing) == "Things"
    assert get_endpoint_from_entity(ObservedProperty) == "ObservedProperties"


def test_get_endpoint_from_entity_using_instances():
    # just some dummy definitions so we get valid pydantic models
    a_person = Person(
        name="Hans Dampf", email="hans.dampf@hd.com", affiliation="HD Inc"
    )
    a_thing_property = ThingProperty(
        id=1,  # this is normally not specified but rather assigned by the frost server
        project="a project",
        station_id="a station id",
        tech_person=a_person,
        science_person=a_person,
        data_person=a_person,
        qaqc_person=a_person,
    )
    a_thing = Thing(
        name="test", description="describe this test", properties=a_thing_property
    )

    an_observed_property = ObservedProperty(
        id=1,  # this is normally not specified but rather assigned by the frost server
        name="an onberved property",
        description="description of an observed property",
        properties={},
        definition="some definition",
    )

    assert get_endpoint_from_entity(a_thing) == "Things"
    assert get_endpoint_from_entity(an_observed_property) == "ObservedProperties"
