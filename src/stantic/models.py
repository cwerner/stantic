from datetime import datetime
from typing import Any, List, Literal, Optional, Tuple

from pydantic import BaseModel, EmailStr, Field

__all__ = [
    "Property",
    "Person",
    "Entity",
    "ThingProperty",
    "LocationProperty",
    "LocationGeo",
    "Location",
    "ObservedProperty",
    "SensorProperty",
    "Sensor",
    "Thing",
    "Unit",
    "Link",
    "Datastream",
    "Observation",
]


class Property(BaseModel):
    pass


# ----------
# model that's not included


class Person(BaseModel):
    name: str
    email: EmailStr
    affiliation: str


class Link(BaseModel):
    id: int = Field(int, alias="@iot.id")

    class Config:
        allow_population_by_field_name = True


# Thing --
class ThingProperty(Property):
    id: str
    project: str
    station_id: str
    tech_person: Person
    science_person: Person
    data_person: Person
    qaqc_person: Person


# base model
class Entity(BaseModel):
    id: Optional[int] = None  # Field(None, alias="@iot.id")
    name: str
    description: str
    properties: Any

    class Config:
        fields = {"id": {"exclude": True}}
        allow_population_by_alias = True
        allow_population_by_field_name = True


class Thing(Entity):
    _tag: Literal["Thing"] = "Thing"
    properties: ThingProperty


# Location --
class LocationProperty(BaseModel):
    pass


class LocationGeo(BaseModel):
    type: Literal["Point"] = "Point"
    coordinates: Tuple[float, float]  # (lon. lat)


class Location(Entity):
    _tag: Literal["Location"] = "Location"
    properties: LocationProperty
    encodingType: Literal["application/geo+json"] = "application/geo+json"
    location: LocationGeo
    Things: Optional[List[Link]] = None


# ObservedProperty --
class ObservedProperty(Entity):
    _tag: Literal["ObservedProperty"] = "ObservedProperty"
    definition: str
    properties: Property


# Sensor --
class SensorProperty(Property):
    pid: str
    sn: str
    offset: float


class Sensor(Entity):
    _tag: Literal["Sensor"] = "Sensor"
    encodingType: str
    metadata: str
    properties: SensorProperty


# Datastream --
class Unit(BaseModel):
    name: str
    symbol: str
    definition: str


class Datastream(Entity):
    _tag: Literal["Datastream"] = "Datastream"
    observationType: str
    unitOfMeasurement: Unit
    Thing: Link
    Sensor: Link
    ObservedProperty: Link


# Observation --
class Observation(BaseModel):
    phenomenonTime: datetime
    result: float
