import datetime
import json
import re
from typing import Any, Dict, Iterable, List, Optional, Tuple, Type, Union

import pandas as pd
import requests

from .models import Datastream, Entity, Location, ObservedProperty, Sensor, Thing

__all__ = ["Server"]

# type definitions
Url = str

DT_FORMAT = "%Y-%m-%dT%H:%M:%S%Z"


def expected_cols_and_dtypes(df: pd.DataFrame) -> bool:
    """check that the dataframe has an datetime index and excatly one col named result"""
    i_type = df.index.inferred_type
    c_names = df.columns.values.tolist()
    return True if ((i_type == "datetime64") and (c_names == ["result"])) else False


def convert_df_to_jsonable_array(df: pd.DataFrame) -> List[Tuple[str, float]]:
    """convert df two a list of lists (that contain datetime and result"""
    # TODO: check if format string is valid
    dates = [datetime.datetime.strftime(d.to_pydatetime(), DT_FORMAT) for d in df.index]
    values = df.result
    return list(zip(dates, values))


def check_if_observations_exist(url: str, dt: datetime.datetime) -> Iterable[int]:
    """check if observation(s) already exist for given datastream and time"""
    offset = datetime.timedelta(seconds=0.1)
    t1 = datetime.datetime.strftime(dt - offset, DT_FORMAT)
    t2 = datetime.datetime.strftime(dt + offset, DT_FORMAT)

    r = requests.get(
        f"{url}?$filter=phenomenonTime ge {t1}Z and phenomenonTime le {t2}Z &$select=@iot.id"
    )

    result = r.json()
    return [int(x["@iot.id"]) for x in result["value"]] if "value" in result else []


def get_endpoint_from_entity(entity: Union[Entity, Type[Entity]]) -> str:
    """get endpoint from entity type"""

    if isinstance(entity, Entity):
        e_type = entity.__class__.__name__  # instance passed
    else:
        e_type = entity.__name__  # Class/ Type passed

    if e_type not in [
        "Thing",
        "Sensor",
        "ObservedProperty",
        "Unit",
        "Datastream",
        "Location",
        "Observation",
    ]:
        # only entities tested for now
        raise NotImplementedError

    return f"{e_type[:-1]}ies" if e_type[-1] == "y" else f"{e_type}s"


class Server:
    """Server model of a FROST server instance"""

    _tag = "cw_"  # filter all server entities by this name tag for now

    def __init__(self, url: Url):
        self._url = url

    @property
    def url(self) -> Url:
        return self._url

    def _get_endpoint_url(
        self, entity: Union[Entity, Type[Entity]], id: Optional[int] = None
    ) -> str:

        e_id = entity.id if hasattr(entity, "id") else None
        if not id:
            id = e_id

        endpoint = get_endpoint_from_entity(entity)
        return f"{self.url}/{endpoint}({id})" if id else f"{self.url}/{endpoint}"

    def _get_id_from_url(self, url: str) -> int:
        s = re.findall(
            r"\((.*?)\)", url
        )  # Search for all brackets (and store their content)
        if len(s) > 0:
            id = int(s[-1])
        else:
            raise ValueError("Could not extract thing_id from header")
        return id

    def _extract_ids_from_navlinks(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """messy!"""

        if "value" in data:
            for d in data["value"]:
                links = [x for x in d if "@iot.navigationLink" in x]
                for link in links:
                    for e in [ObservedProperty, Sensor, Thing]:
                        if e.__name__ in link:
                            url = self.url + d[link].split("FROST-Server/v1.1")[-1]

                            _j = requests.get(url).json()
                            if "value" in _j:
                                d[e.__name__] = [
                                    {"@iot.id": x["@iot.id"]}
                                    for x in requests.get(url).json()["value"]
                                ]
                            else:
                                d[e.__name__] = {"@iot.id": _j["@iot.id"]}
                            continue
        else:
            links = [x for x in data if "@iot.navigationLink" in x]
            for link in links:
                for e in [ObservedProperty, Sensor, Thing]:
                    if e.__name__ in link:
                        url = self.url + data[link].split("FROST-Server/v1.1")[-1]

                        _j = requests.get(url).json()
                        if "value" in _j:
                            data[e.__name__] = [
                                {"@iot.id": x["@iot.id"]}
                                for x in requests.get(url).json()["value"]
                            ]
                        else:
                            data[e.__name__] = {"@iot.id": _j["@iot.id"]}
                        continue

        return data

    # TODO: make 'id' and 'search' mutually exclusive !!!
    def get(
        self,
        E: Type[Entity],
        id: Optional[int] = None,
        search: Optional[str] = None,
        tag_off: Optional[bool] = False,
    ) -> Optional[Union[Entity, Iterable[Entity]]]:
        """Get all or specified entity from server

        Args:
            E: entity type to get
            id: entity id
            search: filter entitites by this search string
            tag_off: (will be removed later)

        Returns:
            Entity or list of requested entities
        """

        url = self._get_endpoint_url(E, id=id)

        # filter response for "cw_" entities, turn-off tag limit with tag_off=True
        if hasattr(Server, "_tag") and not tag_off:
            url += f"?$filter=startswith(name, '{self._tag}')"

        if search:
            if "$filter" in url:
                url += f" and substringof(name, '{search}')"
            else:
                url += f"$filter=substringof('{search}', name)"

        r = requests.get(url)

        if r.status_code == 200:
            data = self._extract_ids_from_navlinks(r.json())

            if id:
                obj = E.parse_obj(data)
                if "@iot.id" in data:
                    obj.id = int(data["@iot.id"])
                return obj
            else:
                objs = []
                for d in data["value"]:
                    obj = E.parse_obj(d)
                    if "@iot.id" in d:
                        obj.id = int(d["@iot.id"])
                    objs.append(obj)
                return objs
        elif r.status_code == 404:
            print(f"No entities found. <{r.status_code}>")
            return None
        else:
            raise NotImplementedError(f"Raised status code {r.status_code}")

    def update(self, entity: Entity) -> None:
        """Update/ patch an entity on the server

        Args:
            entity: the updated entity to be pushed to the server

        Returns:
            Nothing

        """

        id = entity.id
        if not id:
            raise ValueError(f"Entity {entity} has no id and cannot not be updated")

        url = self._get_endpoint_url(entity, id=id)

        headers = {"content-type": "application/json"}
        res = requests.patch(url, data=json.dumps(entity.dict()), headers=headers)
        if res.status_code != 200:
            print(f"Update not successful. Return status {res.status_code}")

    def update_field(
        self, E: Type[Entity], id: int, payload: Dict[str, Any]
    ) -> Optional[Iterable[Entity]]:
        """Update specific fields of an entity on the server

        Args:
            E: entity type to update
            id: entity id
            payload: entity fields and values to be used in update

        Returns:
            Entity or list of requested entities
        """
        payload_fields = set(payload.keys())

        if not payload_fields.issubset(E.__fields__):
            print(f"Provided payload does not match entity {E}")
            raise ValueError

        url = self._get_endpoint_url(E, id=id)

        headers = {"content-type": "application/json"}
        res = requests.patch(url, data=json.dumps(payload), headers=headers)
        if res.status_code != 200:
            print(f"Update not successful. Return status {res.status_code}")

        # get patched entity and return it
        return self.get(E, id=id)

    def post(self, entity: Entity) -> Entity:
        """Post an entity to the server

        Args:
            entity: entity instance

        Returns:
            Posted entity including assigned id
        """

        url = self._get_endpoint_url(entity)
        res = requests.post(url, json=entity.dict(by_alias=True))

        if res.status_code != 201:
            print(f"Something went wrong in POST: {res.status_code}")
            raise ValueError
        else:
            # retrieve id from frost server and update entity
            entity.id = self._get_id_from_url(res.headers["location"])
            print(f"Success: POST {entity}")

        return entity

    def delete(
        self,
        E: Type[Entity],
        id: Optional[int] = None,
        search: Optional[str] = None,
        tag_off: Optional[bool] = False,
    ) -> None:
        """Delete all or specified entity from server

        Args:
            E: entity type to delete
            id: entity id
            search: filter entitites by this search string
            tag_off: (will be removed later)

        Returns:
            Nothing
        """

        url = self._get_endpoint_url(E, id=id)

        # filter response for "cw_" entities, turn-off tag limit with tag_off=True
        if hasattr(Server, "_tag") and not tag_off:
            url += f"?$filter=startswith(name, '{self._tag}')"

        if search:
            if "$filter" in url:
                url += f" and substringof(name, '{search}')"
            else:
                url += f"$filter=substringof('{search}', name)"

        res = requests.delete(url)

        if res.status_code != 200:
            print(f"Delete not successful. Return status {res.status_code}")
        else:
            # check that all entities are gone
            result = self.get(E, id=id)
            if result is not None:
                raise ValueError(
                    "Something went wrong. There are still {E} left after delete!"
                )

    def push_data(
        self,
        datastream: Datastream,
        df: pd.DataFrame,
        batch_mode: Optional[bool] = True,
    ) -> None:
        """Push data to datastream

        Args:
            datastream: target datastream
            df: dataframe with observation values
            batch_mode: push data as one array instead of iterative pushes (much more efficient)

        Returns:
            Nothing
        """

        # validate data dataframe
        if not expected_cols_and_dtypes(df):
            raise ValueError("Dataframe is not compatible")

        if batch_mode:
            url = self.url + "/CreateObservations"

            # push data at once with one big array
            array = convert_df_to_jsonable_array(df)

            payload = {
                "components": ["phenomenonTime", "result"],
                "dataArray@iot.count": len(array),
                "dataArray": array,
                "Datastream": {"@iot.id": datastream.id},
            }

            r = requests.post(url, json=payload)

            if r.status_code != 201:
                raise requests.RequestException(
                    f"DATA PUSH (batched) Observation request error {r.status_code}"
                )
        else:
            url = self._get_endpoint_url(datastream) + "/Observations"

            for timestamp, row in df.iterrows():
                # check if data already exist and delete the entry/ entries
                existing_ids = check_if_observations_exist(
                    url, dt=timestamp.to_pydatetime()
                )
                for id in existing_ids:
                    r = requests.delete(f"{url}({id})")
                    if r.status_code != 200:
                        raise requests.exceptions.RequestException(
                            "DELETE request not successful"
                        )

                dt_str = datetime.datetime.strftime(
                    timestamp.to_pydatetime(), DT_FORMAT
                )
                payload = {"phenomenonTime": dt_str, "result": str(row["result"])}
                r = requests.post(url, json=payload)

                if r.status_code != 201:
                    raise requests.RequestException(
                        f"DATA PUSH Observation request error {r.status_code}"
                    )

    def pull_data(
        self,
        datastream: Datastream,
        *,
        dt_min: Optional[datetime.datetime] = None,
        dt_max: Optional[datetime.datetime] = None,
    ) -> pd.DataFrame:
        """Pull data from datastream

        Args:
            datastream: source datastream
            dt_min: start datetime
            dt_max: end datetime

        Returns:
            Dataframe with observation values
        """

        if dt_min and dt_max:
            if dt_min > dt_max:
                raise ValueError("datetime dt_min must be prior to dt_max")

        dt_min_str, dt_max_str = "", ""
        if dt_min:
            dt_min_str = (
                f"phenomenonTime ge {datetime.datetime.strftime(dt_min, DT_FORMAT)}Z"
            )
        if dt_max:
            dt_max_str = (
                f"phenomenonTime le {datetime.datetime.strftime(dt_max, DT_FORMAT)}Z"
            )

        url = self._get_endpoint_url(datastream) + "/Observations"

        if dt_min_str and dt_max_str:
            url += f"?$filter={dt_min_str} and {dt_max_str}"
        elif dt_min_str:
            url += f"?$filter={dt_min_str}"
        elif dt_max_str:
            url += f"?$filter={dt_max_str}"
        else:
            pass

        r = requests.get(url)
        print(url)
        if r.status_code != 200:
            raise requests.RequestException(
                f"DATA PULL Observation request error {r.status_code}"
            )

        data = r.json()["value"]
        dts = [
            datetime.datetime.fromisoformat(x["phenomenonTime"].replace("Z", "+00:00"))
            for x in data
        ]
        results = [float(x["result"]) for x in data]

        return pd.DataFrame({"result": results}, index=dts).sort_index()

    def dump(
        self, entity: Optional[Union[Type[Entity], Iterable[Type[Entity]]]] = None
    ) -> Dict[Type[Entity], List[Entity]]:
        """Get all instances of requested entity type

        Args:
            entity: entity type or list of entity types

        Returns:
            Dict with entities
        """
        """return all data for given entity types"""
        entities = entity or [Sensor, Thing, Location, Datastream]
        if not isinstance(entities, list):
            entities = [entities]

        data = {}
        for e in entities:
            result = self.get(e)

            if result is not None:
                if not isinstance(result, list):
                    result = [result]
                data[e] = result

        return data

    @property
    def is_alive(self):
        """Check if the server can be reached"""

        try:
            r = requests.get(self.url)
            r.raise_for_status()  # Raises a HTTPError if the status is 4xx, 5xxx
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
            print("FROST server down?")
        except requests.exceptions.HTTPError:
            print(f"FROST server repsonds with HTTPError {r.status_code}")
        else:
            return True
        return False
