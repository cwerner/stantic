import sys
from pathlib import Path
from typing import Any, List, Optional, TextIO, Union

import pandas as pd


def pretty_print(
    x: Union[List[Any], Any],
    file_object: Optional[TextIO] = None,
    # clean:bool=False,
) -> None:
    """do some pretty indentation for data models"""

    def format_item(e: Any) -> str:
        return e.json(indent=4, by_alias=True)

    out = ""

    # TODO: refactor. make this recursive?
    if isinstance(x, list):
        if file_object:
            raise NotImplementedError(
                "Currently containers are not supported for file out"
            )
        out += ", \n".join([format_item(e) for e in x if e])
    elif isinstance(x, dict):
        if file_object:
            raise NotImplementedError(
                "Currently containers are not supported for file out"
            )
        for k, v in x.items():
            if isinstance(x, list):
                out += ", \n".join([format_item(e) for e in v if e])
            else:
                out += format_item(v)
    else:
        out += format_item(x)

    file_object = file_object or sys.stdout
    file_object.write(out + "\n")


def load_data(site: str, *, data_src: Path) -> pd.DataFrame:

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
