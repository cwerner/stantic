from pathlib import Path
from typing import Any, List, Union

import pandas as pd


def pretty_print(x: Union[List[Any], Any]):
    """do some pretty indentation for data models"""
    if not isinstance(x, dict):
        x = {"Content": x}

    for k, v in x.items():
        print(f"{k}:")
        if not isinstance(v, list):
            v = [v]

        out = ", \n".join([e.json(indent=4, by_alias=True) for e in v if e])
        print(out)


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
