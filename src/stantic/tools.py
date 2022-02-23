from typing import Any, List, Union


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
