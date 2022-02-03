from pathlib import Path
from typing import Union, List


def get_absolute_paths(filepath: Union[str, List[str], Path]) -> List[str]:
    """
    Abstracts handling of paths so we can use paths on both k8s in AWS as well as local

    :param filepath: the path or paths to format appropriately
    :return: a list of paths optimized for local or k8s usages
    """
    if not filepath:
        raise ValueError(f"filepath is empty: {filepath}")

    if isinstance(filepath, str):
        if filepath.__contains__(":"):
            return [filepath]
        else:
            data_dir = Path(__file__).parent.parent.joinpath("./")
            return [f"file://{data_dir.joinpath(filepath)}"]
    elif isinstance(filepath, Path):
        data_dir = Path(__file__).parent.parent.joinpath("./")
        return [f"file://{data_dir.joinpath(filepath)}"]
    elif isinstance(filepath, (list, tuple)):
        data_dir = Path(__file__).parent.parent.joinpath("./")
        absolute_paths = []
        for path in filepath:
            if ":" in path:
                absolute_paths.append(path)
            else:
                absolute_paths.append(f"file://{data_dir.joinpath(path)}")
        return absolute_paths
    else:
        raise TypeError(f"Unknown type '{type(filepath)}' for filepath {filepath}")
