from os import path, makedirs, listdir
from pathlib import Path
from shutil import rmtree, copyfile
from typing import Callable, List, Optional, Set

from spark_pipeline_framework.proxy_generator.proxy_generator import ProxyGenerator


def test_can_generate_proxies() -> None:
    data_dir: Path = Path(__file__).parent.joinpath("./")

    temp_folder = data_dir.joinpath("temp")
    if path.isdir(temp_folder):
        rmtree(temp_folder)

    makedirs(temp_folder)

    src_library_folder: str = path.join(data_dir, "library")
    temp_library_folder: str = path.join(data_dir, "temp", "library")

    if not path.exists(temp_library_folder):
        makedirs(temp_library_folder)

    # copytree(src=src_library_folder, dst=temp_library_folder)
    recursive_overwrite(src=src_library_folder, dst=temp_library_folder)

    # Act
    ProxyGenerator.generate_proxies(folder=temp_library_folder)

    # Assert
    data_source_path = path.join(
        temp_library_folder,
        "data_sources",
        "my_data_source",
        "v1",
        "data_sources_my_data_source_v1.py",
    )
    assert path.exists(data_source_path)

    assert path.exists
    (
        path.join(
            temp_library_folder,
            "features",
            "my_sql_feature",
            "v1",
            "features_my_sql_feature_v1.py",
        )
    )
    assert path.exists(
        path.join(
            temp_library_folder,
            "features",
            "my_python_feature",
            "v1",
            "features_my_python_feature_v1.py",
        )
    )

    assert path.exists(
        path.join(
            temp_library_folder,
            "features",
            "member_claims",
            "v1",
            "features_member_claims_v1.py",
        )
    )

    assert not path.exists(
        path.join(
            temp_library_folder,
            "features",
            "member_claims",
            "v1",
            "test",
            "features_member_claims_v1_test.py",
        )
    )


def recursive_overwrite(
    src: str, dst: str, ignore: Optional[Callable[[str, List[str]], Set[str]]] = None
) -> None:
    if path.isdir(src):
        print(f"dir={src}")
        if not path.isdir(dst):
            makedirs(dst)
        files = listdir(src)
        if ignore is not None:
            ignored = ignore(src, files)
        else:
            ignored = set()
        for f in files:
            print(f"file={f}")
            if f not in ignored:
                recursive_overwrite(path.join(src, f), path.join(dst, f), ignore)
    else:
        print(f"src={src}, dst={dst}")
        copyfile(src, dst)
