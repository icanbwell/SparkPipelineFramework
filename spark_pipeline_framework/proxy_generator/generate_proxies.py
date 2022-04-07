from os import getcwd, path
from sys import exit

from spark_pipeline_framework.proxy_generator.proxy_generator import ProxyGenerator


def main() -> int:
    library_folder: str = path.join(getcwd(), "library")
    ProxyGenerator.remove_empty_dirs(library_folder)
    ProxyGenerator.generate_proxies(library_folder)
    return 0


if __name__ == "__main__":
    exit(main())
