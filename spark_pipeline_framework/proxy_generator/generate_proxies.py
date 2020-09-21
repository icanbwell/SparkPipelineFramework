from os import walk, path, listdir, getcwd
from shutil import rmtree
from re import search
from sys import exit
from typing import Match, Optional


def remove_empty_dirs(folder: str) -> None:
    def is_path_empty(directory: str):
        for dir_tuple in walk(directory):
            if [
                file for file in dir_tuple[2] if not (file.startswith(('__', '.')) or file.endswith('.pyc'))
            ]:
                return False
        return True

    for path_tuple in walk(folder):
        # noinspection SpellCheckingInspection
        if path_tuple[0].endswith('__pycache__'):
            pass
        else:
            current_path: str = path.join(folder, path_tuple[0])
            if is_path_empty(current_path):
                try:
                    rmtree(current_path)
                except FileNotFoundError:
                    pass
                else:
                    print(f'Removed {current_path}')


def generate_proxies(folder: str):
    all_objects_in_path = listdir(folder)
    non_special_objects = [
        file for file in all_objects_in_path if not file.startswith(('_', '.'))
    ]
    # noinspection SpellCheckingInspection
    folders = [
        subfolder for subfolder in non_special_objects if
        '.' not in subfolder and len(listdir(path.join(folder, subfolder))) != 0
    ]
    files = [
        file for file in all_objects_in_path if '.' in file and not file.startswith('_')
    ]
    transformer_file_indicators = ('.sql', '.csv')
    path_contains_transformer = len([file for file in files if file.endswith(transformer_file_indicators)]) > 0

    if path_contains_transformer:
        search_result: Optional[Match[str]] = search(r'/library/', folder)
        if search_result:
            transformer_reader_file_name = folder[search_result.end():].replace('/', '_')
            write_transformer(
                file_name=transformer_reader_file_name,
                folder=folder
            )

    # now recursively generate proxies
    for folder in folders:
        generate_proxies(path.join(folder, folder))


def write_transformer(file_name: str, folder: str):
    transformer_reader_class_name = ''.join([s.title() for s in file_name.split('_')])
    transformer_reader_string = f"""
from proxy_base import ProxyBase
from progress_logger.progress_logger import ProgressLogger
from os import path


class {transformer_reader_class_name}(ProxyBase):
def __init__(self, parameters, progress_logger: ProgressLogger = None, verify_count_remains_same=False):
    self.location = path.dirname(os.path.abspath(__file__))
    super().__init__(parameters, progress_logger, verify_count_remains_same)
"""
    with open(path.join(folder, file_name + '.py'), 'w+') as file:
        file.write(transformer_reader_string)


def main():
    library_folder: str = path.join(getcwd(), 'library')
    remove_empty_dirs(library_folder)
    generate_proxies(library_folder)


if __name__ == "__main__":
    exit(main())
