import csv
from pathlib import Path
from typing import Union, Optional, Dict


class TranslatorProxyBase:
    def __init__(
        self, location: Union[str, Path], csv_file: Union[str, Path]
    ) -> None:
        self.location: Union[str, Path] = location
        self.csv_file: Union[str, Path] = csv_file
        self.mapping_dict: Optional[Dict[str, str]] = None

    def get_mapping(self) -> Dict[str, str]:
        # read csv
        # convert first two columns to dictionary
        if not self.mapping_dict:
            self.mapping_dict = {}
            with open(
                Path(self.location).joinpath(self.csv_file), newline=''
            ) as csvfile:
                csv_reader = csv.reader(csvfile, delimiter=',', quotechar='"')
                # next(csv_reader)  # skip header
                for row in csv_reader:
                    self.mapping_dict[row[0]] = row[1]
        return self.mapping_dict
