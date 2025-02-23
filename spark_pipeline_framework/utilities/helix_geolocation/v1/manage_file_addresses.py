import pymongo
from pymongo import MongoClient, UpdateOne
from pymongo.collection import Collection
from typing import List, Dict, Any, Set, Tuple, Generator, Sequence, Iterable
import pathlib
import glob
import json
import csv

from spark_pipeline_framework.utilities.helix_geolocation.v1.raw_address import (
    RawAddress,
)
from spark_pipeline_framework.utilities.helix_geolocation.v1.standardizing_vendor import (
    StandardizingVendor,
)
from spark_pipeline_framework.utilities.helix_geolocation.v1.vendor_response import (
    VendorResponse,
)

# define column mappings
column_mappings = {
    "address_catalog": {
        "line1": "line1",
        "line2": "line2",
        "city": "city",
        "state": "state",
        "zipcode": "zipcode",
    },
    "nppes_npi_pfile_mailing": {
        "line1": "Provider First Line Business Mailing Address",
        "line2": "Provider Second Line Business Mailing Address",
        "city": "Provider Business Mailing Address City Name",
        "state": "Provider Business Mailing Address State Name",
        "zipcode": "Provider Business Mailing Address Postal Code",
    },
    "nppes_npi_pfile_practice": {
        "line1": "Provider First Line Business Practice Location Address",
        "line2": "Provider Second Line Business Practice Location Address",
        "city": "Provider Business Practice Location Address City Name",
        "state": "Provider Business Practice Location Address State Name",
        "zipcode": "Provider Business Practice Location Address Postal Code",
    },
    "nppes_practice_location_pfile": {
        "line1": "Provider Secondary Practice Location Address- Address Line 1",
        "line2": "Provider Secondary Practice Location Address-  Address Line 2",
        "city": "Provider Secondary Practice Location Address - City Name",
        "state": "Provider Secondary Practice Location Address - State Name",
        "zipcode": "Provider Secondary Practice Location Address - Postal Code",
    },
}

# set base directory path
base_directory = pathlib.Path(__file__).parent.joinpath("./")
input_directory = base_directory / "input"
output_directory = base_directory / "output"


# adds geocoded geocodio metadata to file based on passed in address column mapping
def add_geocodio_metadata(
    input_file_path: pathlib.Path,
    geocodio_catalog_file: pathlib.Path,
    address_columns: Dict[str, str],
) -> None:
    # store unique addresses in dict with indexed key for easy lookup
    address_dict = {}
    with open(geocodio_catalog_file, mode="r", newline="") as geocodio_catalog:
        # expected header:
        # [
        #   geocodio_line1,geocodio_city,geocodio_state,geocodio_postal_code,Latitude,Longitude,
        #   Accuracy Score,Accuracy Type,Number,Street,Unit Type,Unit Number,City,State,County,Zip,Country,Source
        # ]
        geocodio_catalog_reader = csv.DictReader(geocodio_catalog)
        for row in geocodio_catalog_reader:
            key = (
                # for some reason the linter doesn't like strings here so type ignore
                row["geocodio_line1"],
                row["geocodio_city"],
                row["geocodio_state"],
                row["geocodio_postal_code"][:5],
            )
            address_dict[key] = row

    input_file_name = input_file_path.stem

    if geocodio_catalog_reader.fieldnames is None:
        print("Geocodio Catalog file is missing fieldnames")
        return

    geocodio_meta_fieldnames = [
        field.replace(" ", "_")
        for field in geocodio_catalog_reader.fieldnames
        if "geocodio" not in field
    ]

    # open a writer for addresses in the file that are missing a geocodio entry
    missing_addresses_path = output_directory / "missing_addresses"
    missing_addresses_file = (
        missing_addresses_path / f"{input_file_name}_missing_addresses.csv"
    )
    missing_addresses_file.parent.mkdir(parents=True, exist_ok=True)
    with missing_addresses_file.open(mode="w", newline="") as out_missing:
        missing_writer = csv.DictWriter(
            out_missing,
            fieldnames=["addr_line_1", "city", "state", "postal_code"],
            quoting=csv.QUOTE_ALL,
        )
        missing_writer.writeheader()
        missing = set()

        output_file_path = output_directory / "geocoded_geocodio_files"
        output_file = output_file_path / f"{input_file_name}_geocoded.csv"
        output_file.parent.mkdir(parents=True, exist_ok=True)

        # open both the input and output files
        with (
            input_file_path.open(mode="r", newline="") as input_file,
            output_file.open(mode="w", newline="") as out_file,
        ):

            input_reader = csv.DictReader(input_file)

            if input_reader.fieldnames is None:
                print("Input file is missing fieldnames")
                return

            input_fieldnames_without_lat_long = [
                field
                for field in input_reader.fieldnames
                if field not in ["Latitude", "Longitude"]
            ]

            writer_fieldnames = []
            for field in geocodio_meta_fieldnames:
                if field in ["Latitude", "Longitude"]:
                    writer_fieldnames.append(field)
                else:
                    writer_fieldnames.append(f"geocodio_{field}")

            writer = csv.DictWriter(
                out_file,
                fieldnames=input_fieldnames_without_lat_long + writer_fieldnames,
                quoting=csv.QUOTE_ALL,
            )

            writer.writeheader()
            for row in input_reader:
                # pull key from input file
                key = (
                    row[address_columns["line1"]],
                    row[address_columns["city"]],
                    row[address_columns["state"]],
                    row[address_columns["zipcode"]][:5],
                )
                # write row with all geocodio fields from lookup dict
                if key in address_dict.keys():
                    row_copy = dict(row)

                    # just in case delete lat/long from input file if they're there
                    if "Latitude" in row_copy.keys():
                        del row_copy["Latitude"]
                    if "Longitude" in row_copy.keys():
                        del row_copy["Longitude"]

                    for field in geocodio_meta_fieldnames:
                        if field in ["Latitude", "Longitude"]:
                            row_copy[field] = address_dict[key][field]
                        else:
                            row_copy[f"geocodio_{field}"] = address_dict[key][
                                field.replace("_", " ")
                            ]

                    writer.writerow(row_copy)

                # write missing address if not found
                else:
                    if key not in missing:
                        missing.add(key)
                        missing_writer.writerow(
                            {
                                "addr_line_1": key[0],
                                "city": key[1],
                                "state": key[2],
                                "postal_code": key[3],
                            }
                        )


# finds cases where the same address hash in a file has different lat/longs
def save_address_collisions(
    glob_string: str, address_column_mapping: Dict[str, str]
) -> None:
    address_collisions: Dict[str, Dict[str, Dict[str, List[int]]]] = {}
    file_type = glob_string.split("/")[-2]

    # check all files in a directory
    for csv_filename in glob.glob(glob_string, recursive=True):
        with open(csv_filename, mode="r") as csv_file:
            csv_reader = csv.DictReader(csv_file)
            count = 1

            for row in csv_reader:
                # store address column keys
                address_key = (
                    row[address_column_mapping["line1"]],
                    row[address_column_mapping["line2"]],
                    row[address_column_mapping["city"]],
                    row[address_column_mapping["state"]],
                    row[address_column_mapping["zipcode"]],
                )

                # store lat and long
                lat_long_key = (
                    row["Latitude"],
                    row["Longitude"],
                )

                # if the keys for different lat/longs are found save them again
                if str(address_key) in address_collisions.keys():
                    if str(lat_long_key) in address_collisions[str(address_key)].keys():
                        if (
                            csv_filename
                            in address_collisions[str(address_key)][
                                str(lat_long_key)
                            ].keys()
                        ):
                            address_collisions[str(address_key)][str(lat_long_key)][
                                csv_filename
                            ].append(count)
                        else:
                            address_collisions[str(address_key)][str(lat_long_key)][
                                csv_filename
                            ] = [count]

                    else:
                        address_collisions[str(address_key)][str(lat_long_key)] = {
                            csv_filename: [count]
                        }

                # if we haven't added these keys yet, do that here
                else:
                    address_collisions[str(address_key)] = {
                        str(lat_long_key): {csv_filename: [count]}
                    }

                count += 1

    # write output to file
    output_file = (
        output_directory / "address_collisions" / f"{file_type}_address_collisions.json"
    )
    output_file.parent.mkdir(parents=True, exist_ok=True)
    with output_file.open(mode="w") as outfile:
        json.dump(address_collisions, outfile, indent=2)


# pass in list of columns to write new file with only those columns
def extract_csv_columns(
    input_file_path: str, output_file_path: str, columns_to_extract: List[str]
) -> None:
    with (
        open(input_file_path, mode="r") as infile,
        open(output_file_path, mode="w", newline="") as outfile,
    ):
        reader = csv.DictReader(infile)
        writer = csv.DictWriter(outfile, fieldnames=columns_to_extract)
        writer.writeheader()
        for row in reader:
            writer.writerow({column: row[column] for column in columns_to_extract})


# pass in a reader object and generate iterations based on chunk_size
def batch_process_csv_reader(
    reader: Iterable[Any], chunk_size: int
) -> Generator[Any, None, None]:
    chunk = []
    for row in reader:
        chunk.append(row)
        if len(chunk) == chunk_size:
            yield chunk
            chunk = []
    if len(chunk) > 0:
        yield chunk


# pulls out address columns and lat and long from a geocoded file based on column mapping
# since this appends, you should either truncate the file or create a new blank one
def build_geocoded_address_catalog(
    input_file: str, output_file: str, column_mapping: Dict[str, str]
) -> None:
    # open output file in append mode
    with open(output_file, mode="a", newline="") as outfile:

        # create list of header fields and write header
        column_names = [c for c in column_mapping.keys()] + ["Latitude", "Longitude"]
        writer = csv.DictWriter(outfile, fieldnames=column_names, quoting=csv.QUOTE_ALL)
        writer.writeheader()

        # open input file and iterate over rows, writing out the selected fields
        with open(input_file, "r") as infile:
            reader = csv.DictReader(infile)
            for row in reader:
                columns = [column_mapping[col] for col in column_mapping.keys()]
                columns += ["Latitude", "Longitude"]
                selected_row = [row[col] for col in columns if col in row]
                writer.writerow(dict(zip(column_names, selected_row)))


# combines several address catalogs in a directory into a single file
def combine_address_catalogs(
    input_file_glob: str = "output/catalog/*.csv",
    output_file_path: pathlib.Path = output_directory
    / "catalog"
    / "full_address_catalog.csv",
) -> None:
    address_set = set()
    with output_file_path.open(mode="w", newline="") as output_file:
        fieldnames: Sequence[str] = []
        for input_file in glob.glob(input_file_glob):

            # ignore full catalog files, rename them if they should be included
            if "full_address_catalog" in input_file:
                continue

            # for each line in each file, try to add the address to the set
            with open(input_file, "r") as infile:
                catalog_reader = csv.DictReader(infile)

                for row in catalog_reader:
                    if catalog_reader.fieldnames is not None:
                        address_key = "|".join(
                            [row[k] for k in catalog_reader.fieldnames]
                        )
                        address_set.add(address_key)

        # iterate over address set and write out values that were geocoded
        catalog_writer = csv.DictWriter(
            output_file, fieldnames=fieldnames, quoting=csv.QUOTE_ALL
        )
        catalog_writer.writeheader()
        for key in address_set:
            row_dict = dict(zip(catalog_writer.fieldnames, key.split("|")))
            if float(row_dict["Latitude"]) == 0 and float(row_dict["Longitude"]) == 0:
                continue
            catalog_writer.writerow(row_dict)


# pulls unique addresses out of address catalog files and uses them to add geocoded data to input file
def extract_addresses_to_be_geocoded(
    address_catalog_glob: str,
    data_file_path: pathlib.Path,
    column_mapping: Dict[str, str],
) -> None:
    address_catalog_set = set()
    for f in glob.glob(address_catalog_glob):
        with open(f, "r") as address_catalog_file:
            catalog_reader = csv.DictReader(address_catalog_file)
            for row in catalog_reader:
                address_key = ",".join(
                    [row[k] for k in ["line1", "line2", "city", "state", "zipcode"]]
                )
                address_catalog_set.add(address_key)

    found_address_counter = 0
    with (
        data_file_path.open(mode="r") as data_file,
        open(f"output/{str(data_file_path)}_addresses_to_geocode.csv", "w") as out_file,
    ):
        data_reader = csv.DictReader(data_file)
        if data_reader.fieldnames is None:
            print("No fieldnames in data file")
            return

        data_reader_fieldnames = [field for field in data_reader.fieldnames]
        csv_writer = csv.DictWriter(out_file, fieldnames=data_reader_fieldnames)
        csv_writer.writeheader()
        for row in data_reader:
            address_key = ",".join([row[v] for k, v in column_mapping.items()])

            if address_key in address_catalog_set:
                found_address_counter += 1

            else:
                csv_writer.writerow(row)

    print(f"Found {found_address_counter} addresses cached")


# checks address cache collection for list of addresses
# returns a list of addresses missing from the address cache
def check_cache(
    collection: Collection[Dict[str, Any]],
    raw_addresses: List[RawAddress],
) -> Tuple[List[RawAddress], List[RawAddress]]:
    # save addresses in a dictionary to look up later
    raw_address_dict = {}
    for address in raw_addresses:
        raw_address_dict[address.to_hash()] = address

    # save address hashes in a set for easy lookup
    unique_address_hashes: Set[str] = set(raw_address_dict.keys())

    # find cached response for all hash keys in one request
    query = {"address_hash": {r"$in": list(unique_address_hashes)}}
    lookup_result: pymongo.cursor.Cursor[Any] = collection.find(query)

    # save cached hashes
    cached_hashes = set()
    for result in lookup_result:
        cached_hashes.add(result["address_hash"])

    # isolate missing hashes
    missing_hashes = set(unique_address_hashes - cached_hashes)

    missing_addresses_list = [
        raw_address_dict[address_hash] for address_hash in missing_hashes
    ]
    cached_addresses_list = [
        raw_address_dict[address_hash] for address_hash in cached_hashes
    ]

    return cached_addresses_list, missing_addresses_list


# saves vendor responses to address cache
def save_to_cache(
    _collection: Collection[Dict[str, Any]],
    vendor_responses: List[VendorResponse],
) -> None:
    requests = [
        UpdateOne(
            {"address_hash": vr.related_raw_address.to_hash()},
            {
                "$set": {
                    "vendor_std_address": vr.api_call_response,
                    "vendor_name": vr.vendor_name,
                    "response_version": vr.response_version,
                }
            },
            upsert=True,
        )
        for vr in vendor_responses
        if vr.related_raw_address
    ]
    if requests:
        _collection.bulk_write(requests=requests)


# uses a list of raw addresses to pull out the related vendor responses in a list
def filter_cached_addresses(
    cache_result: List[RawAddress], addresses: List[Dict[Any, Any]]
) -> List[Dict[Any, Any]]:
    uncached_address_hashes = set()
    for result in cache_result:
        uncached_address_hashes.add(result.to_hash())

    filtered_addresses = []
    for address in addresses:
        raw_address = RawAddress(
            address_id=address["RecordID"],
            line1=address["line1"],
            line2=address["line2"],
            city=address["city"],
            state=address["state"],
            zipcode=address["zipcode"],
        )
        if raw_address.to_hash() in uncached_address_hashes:
            filtered_addresses.append(address)

    return filtered_addresses


# takes an input catalog file and saves missing addresses to cache
def cache_addresses_from_file(
    catalog_file_path: pathlib.Path,
    address_mapping: Dict[str, str],
    connection_string: str,
    save_results_to_cache: bool = False,
    overwrite_cache: bool = False,
    batch_size: int = 10000,
) -> None:
    cache_miss_count = 0
    cache_hits_count = 0

    with catalog_file_path.open(mode="r") as fp:
        reader = csv.DictReader(fp)
        count = 0

        # perform in batches so the message to mongo doesn't grow too big
        for chunk in batch_process_csv_reader(reader, batch_size):
            raw_address_list: List[RawAddress] = []
            chunk_copy = []

            # iterate over rows and save a copy of each row (serves as vendor response)
            for row in chunk:
                row_copy = row.copy()
                raw_address_list.append(
                    RawAddress(
                        address_id=str(count),
                        line1=row[address_mapping["line1"]],
                        line2=row[address_mapping["line2"]],
                        city=row[address_mapping["city"]],
                        state=row[address_mapping["state"]],
                        zipcode=row[address_mapping["zipcode"]],
                    )
                )
                row_copy["RecordID"] = str(count)
                chunk_copy.append(row_copy)
                count += 1

            # get mongo client from connection_string
            mongo_client: MongoClient[Dict[str, Any]] = pymongo.MongoClient(
                connection_string
            )
            _db = mongo_client.get_database("helix_address_cache")
            __collection = _db["helix_address_cache"]

            # check cache for addresses in batch
            cache_hits, cache_misses = check_cache(
                raw_addresses=raw_address_list, collection=__collection
            )

            cache_hits_count += len(cache_hits)
            cache_miss_count += len(cache_misses)

            if save_results_to_cache:
                vendor_response = filter_cached_addresses(
                    cache_result=cache_misses, addresses=chunk_copy
                )
                # noinspection PyProtectedMember
                vendor_responses = StandardizingVendor()._to_vendor_response(
                    vendor_response=vendor_response,
                    raw_addresses=raw_address_list,
                    vendor_name="Geocodio",
                    response_version="1",
                )
                save_to_cache(
                    vendor_responses=vendor_responses, _collection=__collection
                )

                if overwrite_cache:
                    vendor_response = filter_cached_addresses(
                        cache_result=cache_hits, addresses=chunk_copy
                    )
                    # noinspection PyProtectedMember
                    vendor_responses = StandardizingVendor()._to_vendor_response(
                        vendor_response=vendor_response,
                        raw_addresses=raw_address_list,
                        vendor_name="Geocodio",
                        response_version="1",
                    )
                    save_to_cache(
                        vendor_responses=vendor_responses, _collection=__collection
                    )

    print(f"cache_hits={cache_hits_count} cache_misses={cache_miss_count}")
