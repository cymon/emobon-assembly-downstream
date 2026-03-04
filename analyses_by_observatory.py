#! /usr/bin/env python3


import sys
import textwrap
import argparse
import logging as log
from pathlib import Path
import pandas as pd

# Import methods from metagoflow-data-products-ro-crate/utils
PATH_TO_MGF_METHODS = "../metagoflow-data-products-ro-crate/utils"
sys.path.append(PATH_TO_MGF_METHODS)
from technical_replicates import get_technical_replicates  # noqa: E402
from technical_replicates import BROKEN_REPLICATE_PAIRS  # noqa: E402
from inventory_batch_samples import parse_sheet  # noqa: E402
from inventory_batch_samples import path_to_rocrate_repo  # noqa: E402
from megahit_and_metaquast_replicates import main as run_assembly  # noqa: E402

OBSERVATORIES_LOGSHEET = (
    "https://raw.githubusercontent.com/emo-bon/governance-crate/"
    "refs/heads/main/observatories.csv"
)

desc = """
Download all raw sequence data and analyse all technical replicate sample pairs
for a EMO BON station/observatory

a) run megahit and metaquast on each technical replicate
b) run megahit and metaquast on all of the technical replicates for a station
combined


"""


def _read_observatory_names():
    """ """
    df = pd.read_csv(OBSERVATORIES_LOGSHEET, encoding="iso-8859-1")
    all_stations = df[["EMOBON_observatory_id"]].values.tolist()
    stations = [station for sublist in all_stations for station in sublist]
    log.debug(f"Stations: {stations}")
    return stations


def main(
    station_name,
    env_package,
    sequence_data_directory,
    combined,
    threads,
    debug,
):
    """
    Default is to assemble all technical replicates individually.
    If the combined option is specified all replicates are assembled togther.
    """
    # Logging
    if debug:
        log_level = log.DEBUG
    else:
        log_level = log.INFO
    log.basicConfig(format="\t%(levelname)s: %(message)s", level=log_level)

    observatory_abbreviated_names = _read_observatory_names()
    if station_name not in observatory_abbreviated_names:
        log.error(
            f"Observatory name ({station_name}) must be one of "
            f"{', '.join(observatory_abbreviated_names)}"
        )
        sys.exit()
    if env_package not in ["filters", "sediments"]:
        log.error("environment must be one of either 'filters' or 'sediments'")
        sys.exit()

    # Get technical replicates
    reps_generator = get_technical_replicates(station_name, env_package)
    if not reps_generator:
        log.info("No technical replicates found")
        sys.exit()
    all_sample_replicates = list(reps_generator)
    log.info(f"{station_name} replicates {len(all_sample_replicates)}")
    count = 1
    for sample_replicate in all_sample_replicates:
        log.info(f"\t{count} {sample_replicate}")
        count += 1

    # Get list of station/env_package ro-crates
    abs_path_of_current_script = Path(__file__).resolve().parent
    rocrate_repo_path = path_to_rocrate_repo(abs_path_of_current_script)
    all_ro_crates = parse_sheet(env_package, rocrate_repo_path, debug=False)
    station_ro_crates = all_ro_crates[0].get(station_name, None)
    if not station_ro_crates:
        log.info(f"No ro-crates available for {station_name} {env_package}")
        sys.exit()

    # Remove any broken technical replicate pairs
    broken_pairs = [item for pair in BROKEN_REPLICATE_PAIRS for item in pair]
    log.debug(f"Broken pairs: {broken_pairs}")
    # Remove broken pairs from
    filtered_station_ro_crates = []
    for rocrate in station_ro_crates:
        if rocrate[0] in broken_pairs:
            log.info(f"Broken pair {rocrate[0]} removed")
            continue
        else:
            filtered_station_ro_crates.append(rocrate)
    log.info(f"{station_name} ro-crates {len(filtered_station_ro_crates)}")
    count = 1
    for rocrate in filtered_station_ro_crates:
        log.info(f"\t{count} {rocrate}")
        count += 1

    source_mat_ids_in_rocrates = [n[0] for n in filtered_station_ro_crates]

    replicates_with_rocrates = []
    for sample_replicate in all_sample_replicates:
        log.debug(f"Doing rep: {sample_replicate}")
        found = []
        # Some sample_replicates have 4 reps but only 2 sequenced and ro-crated
        # Need to find 2 reps in the replicate
        for rep in sample_replicate:
            if rep in source_mat_ids_in_rocrates:
                found.append(rep)
        if len(found) == 0:
            continue
        elif len(found) > 2:
            log.error(f"Found more than 2 replicates for a sample: {found}")
        elif len(found) == 1:
            log.info(
                f"Sample replicate {sample_replicate} has only one rocrate {found}"
            )
        else:
            replicates_with_rocrates.append(found)
    log.info(f"Sample replicates pairs with ro-crates {len(replicates_with_rocrates)}")
    count = 1
    for replicate in replicates_with_rocrates:
        log.info(f"\t{count} {replicate}")
        count += 1

    # Download raw sequence data for replicate pair
    data_directory = Path(sequence_data_directory)
    data_directory.mkdir(parents=True, exist_ok=True)
    download_raw_data = True
    run_quast = True
    for pair in replicates_with_rocrates:
        log.info(
            f"Running assembly: pair0={pair[0]}, pair1={pair[1]},"
            f" data_directory={data_directory},"
            f" download_raw_data={download_raw_data},"
            f" run_quast={run_quast}, threads={threads},"
            f" debug={debug}"
        )
        run_assembly(
            pair[0],
            pair[1],
            data_directory,
            download_raw_data,
            run_quast,
            threads,
            debug,
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=textwrap.dedent(desc),
    )
    parser.add_argument(
        "station_name",
        help=("EMO BON Station/Observatory Abbreviated Name"),
    )
    parser.add_argument(
        "env_package",
        help=("EMO BON environment parameter: either 'filters' or 'sediment'"),
    )
    parser.add_argument(
        "sequence_data_directory",
        help=("Path to directory to store raw sequence data"),
    )
    parser.add_argument(
        "-c",
        "--combine",
        help="Combine all technical replicates",
        action="store_true",
        default=False,
    )
    parser.add_argument(
        "-t",
        "--threads",
        help="Number of threads. Default: 1",
        default=1,
        type=int,
    )
    parser.add_argument(
        "-d",
        "--debug",
        help="DEBUG logging",
        action="store_true",
        default=False,
    )
    args = parser.parse_args()
    main(
        args.station_name,
        args.env_package,
        args.sequence_data_directory,
        args.combine,
        args.threads,
        args.debug,
    )
