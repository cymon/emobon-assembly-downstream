#! /usr/bin/env python3

import sys
import textwrap
import argparse
import subprocess
import shutil
import logging as log
from pathlib import Path

# Import methods from metagoflow-data-products-ro-crate/utils
PATH_TO_MGF_METHODS = "../metagoflow-data-products-ro-crate/utils"
sys.path.append(PATH_TO_MGF_METHODS)
from technical_replicates import download_raw_sequences_of_replicate_pair  # noqa: E402

desc = """
Run MEGAHIT and METAQUAST on a technical replicate pair combining the raw
sequence data files
"""


def main(
    replica1,
    replica2,
    input_data_directory,
    download_raw_data,
    run_quast,
    threads,
    debug,
):
    """
    Run MEGAHIT and MetaQUAST on a pair of EMO BON technical replicates.

    Input parameters:
    replica1                - EMO BON source_mat_id of replicate 1
    replica2                - EMO BON source_mat_id of replicate 2
    input_data_directory    - directory for the raw sequence data
    download_raw_data       - download raw sequences to input_data_directory
    run_quast               - run metaquast analysis on resulting assembly
    threads                 - number of threads for both MEGAHIT and MetaQUAST
    debug                   - turn on debugging output

    Return: None
    """
    # Logging
    if debug:
        log_level = log.DEBUG
    else:
        log_level = log.INFO
    log.basicConfig(format="\t%(levelname)s: %(message)s", level=log_level)

    # Check for existance of software containers
    megahit_sif = Path("sifs/megahit.sif")
    if not megahit_sif.exists():
        log.error("Cannot find ./sifs/megahit.sif")
        sys.exit()
    quast_sif = Path("sifs/quast.sif")
    if not quast_sif.exists():
        log.error("Cannot find ./sifs/quast.sif")
        sys.exit()

    if not isinstance(threads, int):
        log.erorr("Threads must be an interger")
        sys.exit()

    # Ensure top-level megahit output dir exists
    Path("working/megahit_output").mkdir(parents=True, exist_ok=True)

    # Build megahit output path (do not created dir), check if exists and move
    # on, do this before downloading data
    megahit_output_dir = Path("megahit_output", f"{replica1}-{replica2}-megahit")
    analysis_output_dir = Path("working", megahit_output_dir)
    if analysis_output_dir.exists():
        log.info(f"Found previous analysis at {megahit_output_dir}")
        log.info("Skipping analysis...")
        return

    # Create raw data download dir if necessary
    data_directory = Path(input_data_directory)
    data_directory.mkdir(parents=True, exist_ok=True)

    # Download the raw data file sequences
    if download_raw_data:
        download_raw_sequences_of_replicate_pair(
            [replica1, replica2], outpath=input_data_directory
        )

    # Get paths to downloaded raw data files
    r1_files = list(Path(data_directory, replica1).glob("*.fastq.gz"))
    # Sort them so that _1 comes before _2!
    r1_files.sort()
    r2_files = list(Path(data_directory, replica2).glob("*.fastq.gz"))
    r2_files.sort()
    for rep in [r1_files, r2_files]:
        log.debug(f"Found: {rep[0]}")
        log.debug(f"Found: {rep[1]}")

    # Run MEGAHIT
    cmd = (
        f"apptainer run -B ./working:/output sifs/megahit.sif "
        f"megahit -o /output/{megahit_output_dir} "
        f"--min-contig-len 500 --num-cpu-threads {threads} "
        f"-1 {r1_files[0]},{r2_files[0]} "
        f"-2 {r1_files[1]},{r2_files[1]}"
    )
    log.info(f"Running MEGAHIT: {cmd}")
    output = subprocess.run(cmd, shell=True, capture_output=True)
    if output.returncode != 0:
        raise RuntimeError(f"Apptainer command failed: {output.stderr.decode()}")
    else:
        log.info("MEGAHIT successfully completed")
    # MEGAHIT produces a lot of intermediate data which needs to be removed
    old_data = Path(analysis_output_dir, "intermediate_contigs")
    shutil.rmtree(old_data)

    # QUAST
    if run_quast:
        log.info("Running MetaQUAST...")
        path_to_contigs = Path("working", megahit_output_dir, "final.contigs.fa")
        path_to_mquast_output = Path(analysis_output_dir, "mquast")
        cmd = (
            f"apptainer run sifs/quast.sif metaquast.py --threads {threads} "
            f"--max-ref-number 0 {path_to_contigs} -o {path_to_mquast_output}"
        )
        output = subprocess.run(cmd, shell=True, capture_output=True)
        if output.returncode != 0:
            raise RuntimeError(f"MetaQUAST command failed: {output.stderr.decode()}")
        else:
            log.info("MetaQUAST successfully completed")
    log.info("Done.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=textwrap.dedent(desc),
    )
    parser.add_argument(
        "replica1",
        help=("EMO BON source_mat_id of the first technical replicate"),
    )
    parser.add_argument(
        "replica2",
        help=("EMO BON source_mat_id of the second technical replicate"),
    )
    parser.add_argument(
        "input_data_directory",
        help=(
            "Name of data directory containing the raw sequence data.\n"
            "<data_directory>/<source_mat_id>/<raw_sequences files>"
        ),
    )
    parser.add_argument(
        "-r",
        "--download_raw_data",
        help="Download the raw data files. Default: False",
        action="store_true",
        default=False,
    )
    parser.add_argument(
        "-q",
        "--run_quast",
        help="Run MetaQUAST on resulting contigs. Default: False",
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
        args.replica1,
        args.replica2,
        args.input_data_directory,
        args.download_raw_data,
        args.run_quast,
        args.threads,
        args.debug,
    )
