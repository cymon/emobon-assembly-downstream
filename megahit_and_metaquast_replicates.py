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
Run MEGAHIT and METAQUAST on a technical replicates combining the raw
sequence data files, either as a pair or combined for all pairs

If importing the main function:
technical_replicates = [
    (source_mat_id_A, source_mat_id_B),
    (source_mat_id_C, source_mat_id_D),
    etc...
]
where A and B are a technical replicate pair and
where C and D are a technical replicate pair
etc

else from the command line --technical_replicates is a flat list of replicate
pairs one after another...

All replicates pairs are combined.

"""


def main(
    technical_replicates,
    input_data_directory,
    output_data_directory,
    run_quast,
    threads,
    debug,
):
    """
    Run MEGAHIT and MetaQUAST on a pair of EMO BON technical replicates.

    Input parameters:
    technical_replicates    - List of tuples where each is a pair of
                              source_mat_ids from a technical replicate
    input_data_directory    - directory for the raw sequence data
    output_data_directory   - where the assembly is saved
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
        log.error("Threads must be an interger")
        sys.exit()

    # Ensure top-level megahit output dir exists
    Path("working/megahit_output").mkdir(parents=True, exist_ok=True)

    # Create raw data download dir if necessary
    data_directory = Path(input_data_directory)
    data_directory.mkdir(parents=True, exist_ok=True)

    # Download the raw data file sequences if necessary
    data_paths_for_pairs = []
    for tech_rep in technical_replicates:
        data_paths = download_raw_sequences_of_replicate_pair(
            [tech_rep[0], tech_rep[1]], outpath=input_data_directory
        )
        data_paths_for_pairs.append(data_paths)

    # Build megahit output path (do not created dir)
    megahit_output_dir = Path("megahit_output", f"{output_data_directory}")
    analysis_output_dir = Path("working", megahit_output_dir)
    if analysis_output_dir.exists():
        log.error(
            f"Output directory exists: {analysis_output_dir}"
            f"... refusing to go any further"
        )
        sys.exit()

    # Forward and reverse raw seq data files:
    # data_paths_for_pairs
    # [
    # [[rp1_forward, rp1_reverse],[rp2_forward, rp2_reverse]],
    # [[rp3_forward, rp3_reverse],[rp4_forward, rp4_reverse]],
    # etc
    # ]

    forwards = [
        replicate[0]
        for replicate_pair in data_paths_for_pairs
        for replicate in replicate_pair
    ]
    reverses = [
        replicate[1]
        for replicate_pair in data_paths_for_pairs
        for replicate in replicate_pair
    ]
    forwards_param = ", ".join(forwards)
    reverses_param = ", ".join(reverses)
    log.debug(f"forwards_param = {forwards_param}")
    log.debug(f"reverses_param = {reverses_param}")

    # Run MEGAHIT
    cmd = (
        f"apptainer run -B ./working:/output sifs/megahit.sif "
        f"megahit -o /output/{megahit_output_dir} "
        f"--min-contig-len 500 --num-cpu-threads {threads} "
        f"-1 {forwards_param} "
        f"-2 {reverses_param}"
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
        "--technical_replicates",
        nargs="+",
        required=True,  # Mandatory
        help=(
            "List of source_mat_ids where each set of two are a pair"
            " of technical replicates."
        ),
    )
    parser.add_argument(
        "input_data_directory",
        help=(
            "Name of data directory containing the raw sequence data.\n"
            "<data_directory>/<source_mat_id>/<raw_sequences files>"
        ),
    )
    parser.add_argument(
        "output_data_directory",
        help=(
            "Name of output directory after "
            "./working/megahit_output/<output_data_directory>"
        ),
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

    # Pair the technical replciates
    if not len(args.technical_replicates) % 2 == 0:
        print(
            f"technical_replicates must be a list of pairs not "
            f" and odd number of items {len(args.technical_replicates)}"
        )
        sys.exit()
    replicate_pairs = list(
        zip(args.technical_replicates[::2], args.technical_replicates[1::2])
    )

    main(
        replicate_pairs,
        args.input_data_directory,
        args.output_data_directory,
        args.run_quast,
        args.threads,
        args.debug,
    )
