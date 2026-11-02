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

# The path to local raw sequence data files if local, path is prefix to the
# genoscope directory: www.genoscope/etc...
LOCAL_DATA_PATH_PREFIX = Path("/usr/local/scratch/emo-bon-geneoscope-data")

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
    do_mapping,
    do_gene_prediction,
    threads,
    debug,
):
    """
    Run MEGAHIT and MetaQUAST on a pair of EMO BON technical replicates.

    Input parameters:
    technical_replicates    - List of tuples where each is a pair of
                              source_mat_ids from a technical replicate
    input_data_directory    - directory for the raw sequence data if downloading
                              from remote
    output_data_directory   - where the assembly is saved
    run_quast               - run Metaquast
    do_mapping              - run bowtie2 mapping
    do_gene_prediction      - run Prodigal
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

    # Check for executables
    if not shutil.which("bowtie2"):
        log.error("Cannot find bowtie2 the environment")
        sys.exit()
    if not shutil.which("prodigal"):
        log.error("Cannot find prodigal the environment")
        sys.exit()
    if not shutil.which("samtools"):
        log.error("Cannot find samtools the environment")
        sys.exit()

    # Ensure top-level megahit output dir exists
    Path("working/megahit_output").mkdir(parents=True, exist_ok=True)

    # Download the raw data file sequences if necessary
    # justpaths works here because genoscope results are on the same machine
    justpaths = True
    if justpaths:
        # NB using direct paths to genoscope directory
        # Create raw data download dir if necessary
        # data_directory = Path(input_data_directory)
        # data_directory.mkdir(parents=True, exist_ok=True)
        log.info(f"Using local data files at {LOCAL_DATA_PATH_PREFIX}")
    else:
        log.info(f"Downloading remote raw data file to {input_data_directory}")

    data_paths_for_pairs = []
    for tech_rep in technical_replicates:
        data_paths = download_raw_sequences_of_replicate_pair(
            [tech_rep[0], tech_rep[1]], justpaths=justpaths
        )
        # Need to adjust paths to the full local path
        if justpaths:
            # Oh God, recursion:
            def prefix_paths(paths, prefix):
                if isinstance(paths, (list, tuple)):
                    return [prefix_paths(p, prefix) for p in paths]
                return f"{prefix}/{paths}"

            data_paths_for_pairs.append(
                prefix_paths(data_paths, LOCAL_DATA_PATH_PREFIX)
            )
        else:
            data_paths_for_pairs.append(data_paths)

    count = 1
    for pair in data_paths_for_pairs:
        for p in pair:
            log.info(f"Data paths for replicate pair {count}:")
            log.info(f"\tforward - {p[0]}")
            log.info(f"\treverse - {p[1]}")
        count += 1

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

    assert len(forwards) == len(reverses), f"{len(forwards)} != {len(reverses)}"

    if input_data_directory:
        # Using remote data
        log.error(
            "This is broken because of mounting the correct data directory"
            "into the megahit container"
        )
        sys.exit()
    else:
        # Using local data
        megahit_forwards_param = ",".join(
            str(path.replace(str(LOCAL_DATA_PATH_PREFIX), "/data")) for path in forwards
        )
        megahit_reverses_param = ",".join(
            str(path.replace(str(LOCAL_DATA_PATH_PREFIX), "/data")) for path in reverses
        )
        bowtie2_forwards_param = ",".join(str(path) for path in forwards)
        bowtie2_reverses_param = ",".join(str(path) for path in reverses)
        # for r1, r2 in megahit_forwards_param:
        #    log.debug(f"\tMEGAHIT rep1 forward: {r1}")
        #    log.debug(f"\tMEGAHIT rep2 forward: {r2}")
        # for r1, r2 in megahit_reverses_param:
        #    log.debug(f"\tMEGAHIT rep1 reverse: {r1}")
        #    log.debug(f"\tMEGAHIT rep2 reverse: {r2}")
        # for r1, r2 in bowtie2_forwards_param:
        #    log.debug(f"\tBOWTIE2 rep1 forward: {r1}")
        #    log.debug(f"\tBOWTIE2 rep2 forward: {r2}")
        # for r1, r2 in bowtie2_reverses_param:
        #    log.debug(f"\tBOWTIE2 rep1 reverse: {r1}")
        #    log.debug(f"\tBOWTIE2 rep2 reverse: {r2}")

    # Build megahit output path (do not created dir)
    megahit_output_dir = Path("megahit_output", f"{output_data_directory}")
    analysis_output_dir = Path("working", megahit_output_dir)
    path_to_contigs = Path(analysis_output_dir, "final.contigs.fa")
    if analysis_output_dir.exists():
        # If output dir exists MEGAHIT assembly must be present
        if not path_to_contigs.exists():
            log.error(
                f"MEGAHIT output dir already exists at:"
                f"{analysis_output_dir} but assembly is missing. "
                "MEGAHIT refuses to write to an already existing output directory"
            )
            sys.exit()
        else:
            log.info(f"MEGAHIT assembly found at {path_to_contigs} - skipping assembly")
    else:
        # Run MEGAHIT
        cmd = (
            f"apptainer run "
            f"-B ./working:/output "
            f"-B /usr/local/scratch/emo-bon-geneoscope-data:/data "
            f"sifs/megahit.sif "
            f"megahit -o /output/{megahit_output_dir} "
            f"--min-contig-len 500 --num-cpu-threads {threads} "
            f"--memory 120 "
            f"-1 {megahit_forwards_param} "
            f"-2 {megahit_reverses_param}"
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
    if not run_quast:
        log.info("MetaQUAST not requested to be run")
    else:
        path_to_mquast_output = Path(analysis_output_dir, "mquast")
        # Check to see if already present
        if Path(path_to_mquast_output, "report.txt").exists():
            log.info(
                f"MQUAST analysis found at {path_to_mquast_output} - " "skipping MQUAST"
            )
        else:
            log.info("Running MetaQUAST...")
            cmd = (
                f"apptainer run sifs/quast.sif metaquast.py --threads {threads} "
                f"--max-ref-number 0 {path_to_contigs} -o {path_to_mquast_output}"
            )
            output = subprocess.run(cmd, shell=True, capture_output=True)
            if output.returncode != 0:
                raise RuntimeError(
                    f"MetaQUAST command failed: {output.stderr.decode()}"
                )
            else:
                log.info("MetaQUAST successfully completed")

    if not do_mapping:
        log.info("Bowtie2 mapping not requested to be run")
    else:
        # BOWTIE2
        # Build an index and map reads
        bowtie2_build_result = Path(f"{analysis_output_dir}/final.contigs.fa.1.bt2")
        if bowtie2_build_result.exists():
            log.info(
                f"Bowtie2 index found at {bowtie2_build_result} "
                f"- skipping Bowtie2-build index building"
            )
        else:
            log.info("Building index with bowtie2-build...")
            # Build the index
            cmd = (
                f"bowtie2-build {path_to_contigs} {path_to_contigs} "
                f"> {analysis_output_dir}/bowtie2-build.log"
            )
            output = subprocess.run(cmd, shell=True, capture_output=True)
            if output.returncode != 0:
                raise RuntimeError(
                    f"bowtie2-build command failed: {output.stderr.decode()}"
                )
            else:
                log.info("bowtie2 indexing successfully completed")
        bowtie2_bam_result = Path(f"{analysis_output_dir}/assembly.bam")
        if bowtie2_bam_result.exists():
            log.info(
                f"Bowtie2 assembly found at {bowtie2_bam_result} - skipping Bowtie2"
            )
        else:
            # Do the mapping
            log.info("Mapping reads with bowtie2...")
            cmd = (
                f"bowtie2 -x {path_to_contigs} "
                f"-1 {bowtie2_forwards_param} "
                f"-2 {bowtie2_reverses_param} "
                f"--very-sensitive -p 8 2> {analysis_output_dir}/bowtie2.log "
                f"| samtools sort -@ 8 -o {bowtie2_bam_result} -"
            )
            log.info(f"Running bowtie2: {cmd}")
            output = subprocess.run(cmd, shell=True, capture_output=True)
            if output.returncode != 0:
                raise RuntimeError(
                    f"bowtie2 mapping command failed: {output.stderr.decode()}"
                )
            else:
                log.info("bowtie2 mapping successfully completed")

        if not do_gene_prediction:
            log.info("Prodigal gene prediction not requested to be run")
        else:
            prodigal_output = Path(f"{analysis_output_dir}/prodigal")
            prodigal_result_test = Path(f"{prodigal_output}/annotations.gff")
            if prodigal_result_test.exists():
                log.info(
                    f"Prodigal annotations found at {prodigal_result_test} "
                    f"- skipping Prodigal gene predictions"
                )
            else:
                Path(prodigal_output).mkdir(exist_ok=True)
                contigs_path = Path(f"{analysis_output_dir}/final.contigs.fa")
                gff_out = Path(f"{prodigal_output}/annotations.gff")
                proteins_out = Path(f"{prodigal_output}/proteins.faa")
                genes_out = Path(f"{prodigal_output}/genes.ffn")
                logout = Path(f"{prodigal_output}/log")
                # Run prodigal
                cmd = (
                    f"prodigal -i {contigs_path} "
                    f"-p meta "
                    f"-f gff "
                    f"-o {gff_out} "
                    f"-a {proteins_out} "
                    f"-d {genes_out} "
                    f"> {logout}"
                )
                log.info(f"Running Prodigal: {cmd}")
                output = subprocess.run(cmd, shell=True, capture_output=True)
                if output.returncode != 0:
                    raise RuntimeError(
                        f"Prodigal gene prediction command failed: {output.stderr.decode()}"
                    )
                else:
                    log.info("Prodigal gene prediction successfully completed")
    return True


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
        default=None,
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
        "-m",
        "--do_bowtie2_mapping",
        help="Run Bowtie2 mapping. Default: False",
        action="store_true",
        default=False,
    )
    parser.add_argument(
        "-g",
        "--do_gene_prediction",
        help="Run Prodigal gene predicition: Default: False",
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
        args.do_mapping,
        args.do_gene_prediction,
        args.threads,
        args.debug,
    )
