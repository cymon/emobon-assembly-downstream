#!/usr/bin/env python3

"""
test_technical_replicates.py — Bray-Curtis dissimilarity between two technical
replicate taxonomy profiles (mseq.tsv: ID <TAB> count <TAB> lineage <TAB> length),
combining SSU + LSU marker profiles per replicate.

Intent is to identify broken/bad replicates that shouldn't be combined and
distinguish between a) broken due to community complexity and lack of read depth
an b) mixed community due to a process mistake.

Both bc_raw and bc_filt are computed on **library-size-normalised** proportions
via scipy.spatial.distance.braycurtis. The count-space formulation (which was
used in earlier versions of this script) conflated library-size imbalance with
composition: for identical compositions differing only by depth factor r, it
returned (r−1)/(r+1), reaching 0.313 at r=1.9. That artefact dominated the high-BC
pairs in the EMOBON dataset and falsified the type-a/type-b test.

Metrics output per marker (SSU, LSU) and combined:
  * *_suma / *_sumb          — total marker counts per replicate (untransformed)
  * *_bc_raw                 — BC on full profiles, normalised to proportions
  * *_bc_filt                — BC restricted to lineages >0.01% (prop) in BOTH replicates
  * *_kept                   — lineages passing the abundance filter (intersection)
  * *_rare                   — shared lineages below the threshold in either replicate
  * *_sing                   — singleton lineages (present in exactly one replicate)

Interpretation:
  * Healthy technical replicates cluster near a baseline floor (~0.08–0.12 in
    these EMOBON data, driven by assembly stochasticity and classification
    instability in the abundant core).
  * Elevated *_bc_filt (>~0.18) indicates genuine abundant-lineage divergence
    between replicates, not library-size artefact.
  * The original type-a/type-b distinction (collapse of bc_filt vs raw under
    filtering) is invalid for this dataset: count-space BC already conflated
    the two signals; with normalisation there is no differential collapse to
    measure. The meaningful comparison is *_bc_filt against the population
    median floor, not against *_bc_raw.

Diagnostic expectations for the EMOBON dataset (to be verified):
  * Pairs with severe marker-count imbalance (e.g. 1.5–2× depth ratio) should
    show *_bc_raw ≈ *_bc_filt, both near the baseline floor if the composition
    is truly similar, or elevated if the imbalance coincides with genuine
    profile divergence.
  * Singletons (*_sing) are counted for informational purposes but do not
    contribute to *_bc_filt; high *_sing alongside low *_bc_filt does not
    imply undersampling noise in the abundant core.

Author note: This script was modified on 2026-10-05 to replace the count-space
BC implementation (which was arithmetically correct but confounded with library
size) with proportion-space BC via SciPy. The bray_curtis_filtered() function
was previously left in count-space, creating a scale mismatch between bc_raw
and bc_filt; that bug was corrected in the same revision.
"""

import os
import sys
import subprocess
from pathlib import Path
from scipy.spatial.distance import braycurtis

DVC_REPO_ROOT = "/usr/local/storage1/git-repos/metagoflow-data-products-ro-crate/analysis-results-cluster-01-crate"

verbose = False


def parse_taxonomy(paths):
    """Aggregate counts by lineage across one or more mseq.tsv files."""
    counts = {}
    for path in paths:
        file_counts = {}
        skipped = 0
        with open(path) as fh:
            for line in fh:
                if line.startswith("#"):
                    continue
                line = line.rstrip("\n")
                if not line:
                    continue
                parts = line.split("\t")
                if len(parts) < 3:
                    skipped += 1
                    continue
                try:
                    count = float(parts[1])
                except ValueError:
                    skipped += 1
                    continue
                lineage = parts[2]
                file_counts[lineage] = file_counts.get(lineage, 0.0) + count
        if skipped:
            print(f"[warn] {path}: skipped {skipped} non-data lines", file=sys.stderr)
        for k, v in file_counts.items():
            counts[k] = counts.get(k, 0.0) + v
    return counts


def proportions(counts):
    total = sum(counts.values())
    return {k: v / total for k, v in counts.items()} if total else {}


def bray_curtis(a, b):
    """Library-size-normalised BC on lineage-count dicts, via SciPy."""
    pa, pb = proportions(a), proportions(b)
    if not pa or not pb:
        return 1.0
    keys = sorted(set(pa) | set(pb))
    return braycurtis(
        [pa.get(k, 0.0) for k in keys],
        [pb.get(k, 0.0) for k in keys],
    )


def bray_curtis_filtered(a, b, min_abundance_prop=0.0001):
    """BC over lineages present in BOTH samples with abundance
    > min_abundance_prop (proportion of each sample's total) in each.

    Returns tuple: (bc, n_kept, n_shared_rare, n_singletons)
    """
    keys_a, keys_b = set(a), set(b)
    n_singletons = len(keys_a ^ keys_b)
    all_common = keys_a & keys_b

    pa, pb = proportions(a), proportions(b)
    if not pa or not pb:
        return 1.0, 0, len(all_common), n_singletons

    kept = [
        t
        for t in all_common
        if pa[t] > min_abundance_prop and pb[t] > min_abundance_prop
    ]

    n_shared_rare = len(all_common) - len(kept)
    if not kept:
        return 1.0, 0, len(all_common), n_singletons

    bc = braycurtis(
        [pa[t] for t in kept],
        [pb[t] for t in kept],
    )
    return float(bc), len(kept), n_shared_rare, n_singletons


def report(label, a, b, min_abundance_prop=0.0001):
    bc_raw = bray_curtis(a, b)
    bc_filt, n_kept, n_shared_rare, n_sing = bray_curtis_filtered(
        a, b, min_abundance_prop
    )

    if verbose:
        print(f"--- {label} ---")
        print(f"sum_A\t{sum(a.values()):.0f}")
        print(f"sum_B\t{sum(b.values()):.0f}")
        print(f"lineages_A\t{len(a)}")
        print(f"lineages_B\t{len(b)}")
        print(f"shared_lineages\t{len(set(a) & set(b))}")
        print(f"singleton_lineages\t{n_sing}")
        print(f"bc_raw\t{bc_raw:.4f}")
        print(f"bc_filtered({min_abundance_prop*100:.2f}%)\t{bc_filt:.4f}")
        print(f"kept_lineages\t{n_kept}")
        print(f"shared_rare_lineages\t{n_shared_rare}")

    return [
        f"{sum(a.values()):.0f}",
        f"{sum(b.values()):.0f}",
        f"{bc_raw:.4f}",
        f"{bc_filt:.4f}",
        str(n_kept),
        str(n_shared_rare),
        str(n_sing),
    ]


def crate_path(crate, marker):
    return (
        "/usr/local/storage1/git-repos/metagoflow-data-products-ro-crate/"
        f"analysis-results-cluster-01-crate/{crate}/"
        f"taxonomy-summary/{marker}/DBB.merged_{marker}.fasta.mseq.tsv"
    )


def crate_path_from_dvc_repo(crate, marker):
    return f"{crate}/taxonomy-summary/{marker}/DBB.merged_{marker}.fasta.mseq.tsv"


def checkout_taxonomy_files(ssu1, ssu2, lsu1, lsu2):
    """Checkout the DBB.merged_{marker}.fasta.mseq.tsv files from the ro-crates"""
    cwd = os.getcwd()
    os.chdir(Path(DVC_REPO_ROOT))
    cmd = f"dvc checkout {ssu1} {ssu2} {lsu1} {lsu2}"
    if verbose:
        print(f"Running: {cmd}")
    output = subprocess.run(cmd, shell=True, capture_output=True)
    if output.returncode != 0:
        raise RuntimeError(f"dvc checkout command failed: {output.stderr.decode()}")
    else:
        if verbose:
            print("dvc checkout successfully completed")
    os.chdir(cwd)


def merge_profiles(*profiles):
    """Merge one or more lineage->count dicts, summing shared keys."""
    merged = {}
    for p in profiles:
        for k, v in p.items():
            merged[k] = merged.get(k, 0.0) + v
    return merged


def main(sample_replicate1, sample_replicate2):
    crate1 = f"{sample_replicate1}-ro-crate"
    crate2 = f"{sample_replicate2}-ro-crate"

    # Taxonomy paths relative to the dvc repo:
    ssu1_repo, ssu2_repo = (
        crate_path_from_dvc_repo(crate1, "SSU"),
        crate_path_from_dvc_repo(crate2, "SSU"),
    )
    lsu1_repo, lsu2_repo = (
        crate_path_from_dvc_repo(crate1, "LSU"),
        crate_path_from_dvc_repo(crate2, "LSU"),
    )

    checkout_taxonomy_files(ssu1_repo, ssu2_repo, lsu1_repo, lsu2_repo)

    # Absolute paths to the taxonomy files
    ssu1, ssu2 = crate_path(crate1, "SSU"), crate_path(crate2, "SSU")
    lsu1, lsu2 = crate_path(crate1, "LSU"), crate_path(crate2, "LSU")

    # Parse each marker's profiles once
    ssu_a = parse_taxonomy([ssu1])
    ssu_b = parse_taxonomy([ssu2])
    lsu_a = parse_taxonomy([lsu1])
    lsu_b = parse_taxonomy([lsu2])

    # Per-marker diagnostics
    ssu = report("SSU only", ssu_a, ssu_b)
    lsu = report("LSU only", lsu_a, lsu_b)

    # Combined: plain concatenation (marker read depth weights)
    concat_a = merge_profiles(ssu_a, lsu_a)
    concat_b = merge_profiles(ssu_b, lsu_b)
    con = report("combined", concat_a, concat_b)

    reps = f"{sample_replicate1}-{sample_replicate2}"
    print("\t".join([reps, *ssu, *lsu, *con]))

    # Cleanup files from the crate repos:
    for path in [ssu1, ssu2, lsu1, lsu2]:
        try:
            Path(path).unlink()  # raises FileNotFoundError if absent
            if verbose:
                print(f"removed {path}")
        except OSError as e:
            print(f"ERROR removing {path}: {e}")
            sys.exit()


if __name__ == "__main__":
    replicate_pairs = [
        ["EMOBON_AAOT_Wa_1", "EMOBON_AAOT_Wa_2"],
        ["EMOBON_AAOT_Wa_26", "EMOBON_AAOT_Wa_27"],
        ["EMOBON_AAOT_Wa_41", "EMOBON_AAOT_Wa_42"],
        ["EMOBON_AAOT_Wa_46", "EMOBON_AAOT_Wa_47"],
        ["EMOBON_AAOT_Wa_61", "EMOBON_AAOT_Wa_62"],
        ["EMOBON_AAOT_Wa_66", "EMOBON_AAOT_Wa_67"],
        ["EMOBON_BPNS_Wa_1", "EMOBON_BPNS_Wa_2"],
        ["EMOBON_BPNS_Wa_21", "EMOBON_BPNS_Wa_22"],
        ["EMOBON_BPNS_Wa_26", "EMOBON_BPNS_Wa_27"],
        ["EMOBON_BPNS_Wa_41", "EMOBON_BPNS_Wa_42"],
        ["EMOBON_BPNS_Wa_46", "EMOBON_BPNS_Wa_47"],
        ["EMOBON_BPNS_Wa_61", "EMOBON_BPNS_Wa_62"],
        ["EMOBON_BPNS_Wa_66", "EMOBON_BPNS_Wa_67"],
        ["EMOBON_BPNS_Wa_6", "EMOBON_BPNS_Wa_7"],
        ["EMOBON_EMT21_Wa_16", "EMOBON_EMT21_Wa_17"],
        ["EMOBON_EMT21_Wa_1", "EMOBON_EMT21_Wa_2"],
        ["EMOBON_EMT21_Wa_21", "EMOBON_EMT21_Wa_22"],
        ["EMOBON_EMT21_Wa_31", "EMOBON_EMT21_Wa_32"],
        ["EMOBON_EMT21_Wa_36", "EMOBON_EMT21_Wa_37"],
        ["EMOBON_EMT21_Wa_6", "EMOBON_EMT21_Wa_7"],
        ["EMOBON_ESC68N_Wa_1", "EMOBON_ESC68N_Wa_2"],
        ["EMOBON_ESC68N_Wa_3", "EMOBON_ESC68N_Wa_4"],
        ["EMOBON_MBAL4_Wa_11", "EMOBON_MBAL4_Wa_12"],
        ["EMOBON_MBAL4_Wa_13", "EMOBON_MBAL4_Wa_14"],
        ["EMOBON_MBAL4_Wa_1", "EMOBON_MBAL4_Wa_2"],
        ["EMOBON_MBAL4_Wa_3", "EMOBON_MBAL4_Wa_4"],
        ["EMOBON_NRMCB_Wa_1", "EMOBON_NRMCB_Wa_2"],
        ["EMOBON_NRMCB_Wa_23", "EMOBON_NRMCB_Wa_24"],
        ["EMOBON_NRMCB_Wa_28", "EMOBON_NRMCB_Wa_29"],
        ["EMOBON_NRMCB_Wa_43", "EMOBON_NRMCB_Wa_44"],
        ["EMOBON_NRMCB_Wa_48", "EMOBON_NRMCB_Wa_49"],
        ["EMOBON_NRMCB_Wa_63", "EMOBON_NRMCB_Wa_64"],
        ["EMOBON_NRMCB_Wa_68", "EMOBON_NRMCB_Wa_69"],
        ["EMOBON_RFormosa_Wa_11", "EMOBON_RFormosa_Wa_12"],
        ["EMOBON_RFormosa_Wa_15", "EMOBON_RFormosa_Wa_16"],
        ["EMOBON_RFormosa_Wa_1", "EMOBON_RFormosa_Wa_2"],
        ["EMOBON_RFormosa_Wa_21", "EMOBON_RFormosa_Wa_22"],
        ["EMOBON_RFormosa_Wa_25", "EMOBON_RFormosa_Wa_26"],
        ["EMOBON_RFormosa_Wa_5", "EMOBON_RFormosa_Wa_6"],
    ]

    print(
        "replicate_pair\t"
        "ssu_suma\tssu_sumb\tssu_bc_raw\tssu_bc_filt\tssu_kept\tssu_rare\tssu_sing\t"
        "lsu_suma\tlsu_sumb\tlsu_bc_raw\tlsu_bc_filt\tlsu_kept\tlsu_rare\tlsu_sing\t"
        "con_suma\tcon_sumb\tcon_bc_raw\tcon_bc_filt\tcon_kept\tcon_rare\tcon_sing"
    )
    for replicate_pair in replicate_pairs:
        main(replicate_pair[0], replicate_pair[1])
