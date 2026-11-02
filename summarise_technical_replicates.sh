#!/usr/bin/env bash
# summarise_technical_replicates.sh
#
# Combined QC table per replicate pair: Bray-Curtis (raw + filtered, library-size
# normalised), assembly rate, MEGAHIT read/k-mer stats, depth index, mapping
# diagnostics, and a rule-based flag.
#
# Usage:
#   summarise_technical_replicates.sh RESULTS.tsv [MEGAHIT_OUTDIR]
#
#   RESULTS.tsv    output of the replicate-pair Bray-Curtis script, EXPECTED_COLS
#                  (default 22) tab-separated columns, pair id in column 1.
#                  BC values come from the combined SSU+LSU block:
#                    col 16 con_suma, col 17 con_sumb, col 18 con_bc_raw, col 19 con_bc_filt
#   MEGAHIT_OUTDIR defaults to $OUTDIR or working/megahit_output. One EMOBON_*/
#                  subdirectory per pair containing: log, bowtie2.log, assembly.bam
#
# Pairs without assembly.bam are skipped silently (mapping not started). Pairs with
# incomplete/unparseable inputs are skipped with a WARN on stderr. A summary of
# included/skipped counts is printed to stderr at the end.
#
# Tunable via environment: EXPECTED_COLS COL_SUMA COL_SUMB COL_BC_RAW COL_BC_FILT
#   READ_LEN_EFF BC_OUTLIER BC_HEALTHY DEPTH_RATIO_MAX AR_LOW AR_OK
#
# Output columns
#   sample_pair            Replicate pair identifier, e.g. EMOBON_BPNS_Wa_6-EMOBON_BPNS_Wa_7
#   BC_raw                 Library-size-normalised Bray-Curtis dissimilarity, full profiles.
#   BC_filt                BC restricted to lineages >0.01% abundance in BOTH replicates.
#                          Normally <= BC_raw; if similar, divergence is in the abundant core.
#   AR_pct                 Assembly rate: bowtie2 overall alignment rate (%) of raw reads
#                          mapped back to the assembly.
#   reads_M                Raw reads input to MEGAHIT (millions); summed over all read
#                          libraries ("Lib N ...: X reads, Y max read length" lines).
#   solid_B                Solid edges (k-mers), billions, from the LAST "Total number of
#                          solid edges" line (i.e. the largest k in the k-list).
#   depth_x                Depth INDEX = reads * READ_LEN_EFF / solid_edges. READ_LEN_EFF
#                          defaults to 131 (150 bp reads less trimming) and is NOT derived
#                          from the data. Solid edges are not genome size, so this is a
#                          relative index for comparing samples, not true coverage.
#   depth_ratio            Larger/smaller of con_suma and con_sumb. Flags library
#                          imbalance; not a composition metric (BC is already normalised).
#   discordant_rate_pct    Discordant pairs as % of all pairs. Expected ~3-4%.
#   concordant_0_rate_pct  Pairs with NO concordant AND NO discordant alignment, % of all
#                          pairs (taken from bowtie2's "aligned 0 times concordantly or
#                          discordantly" line; falls back to conc0 - discordant).
#   con_suma, con_sumb     Total marker counts (SSU+LSU) for replicate A / B.
#   flag                   Comma-separated issues, or "healthy" / "review":
#                            composition_outlier  BC_raw > BC_OUTLIER (0.20)
#                            low_AR               AR_pct < AR_LOW (50)
#                            depth_imbalanced     depth_ratio > DEPTH_RATIO_MAX (1.5)
#                            healthy              none of the above, BC_raw < BC_HEALTHY
#                                                 (0.11) and AR_pct >= AR_OK (75)
#                            review               none of the above but not clearly healthy
#
# Rows are sorted by depth_x, descending.

set -euo pipefail
export LC_ALL=C   # '.' decimal separator and numeric sort independent of user locale

(( BASH_VERSINFO[0] >= 4 )) || { echo "ERROR: bash >= 4 required" >&2; exit 1; }

RESULTS="${1:?Usage: $0 replicate_results.tsv [megahit_output_dir]}"
OUTDIR="${2:-${OUTDIR:-working/megahit_output}}"

EXPECTED_COLS="${EXPECTED_COLS:-22}"
COL_SUMA="${COL_SUMA:-16}"
COL_SUMB="${COL_SUMB:-17}"
COL_BC_RAW="${COL_BC_RAW:-18}"
COL_BC_FILT="${COL_BC_FILT:-19}"

READ_LEN_EFF="${READ_LEN_EFF:-131}"
BC_OUTLIER="${BC_OUTLIER:-0.20}"
BC_HEALTHY="${BC_HEALTHY:-0.11}"
DEPTH_RATIO_MAX="${DEPTH_RATIO_MAX:-1.5}"
AR_LOW="${AR_LOW:-50}"
AR_OK="${AR_OK:-75}"

warn() { printf 'WARN: %s\n' "$*" >&2; }

[[ -f "$RESULTS" ]] || { printf 'ERROR: results TSV not found: %s\n' "$RESULTS" >&2; exit 1; }
[[ -d "$OUTDIR" ]]  || { printf 'ERROR: MEGAHIT output dir not found: %s\n' "$OUTDIR" >&2; exit 1; }

shopt -s nullglob
dirs=( "$OUTDIR"/EMOBON_*/ )
(( ${#dirs[@]} )) || { printf 'ERROR: no EMOBON_*/ directories in %s\n' "$OUTDIR" >&2; exit 1; }

tmp=$(mktemp)
lookup=$(mktemp)
trap 'rm -f "$tmp" "$lookup"' EXIT

# ---- Load and validate the BC table once ------------------------------------
# Emits: id <TAB> bc_raw <TAB> bc_filt <TAB> suma <TAB> sumb
# Rows with wrong column count, non-numeric values, non-positive counts or
# duplicated ids are dropped with a warning. A non-numeric first row is treated
# as a header and ignored silently.
awk -F'\t' -v cols="$EXPECTED_COLS" -v ca="$COL_SUMA" -v cb="$COL_SUMB" \
    -v cr="$COL_BC_RAW" -v cf="$COL_BC_FILT" '
  function isnum(x) { return x ~ /^[0-9]+(\.[0-9]+)?([eE][-+]?[0-9]+)?$/ }
  { sub(/\r$/, "") }
  NF == 0 { next }
  NR == 1 && !isnum($cr) { next }                     # header
  NF != cols {
    printf "WARN: results row %d (%s) has %d columns, expected %d; skipping\n", NR, $1, NF, cols > "/dev/stderr"
    next
  }
  {
    id = $1
    if (++seen[id] > 1) next
    if (!isnum($cr) || !isnum($cf) || !isnum($ca) || !isnum($cb) || $ca + 0 <= 0 || $cb + 0 <= 0) {
      printf "WARN: non-numeric or non-positive BC/count values for %s; skipping\n", id > "/dev/stderr"
      bad[id] = 1; next
    }
    row[id] = $cr "\t" $cf "\t" $ca "\t" $cb
  }
  END {
    for (id in seen) {
      if (seen[id] > 1) {
        printf "WARN: %s appears %d times in results; skipping\n", id, seen[id] > "/dev/stderr"
      } else if (!(id in bad) && (id in row)) {
        print id "\t" row[id]
      }
    }
  }
' "$RESULTS" > "$lookup"

declare -A LOOKUP
while IFS=$'\t' read -r id rest; do
  LOOKUP["$id"]="$rest"
done < "$lookup"

# ---- Per-sample summary -----------------------------------------------------
n_ok=0; n_unmapped=0; n_skipped=0

for dir in "${dirs[@]}"; do
  dir="${dir%/}"
  id="${dir##*/}"
  log="$dir/log"
  bt2="$dir/bowtie2.log"

  # Mapping not started: skip silently
  if [[ ! -f "$dir/assembly.bam" ]]; then
    n_unmapped=$((n_unmapped + 1)); continue
  fi
  if [[ ! -f "$bt2" ]]; then
    warn "assembly.bam exists but no bowtie2.log for $id"
    n_skipped=$((n_skipped + 1)); continue
  fi
  if [[ ! -f "$log" ]]; then
    warn "no megahit log for $id"
    n_skipped=$((n_skipped + 1)); continue
  fi
  if [[ -z "${LOOKUP[$id]:-}" ]]; then
    warn "no valid BC row for $id in $RESULTS, skipping"
    n_skipped=$((n_skipped + 1)); continue
  fi

  IFS=$'\t' read -r bc_raw bc_filt suma sumb <<< "${LOOKUP[$id]}"

  # awk prints its row only in END, after every check has passed, so a failure
  # here leaves nothing partial behind.
  if ! row=$(awk -v id="$id" -v mlog="$log" \
      -v bc="$bc_raw" -v bcf="$bc_filt" -v suma="$suma" -v sumb="$sumb" \
      -v len="$READ_LEN_EFF" -v bc_out="$BC_OUTLIER" -v bc_ok="$BC_HEALTHY" \
      -v dr_max="$DEPTH_RATIO_MAX" -v ar_low="$AR_LOW" -v ar_ok="$AR_OK" '
    function fail(msg) {
      printf "WARN: %s for %s\n", msg, id > "/dev/stderr"; failed = 1; exit 1
    }
    { sub(/\r$/, "") }

    # ---- MEGAHIT log ----
    FILENAME == mlog {
      if (match($0, /[0-9]+ reads, [0-9]+ max read length/)) {
        s = substr($0, RSTART, RLENGTH); sub(/ reads.*/, "", s)
        if ($0 ~ /Lib [0-9]+/) reads_lib += s; else reads_any = s + 0
      }
      if ($0 ~ /Total number of solid edges:/) {
        e = $NF; gsub(/[^0-9]/, "", e); solid = e + 0      # keep last (largest k)
      }
      next
    }

    # ---- bowtie2.log ----
    {
      sub(/^[ \t]+/, "")
      if ($0 ~ /^[0-9]+ \([0-9.]+%\) were paired/)                total_pairs = $1 + 0
      if ($0 ~ /^[0-9]+ \([0-9.]+%\) aligned concordantly 0 times/) conc0 = $1 + 0
      if ($0 ~ /^[0-9]+ \([0-9.]+%\) aligned discordantly 1 time/)  disc = $1 + 0
      if ($0 ~ /^[0-9]+ pairs aligned 0 times concordantly or discordantly/) { none = $1 + 0; have_none = 1 }
      if ($0 ~ /overall alignment rate/) { pct = $1; gsub(/%/, "", pct); pct += 0; have_ar = 1 }
    }

    END {
      if (failed) exit 1
      reads = (reads_lib > 0) ? reads_lib : reads_any
      if (reads <= 0 || solid <= 0) fail("could not parse read/solid-edge counts from MEGAHIT log")
      if (total_pairs <= 0)         fail("no paired-read count in bowtie2.log (unpaired run or incomplete log)")
      if (!have_ar)                 fail("no overall alignment rate in bowtie2.log (mapping incomplete?)")

      if (!have_none) { none = conc0 - disc; if (none < 0) none = 0 }

      depth             = (reads * len) / solid
      discordant_rate   = (disc / total_pairs) * 100
      concordant_0_rate = (none / total_pairs) * 100
      a = suma + 0; b = sumb + 0
      depth_ratio = (a <= b) ? (b / a) : (a / b)

      flag = ""
      if (bc + 0 > bc_out + 0)       flag = flag (flag ? "," : "") "composition_outlier"
      if (pct < ar_low + 0)          flag = flag (flag ? "," : "") "low_AR"
      if (depth_ratio > dr_max + 0)  flag = flag (flag ? "," : "") "depth_imbalanced"
      if (flag == "") flag = (bc + 0 < bc_ok + 0 && pct >= ar_ok + 0) ? "healthy" : "review"

      printf "%s\t%s\t%s\t%.2f\t%.1f\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\t%s\t%s\t%s\n", \
        id, bc, bcf, pct, reads / 1e6, solid / 1e9, depth, \
        depth_ratio, discordant_rate, concordant_0_rate, suma, sumb, flag
    }
  ' "$log" "$bt2"); then
    n_skipped=$((n_skipped + 1)); continue
  fi

  printf '%s\n' "$row" >> "$tmp"
  n_ok=$((n_ok + 1))
done

{
  printf "sample_pair\tBC_raw\tBC_filt\tAR_pct\treads_M\tsolid_B\tdepth_x\tdepth_ratio\tdiscordant_rate_pct\tconcordant_0_rate_pct\tcon_suma\tcon_sumb\tflag\n"
  sort -t$'\t' -k7,7gr "$tmp"
}

printf 'Summary: %d included, %d not yet mapped (no assembly.bam), %d skipped with warnings\n' \
  "$n_ok" "$n_unmapped" "$n_skipped" >&2
