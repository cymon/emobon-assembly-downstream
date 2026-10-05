#!/usr/bin/env bash
# summarise_technical_replicates.sh

# Combined QC table per sample pair: BC (raw + filtered, normalised),
# AR, MEGAHIT reads, solid k-mers, depth, discordant, concordant-0.
# Skips silently any sample lacking a completed mapping (no assembly.bam).
#
# Expects RESULTS to be the output of test_technical_replicates.py with
# 22 columns; BC values are taken from the combined (SSU+LSU) block:
# col 16 con_suma, col 17 con_sumb, col 18 con_bc_raw, col 19 con_bc_filt

# Field descriptions for summarise_technical_replicates.sh output
#
# sample_pair:            Replicate pair identifier, e.g. EMOBON_BPNS_Wa_6-EMOBON_BPNS_Wa_7
#
# --- Bray-Curtis metrics (from calculate_BrayCurtis_between_technical_replicates.py, library-size normalised) ---
# BC_raw:                 Library-size-normalised Bray-Curtis dissimilarity on full
#                         profiles (SSU+LSU combined). Healthy pairs cluster 0.03-0.11;
#                         values >0.20 indicate genuine composition divergence
#                         between replicates.
# BC_filt:                BC restricted to lineages >0.01% abundance in BOTH replicates.
#                         Should be <= BC_raw. If similar to BC_raw, the dissimilarity
#                         is in the abundant core, not the rare tail.
# con_suma:               Total marker counts (SSU+LSU) for replicate A (first member
#                         of pair). Raw count before normalisation.
# con_sumb:               Total marker counts (SSU+LSU) for replicate B (second member).
#
# --- Depth balance diagnostic (computed in script) ---
# depth_ratio:            Larger/smaller ratio of con_suma vs con_sumb. Values >1.5
#                         flag library preparation imbalance (unequal merge rates,
#                         extraction yield, sequencing load). NOT a composition metric;
#                         the BC values above are already normalised against this.
#
# --- Assembly performance (from MEGAHIT log) ---
# reads_M:                Total raw reads input to MEGAHIT (millions). Parsed from
#                         the "X reads, Y max read length" header line.
# solid_B:                Total solid edges (k-mers) in the assembly (billions).
#                         From the "Total number of solid edges:" log line.
# depth_x:                Estimated mean coverage = (reads x 131bp) / solid_edges.
#                         The 131 assumes 150bp reads minus trimming. Low depth (<8x)
#                         predicts low AR and high assembly complexity artefacts.
# AR_pct:                 Assembly Rate = overall alignment rate (%) from bowtie2
#                         mapping of raw reads back onto the assembly. High AR (>80%)
#                         indicates good community recapture; low AR (<50%) suggests
#                         undersampling of a complex community.
#
# --- Mapping diagnostics (from bowtie2.log) ---
# discordant_rate_pct:    Discordant pairs as % of all pairs. Reflects insert-size tail
#                         beyond the bowtie2 -X 500 default. Expected ~3-4%; values >5%
#                         may indicate assembly mis-joins, chimeric contigs, or
#                         short-insert contamination.
# concordant_0_rate_pct:  Pairs with no concordant or discordant alignment (% of all
#                         pairs). High rates (>40%) indicate reads from taxa not
#                         captured in the assembly, or poor assembly overall.
#
# --- Interpretation guides ---
# Healthy pair:           BC_raw < 0.11, depth_ratio < 1.4, AR_pct > 75
# Depth-imbalanced but
#   compositionally OK:   BC_raw < 0.11, depth_ratio > 1.5 (e.g. EMT21_Wa_31-32)
# Composition outlier:    BC_raw > 0.20 regardless of depth_ratio
#   (ESC68N_Wa_1-2, MBAL4_Wa_11-12, AAOT_Wa_41-42)
# Assembly failure only:  Low AR_pct with normal BC (e.g. NRMCB_Wa_63-64);
#                         retain unless AR too low for downstream analysis
#
# All BC values derive from the combined SSU+LSU marker profile.

#Tier 1: Healthy pairs (the majority)
#Cluster around 0.03–0.09. Examples:
#
#    BPNS_6-7: 0.0278 (best)
#    AAOT_46-47: 0.0382 (collapsed from 0.1743)
#    EMT21_36-37: 0.0356
#    NRMCB_68-69: 0.0369
#
#Tier 2: Genuine composition outliers (remain elevated after normalisation)
#ESC68N_Wa_1-2: 0.2740 (was 0.2669)
#MBAL4_Wa_11-12: 0.2555 (was 0.2291)
#AAOT_Wa_41-42: 0.2063 (was 0.2262)

set -euo pipefail

RESULTS="${1:?Usage: $0 bray_curtis_replicate_results.tsv}"
OUTDIR="working/megahit_output"

[[ -f "$RESULTS" ]] || { echo "ERROR: results TSV not found: $RESULTS" >&2; exit 1; }

tmp=$(mktemp)
trap 'rm -f "$tmp"' EXIT

for dir in "$OUTDIR"/EMOBON_*/; do
  id=$(basename "$dir")
  log="$dir/log"
  bt2="$dir/bowtie2.log"

  # Skip silently if mapping hasn't been done yet
  [[ -f "$dir/assembly.bam" ]] || continue

  # Hard errors only for unexpected inconsistencies:
  # bam exists but its log is missing, or megahit log missing
  if [[ ! -f "$bt2" ]]; then
    echo "WARN: assembly.bam exists but no bowtie2.log for $id" >&2
    continue
  fi
  [[ -f "$log" ]] || { echo "WARN: no megahit log for $id" >&2; continue; }

  # Extract combined BC (proportion-normalised) and marker-count totals
  # from the 22-column replicate-pair results.
  bc_raw=$(awk -F'\t' -v id="$id" '$1==id {print $18}' "$RESULTS")
  bc_filt=$(awk -F'\t' -v id="$id" '$1==id {print $19}' "$RESULTS")
  suma=$(awk -F'\t' -v id="$id" '$1==id {print $16}' "$RESULTS")
  sumb=$(awk -F'\t' -v id="$id" '$1==id {print $17}' "$RESULTS")

  # bc_raw is required; bc_filt/suma/sumb must be present if bc_raw is
  [[ -n "${bc_raw:-}" ]] || { echo "WARN: no BC_raw value for $id, skipping" >&2; continue; }
  [[ -n "${bc_filt:-}" && -n "${suma:-}" && -n "${sumb:-}" ]] || {
    echo "WARN: incomplete BC row for $id, skipping" >&2; continue
  }

  awk -v id="$id" -v bc="$bc_raw" -v bcf="$bc_filt" \
      -v suma="$suma" -v sumb="$sumb" -v bt2="$bt2" '
    match($0, /[0-9]+ reads, [0-9]+ max read length/) {
      reads = substr($0, RSTART, RLENGTH)
      sub(/ reads.*/, "", reads)
    }
    /Total number of solid edges:/ {
      m = $NF; gsub(/[^0-9]/, "", m); solid = m
    }
    END {
      if (reads <= 0 || solid <= 0) {
        printf "WARN: bad parse of %s\n", id > "/dev/stderr"; exit 1
      }
      depth = (reads * 131) / solid

      # Parse bowtie2.log for concordant-0, discordant, and total pairs
      total_pairs = 0
      concordant_0_count = 0
      discordant_count = 0
      pct_align = ""

      while ((getline line < bt2) > 0) {
        sub(/^[ \t]+/, "", line)          # strip bowtie2 indentation
        if (line ~ /^[0-9]+ \([0-9]+\.[0-9]+%\) were paired/)
          total_pairs = line + 0
        if (line ~ /^[0-9]+ \([0-9]+\.[0-9]+%\) aligned concordantly 0 times/)
          concordant_0_count = line + 0
        if (line ~ /^[0-9]+ \([0-9]+\.[0-9]+%\) aligned discordantly 1 time/)
          discordant_count = line + 0
        if (line ~ /overall alignment rate$/) {
          split(line, a, " ")
          pct_align = a[1]; gsub(/%/, "", pct_align)
        }
      }
      close(bt2)

      if (total_pairs <= 0) {
        printf "WARN: no pair count in bowtie2.log for %s\n", id > "/dev/stderr"; exit 1
      }

      discordant_rate = (discordant_count / total_pairs) * 100
      concordant_0_rate = (concordant_0_count / total_pairs) * 100

      if (pct_align == "") {
        printf "WARN: no AR line in bowtie2.log for %s\n", id > "/dev/stderr"; exit 1
      }

      # Marker-count depth ratio between replicates (diagnostic for
      # library-size imbalance; the BC itself is normalised)
      depth_ratio = (suma <= sumb) ? (sumb / suma) : (suma / sumb)

      printf "%s\t%s\t%s\t%.2f\t%.1f\t%.2f\t%.1f\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\n", \
        id, bc, bcf, pct_align, reads/1e6, solid/1e9, depth, \
        depth_ratio, discordant_rate, concordant_0_rate, suma, sumb
    }
  ' "$log"
done > "$tmp"

{
  printf "sample_pair\tBC_raw\tBC_filt\tAR_pct\treads_M\tsolid_B\tdepth_x\tdepth_ratio\tdiscordant_rate_pct\tconcordant_0_rate_pct\tcon_suma\tcon_sumb\n"
  sort -t$'\t' -k7,7gr "$tmp"
}
