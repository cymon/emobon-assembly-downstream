# emobon-assembly-downstream

Analyses of EMO BON data beyond the MetaGOflow analyses and RO-Crate building

a) make assemblies and mappings
    - `analyses_by_observatory.py` loop for
      `megahit_and_metaquast_replicates.py`
        runs `megahit` on all 4 raw reads files of technical reps
        runs `mquast` (optional)
        runs `bowtie2` mapping (optional)
        runs `prodigal` gene prediction (optional) 

b) test technical replicate

    - `calculate_BrayCurtis_between_technical_replicates.py`
       uses the MGF SSU and LSU taxonomies combined to calculate a BC metric
        between technical replicate 1 and 2
    - `megahit_assembly_stats.sh`
       takes the table output from calculate_BrayCurtis...
       adds the Alignment Rate, solid Kmer value, read number, and k-mer depth
       from bowtie2 and megahit logs

c) plotting
    -- plot_BC_versus_AR.py


### Not to be confused:

    Richness/diversity: number of distinct genomes in the community (proxied by total solid k-mers)
    Coverage/depth: reads per genome (proxied by mean k-mer depth)

### The correct relationship:
Metric	Meaning
High solid k-mers + Low mean depth	HIGH diversity community — too many distinct genomes for the sequencing depth given
Low solid k-mers + High mean depth	LOW diversity community — few genomes, each well-covered

Bray-Curtis can be high due to community complexity, which is why we need the assembly and mapping to determine k-mer diversity and read depth. What we are looking for is high BC/low kmer diversity/high read depth - which would likely be the sign of an experimental broken pair.

### Correct reading of samples:

    EMT21_Wa_16-17 (4.8× depth): 6.34B solid k-mers at only 4.8× mean depth = very high diversity community that was undersampled by sequencing. The assembly fails because each genome sits at the edge of or below the k-mer solidity threshold.
    BPNS_Wa_6-7 (20.2× depth): 1.50B solid k-mers at 20.2× mean depth = lower diversity community that was well-sampled by sequencing. Fewer distinct genomes, each with sufficient depth to form long contigs.
    AAOT_Wa_26-27 (16.8× depth): 1.52B solid k-mers at 16.8× = similar to BPNS_Wa_6-7 — moderate diversity, well-sampled.

### Field descriptions in summary_statistics.tsv (from summarise_technical_replicates.sh):

-   *sample_pair*            Replicate pair identifier, e.g. EMOBON_BPNS_Wa_6-EMOBON_BPNS_Wa_7
-   *BC_raw*                 Library-size-normalised Bray-Curtis dissimilarity, full profiles.
-   *BC_filt*                BC restricted to lineages >0.01% abundance in BOTH replicates.
                          Normally <= BC_raw; if similar, divergence is in the abundant core.
-   *AR_pct*                 Assembly rate: bowtie2 overall alignment rate (%) of raw reads
                          mapped back to the assembly.
-   *reads_M*                Raw reads input to MEGAHIT (millions); summed over all read
                          libraries ("Lib N ...: X reads, Y max read length" lines).
-   *solid_B*                Solid edges (k-mers), billions, from the LAST "Total number of
                          solid edges" line (i.e. the largest k in the k-list).
-   *depth_x*                Depth INDEX = reads * READ_LEN_EFF / solid_edges. READ_LEN_EFF
                          defaults to 131 (150 bp reads less trimming) and is NOT derived
                          from the data. Solid edges are not genome size, so this is a
                          relative index for comparing samples, not true coverage.
-   *depth_ratio*            Larger/smaller of con_suma and con_sumb. Flags library
                          imbalance; not a composition metric (BC is already normalised).
-   *discordant_rate_pct*    Discordant pairs as % of all pairs. Expected ~3-4%.
-   *concordant_0_rate_pct*  Pairs with NO concordant AND NO discordant alignment, % of all
                          pairs (taken from bowtie2's "aligned 0 times concordantly or
                          discordantly" line; falls back to conc0 - discordant).
-   *con_suma*, *con_sumb*     Total marker counts (SSU+LSU) for replicate A / B.
-   *flag*                   Comma-separated issues, or "healthy" / "review":
                            - composition_outlier  BC_raw > BC_OUTLIER (0.20)
                            - low_AR               AR_pct < AR_LOW (50)
                            - depth_imbalanced     depth_ratio > DEPTH_RATIO_MAX (1.5)
                            - healthy              none of the above, BC_raw < BC_HEALTHY
                                                 (0.11) and AR_pct >= AR_OK (75)
                            - review               none of the above but not clearly healthy

 Rows are sorted by depth_x, descending.


