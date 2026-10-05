# emobon-assembly-downstream
Analyses of EMO BON data beyond the MetaGOflow analyses and RO-Crate building

a) make assemblies and mappings
    -- analyses_by_observatory.py loop for
        -- megahit_and_metaquast_replicates.py
        runs megahit on all 4 raw reads files of technical reps
        runs mquast (optional)
        runs bowtie2 mapping (optional) 

b) test technical replicate

    -- calculate_BrayCurtis_between_technical_replicates.py
       uses the MGF SSU and LSU taxonomies combined to calculate a BC metric
        between rep1 and rep2
    -- megahit_assembly_stats.sh
       takes the table output from calculate_BrayCurtis...
       adds the Alignment Rate, solid Kmer value, read number, and k-mer depth
       from bowtie2 and megahit logs

c) plotting
    -- plot_BC_versus_AR.py


The correct relationship:
Metric	Meaning
High solid k-mers + Low mean depth	HIGH diversity community — too many distinct genomes for the sequencing depth given
Low solid k-mers + High mean depth	LOW diversity community — few genomes, each well-covered

Corrected reading of your samples:

    EMT21_Wa_16-17 (4.8× depth): 6.34B solid k-mers at only 4.8× mean depth = very high diversity community that was undersampled by sequencing. The assembly fails because each genome sits at the edge of or below the k-mer solidity threshold.
    BPNS_Wa_6-7 (20.2× depth): 1.50B solid k-mers at 20.2× mean depth = lower diversity community that was well-sampled by sequencing. Fewer distinct genomes, each with sufficient depth to form long contigs.
    AAOT_Wa_26-27 (16.8× depth): 1.52B solid k-mers at 16.8× = similar to BPNS_Wa_6-7 — moderate diversity, well-sampled.

The distinction I confused:

    Richness/diversity: number of distinct genomes in the community (proxied by total solid k-mers)
    Coverage/depth: reads per genome (proxied by mean k-mer depth)

Your worst samples have high richness + low coverage — exactly the combination that breaks metagenome assembly. I said "low diversity" when I meant "low coverage." These are not the same thing.

Revised interpretation of the reject list:
Sample	Solid k-mers	Depth	Correct diagnosis
EMT21_Wa_16-17	6.34B (high)	4.8× (low)	High diversity + undersampled → reject
AAOT_Wa_61-62	6.75B (high)	5.9× (low)	High diversity + undersampled → reject
EMT21_Wa_31-32	5.30B (medium-high)	6.0× (low)	High diversity + undersampled → reject
BPNS_Wa_6-7	1.50B (low)	20.2× (high)	Low diversity + oversampled → ✅ keep

What this means:

Samples with >5B solid k-mers at <7× depth are undersampled high-diversity communities. They're not "process failures" — they're real biological conditions where the sequencing strategy (depth per sample) was insufficient for the community complexity. Rejecting them from an assembly-based pipeline is correct, but the reason is insufficient sequencing depth, not replicate inconsistency or technical artefact.

If these samples are biologically essential (e.g., represent a key habitat or timepoint), the solution is re-sequencing to higher depth, not discarding. If they're expendable, rejection is justified as "assembly-quality insufficient for downstream."

Relationship	r	Reading
depth ↔ AR	≈ +0.89 (r² ≈ 0.79)	Strong. Depth explains ~80% of recruitment variance.
depth ↔ BC	≈ −0.38	Weak. Most BC variance is not depth-driven.
BC ↔ AR	≈ −0.42 (as before)	Mostly mediated through depth in the low tail.

Feature 1: the cliff sits at ~9.5–10×, sharper than the tiers I guessed. Between depth 10.2 (AR 77.6%) and 9.0 (AR 56.2%) there's a ~20-point drop with nothing in between; below ~7× it degrades again to the 26–43% floor. Above ~10× the AR plateaus at 77–83% and never reaches higher until the 20× outlier. That plateau is very likely your merge-rate ceiling showing through: the assemblies are built from merged reads only, so unmerged reads (~30–45% per sample) can only recruit when they happen to map to contigs others built. AR topping out ~80% at any depth is the structural signature of that pipeline choice, not a depth limit — consistent with, though not proof of, the merge fractions measured earlier this thread.

Feature 2: BC decouples from depth above ~8×. The weak r confirms what the second plot shows: elevated BC occurs at 11–14× just as low BC does (RFormosa_Wa_5-6 at 14.2× with BC 0.207 vs EMT21_Wa_21-22 at 12.0× with 0.052). Only below ~6.5× does BC become reliably inflated (EMT21_Wa_31-32 at 0.327, ESC68N_Wa_1-2 at 0.267). Implication: BC >0.2 at depth >8× is not explainable by assembly stochasticity — that's your genuine process/biology-difference category.


Headers for megahit_assemble_stats.py:
BC — Bray-Curtis dissimilarity (unitless, 0–1)

Dissimilarity between the taxonomic profiles of the two technical replicates, each computed independently (mseq taxonomy on each replicate's separately assembled merged reads). 0 = identical composition, 1 = completely disjoint. Used as the combine-or-reject gate: <0.15 is normal technical variance; ≥0.20 warrants investigation; >0.25 is grounds for rejection. Caution on interpretation: because each profile derives from an assembly that truncates the rare biosphere, BC measured this way is a conservative underestimate of true replicate dissimilarity at low depth, and an unstable quantity overall (see depth_x) — assembly stochasticity at marginal coverage can inflate or deflate it. Read it jointly with depth, never alone.

AR_pct — Alignment rate / recruitment (percent of reads)

Percentage of all raw paired reads from both replicates (all four FASTQ files) that Bowtie2 recruits to the pair's own combined MEGAHIT assembly. Units: % of input reads (properly paired + discordant + single-mapped, as reported by Bowtie2's overall alignment rate). Measures assembly completeness: how much of the sample's sequence the assembly actually represents. Because reads below assembly-sufficient coverage never enter contigs, AR falls when community diversity spreads the same sequencing depth thin. Expected ~77–83% at adequate depth — the ceiling being set by the fraction of reads that merged (only merged reads fed the assembly), not by depth alone. Below ~45% the assembly is unusable for downstream per-contig analyses.

reads_M — Raw input reads (millions of reads)

Total reads ingested by MEGAHIT's k=21 k-mer counter across all four FASTQ files of the pair (post-cleaning; the _clean files). Units: millions of reads. Range in this dataset: ~195–373 M. Primarily a sanity check — it constrains nothing by itself, but guards against input-volume surprises (missing files, truncated downloads). Note it is raw read count, not bases; with 151 bp reads, multiply by ~0.15 for approximate Gbp.

solid_B — Distinct solid k-mers (billions)

Count of distinct 21-mers that exceeded MEGAHIT's solidity (minimum-depth) threshold during the k=21 counting pass — the nodes of the assembly's initial de Bruijn graph. Units: billions of distinct k-mers. Interpretation: a proxy for community genomic complexity/richness — more distinct genomes and strains (plus, to a lesser extent, higher sequencing error) yield more distinct solid k-mers. Range here: 1.50–7.33 B. It is the numerator-context for depth: the same read count over more solid k-mers means each k-mer is covered less deeply.

depth_x — Mean k-mer depth (fold-coverage, "×")

Approximate mean coverage of the solid k-mer set: (reads_M × 131) / solid_B, where 131 = number of 21-mers per 151 bp read (151 − 21 + 1). Units: fold (×). Interpretation: the single best predictor of assembly quality in this dataset — mean reads supporting each solid k-mer, i.e. depth per distinct genomic unit rather than depth per sample. At >10×, assemblies consolidate (AR plateaus ~78–83%); between 6–9× they straddle the solidity threshold and fragment; below ~6× the assembly captures only a minority of the community. Low depth × high solid-k-mer count = high-diversity community undersampled for assembly — a design limitation, not a process error.

One composite reading rule worth stating alongside the column docs: depth_x predicts AR_pct (r ≈ +0.89), but explains little of BC (r ≈ −0.38) — so the table's own logic is: use depth to judge whether assembly-based results are trustworthy for a pair, and use BC (at adequate depth) to judge whether the replicates are consistent enough to combine.

Tier 1: Healthy pairs (the majority)
Cluster around 0.03–0.09. Examples:

    BPNS_6-7: 0.0278 (best)
    AAOT_46-47: 0.0382 (collapsed from 0.1743)
    EMT21_36-37: 0.0356
    NRMCB_68-69: 0.0369

Tier 2: Genuine composition outliers (remain elevated after normalisation)
ESC68N_Wa_1-2: 0.2740 (was 0.2669)
MBAL4_Wa_11-12: 0.2555 (was 0.2291)
AAOT_Wa_41-42: 0.2063 (was 0.2262)
