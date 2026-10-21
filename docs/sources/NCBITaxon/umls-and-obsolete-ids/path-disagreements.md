# UMLS CUIs whose direct and MeSH paths to NCBITaxon disagree

48 of the 66,793 UMLS CUIs that reach NCBITaxon both directly and through MeSH reach a different
NCBITaxon each way (all of them are in `path-disagreements.csv`). A random sample of 10:

| CUI | direct NCBITaxon | MeSH | NCBITaxon via MeSH |
|---|---|---|---|
| UMLS:C0600450 Blattellidae | NCBITaxon:3046527 Blattellidae | MESH:D020048 Blattellidae | NCBITaxon:1049651 Ectobiidae (German cockroach family) |
| UMLS:C0993566 Daboia | NCBITaxon:42188 Daboia | MESH:D017840 Daboia | NCBITaxon:8707 Daboia russelii (Russell's viper) |
| UMLS:C0009169 Coca | NCBITaxon:289672 Erythroxylum coca (coca) | MESH:D003041 Coca | NCBITaxon:13511 Erythroxylum |
| UMLS:C0314845 Achromobacter xylosoxidans | NCBITaxon:85698 Achromobacter xylosoxidans | MESH:D042441 Achromobacter denitrificans | NCBITaxon:32002 Achromobacter denitrificans |
| UMLS:C1060600 Russula cremoricolor | NCBITaxon:2860945 Russula cremoricolor | MESH:C000701688 Russula cremoricolor | NCBITaxon:125791 Russula cremicolor |
| UMLS:C1031468 Wasabia | NCBITaxon:75806 Eutrema japonicum (wasabe) | MESH:D031228 Wasabia | NCBITaxon:98005 Eutrema |
| UMLS:C0751990 Epsilonproteobacteria | NCBITaxon:3031852 Epsilonproteobacteria | MESH:D020565 Epsilonproteobacteria | NCBITaxon:29547 Campylobacterota (e-proteobacteria) |
| UMLS:C0325152 Orycteropodidae | NCBITaxon:9816 Orycteropodidae (aardvark) | MESH:D000091123 Orycteropodidae | NCBITaxon:9815 Tubulidentata (aardvarks) |
| UMLS:C1016673 Bupleurum chinense | NCBITaxon:52451 Bupleurum chinense | MESH:C000712280 Bupleurum falcatum | NCBITaxon:46367 Bupleurum falcatum |
| UMLS:C0331277 Aloysia | NCBITaxon:105887 Aloysia | MESH:C000655467 Aloysia | NCBITaxon:925377 Aloysia citrodora (lemon verbena) |
