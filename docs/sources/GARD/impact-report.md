# Source impact report: GARD

- Generated: 2026-08-26 02:11:39 UTC
- Babel commit: 80948dcc0896969c3a19da04fa5de29cf7629869
- Source pipelines: disease
- Source prefixes: GARD
- Comparison mode: synthetic

## 1. Identifiers added

Totals: 16,214 identifiers across 1 prefix(es) in 1 pipeline(s).

### By prefix

- GARD: 16,214

### By pipeline

- disease: 16,214

## 2. Biolink types

### Overall declared type breakdown

- biolink:Disease: 16,214

### Source-declared (from each ids file)

- disease / GARD
  - biolink:Disease: 16,214

### Final compendium-assigned (after glom)

- disease / Disease.txt: 16,214 GARD identifiers

## 3. Cross-references added

Totals: 0 cross-reference rows across 0 concord file(s).

### By pipeline

- disease / GARD: 0

### Partner prefix breakdown (per pipeline)

- disease
  - (no concord rows)

### Join pathways (every asserted cross-reference, not only this source's own)

`status` is `added` when GARD's own concord file asserts the pathway and `from_other_source` when
another source's does — the latter may predate this addition. The prefix pair is sorted, so
`asserted_by` is what tells you which side declared it.

| pipeline | predicate | prefix pair | asserted by | status | xrefs |
|---|---|---|---|---|---|
| disease | `xref` | GARD ↔ MONDO | `MONDO_GARD` | from_other_source | 15,936 |
| disease | `xref` | DOID ↔ GARD | `DOID` | from_other_source | 1,902 |
| disease | `xref` | GARD ↔ NCIT | `GARD_LABEL` | from_other_source | 210 |
| disease | `xref` | GARD ↔ MONDO | `GARD_LABEL` | from_other_source | 42 |
| disease | `xref` | GARD ↔ orphanet | `GARD_LABEL` | from_other_source | 7 |
| disease | `xref` | GARD ↔ MESH | `GARD_LABEL` | from_other_source | 6 |

## 4. Clique impact

**Worst-case view.** This report is computed from the intermediate identifier and concord files and
cannot see downstream filtering that happens later in the build — most notably the Biolink Model's
per-class prefix restrictions, which drop identifiers whose prefix is not permitted for a clique's
biolink type. The counts and detail files below are therefore an *upper bound*: they show every
change the source could introduce before that filtering is applied.

### disease

- 12 new cliques composed only of GARD identifiers (a 0.00% increase over the 440,647 pre-existing
  cliques)
- 15,872 existing cliques contain GARD identifiers in the after state (3.60% of the 440,647
  pre-existing cliques). Of these, 14,304 cliques gain at least one structurally new identifier from
  GARD, and 1,568 already contained the GARD CURIE via an xref from another source — GARD's ids file
  now also lists those existing CURIEs as first-class typed identifiers.
- 22 existing cliques will be merged because of new GARD cross-references
- 14,314 structurally-new GARD identifiers are added to existing cliques (14,314 via expansion, 0
  via merges). This is distinct from the 14,326 existing cliques that change, since one clique can
  gain several identifiers.
- Total cliques in this pipeline go from 440,647 to 440,640
- Sample of new cliques (top 100, unsurvivable and largest first):
  [`impact-report/new-cliques-top-100.csv`](impact-report/new-cliques-top-100.csv)
- Full list of modified cliques (one row per added/preexisting GARD identifier):
  `impact-report/modified-cliques.csv` -- not committed (see `.gitignore`); regenerate it with
  `uv run source-impact-report`
- Cross-reference summary (join pathways with counts and example rows):
  [`impact-report/new-xrefs-summary.csv`](impact-report/new-xrefs-summary.csv)

#### Sample merges (up to 3)

- GARD:6408 bridges DOID:0050424, MONDO:0021055
- GARD:7193 bridges DOID:0111252, MONDO:0007039
- GARD:9281 bridges DOID:10041, MONDO:0018453

#### Sample pure-new cliques (up to 3)

- `GARD:15005` "Pacak-Zhung syndrome"
  **(NOT emitted — prefix not registered in Biolink Model for `biolink:Disease`)**
- `GARD:15006` "STAT5 Haploinsufficiency"
  **(NOT emitted — prefix not registered in Biolink Model for `biolink:Disease`)**
- `GARD:15009` "Monocytosis/myelocytosis, Autoimmunity, Gain of function, Immunodeficiency, Short
  stature" **(NOT emitted — prefix not registered in Biolink Model for `biolink:Disease`)**

#### Sample expanded cliques (up to 3)

Of the 15,872 cliques that contain GARD identifiers in the after state, 0 would also see their
preferred identifier change as a result of adding GARD. The sample below leads with
preferred-id-change cliques (if any), then structurally grown cliques, then cliques where GARD only
adds CURIEs that were already present via xref. Within each clique, identifiers are listed in the
same order they would appear in the compendium (biolink prefix priority, then lexicographic within
prefix).

- Clique with 68 identifiers — typed as `biolink:Disease` — gains 1 new member(s) from GARD:
  - [`MONDO:0004604`](http://purl.obolibrary.org/obo/MONDO_0004604) "Hodgkin's lymphoma,
    lymphocytic-histiocytic predominance" **(preferred)**
  - [`DOID:8543`](http://purl.obolibrary.org/obo/DOID_8543) "Hodgkin's lymphoma,
    lymphocytic-histiocytic predominance"
  - [`DOID:8567`](http://purl.obolibrary.org/obo/DOID_8567) "Hodgkin's lymphoma"
  - [`DOID:8628`](http://purl.obolibrary.org/obo/DOID_8628) "Hodgkin's lymphoma, lymphocytic
    depletion"
  - [`DOID:8642`](http://purl.obolibrary.org/obo/DOID_8642) "Hodgkin's paragranuloma"
  - [`DOID:8654`](http://purl.obolibrary.org/obo/DOID_8654) "Hodgkin's lymphoma, mixed cellularity"
  - [`OMIM:236000`](http://purl.obolibrary.org/obo/OMIM_236000)
  - [`OMIM:300221`](http://purl.obolibrary.org/obo/OMIM_300221)
  - [`OMIM:400021`](http://purl.obolibrary.org/obo/OMIM_400021)
  - [`orphanet:98293`](http://www.orpha.net/ORDO/Orphanet_98293)
  - [`orphanet:98845`](http://www.orpha.net/ORDO/Orphanet_98845)
  - [`EFO:0000183`](http://www.ebi.ac.uk/efo/EFO_0000183) "obsolete_Hodgkins lymphoma"
  - [`UMLS:C0019829`](http://identifiers.org/umls/C0019829) "Hodgkin Disease"
  - [`UMLS:C0152266`](http://identifiers.org/umls/C0152266) "Mixed Cellularity Hodgkin Lymphoma"
  - [`UMLS:C0152267`](http://identifiers.org/umls/C0152267) "Hodgkin lymphoma, lymphocyte depletion"
  - [`UMLS:C1266194`](http://identifiers.org/umls/C1266194) "Lymphocyte Rich Classical Hodgkin
    Lymphoma"
  - [`UMLS:C5235037`](http://identifiers.org/umls/C5235037) "Hodgkin sarcoma"
  - [`MESH:D006689`](http://id.nlm.nih.gov/mesh/D006689) "Hodgkin Disease"
  - [`MEDDRA:10020206`](http://identifiers.org/meddra/10020206)
  - [`MEDDRA:10020219`](http://identifiers.org/meddra/10020219)
  - [`MEDDRA:10020231`](http://identifiers.org/meddra/10020231)
  - [`MEDDRA:10020232`](http://identifiers.org/meddra/10020232)
  - [`MEDDRA:10020242`](http://identifiers.org/meddra/10020242)
  - [`MEDDRA:10020255`](http://identifiers.org/meddra/10020255)
  - [`MEDDRA:10020272`](http://identifiers.org/meddra/10020272)
  - [`MEDDRA:10020281`](http://identifiers.org/meddra/10020281)
  - [`MEDDRA:10020290`](http://identifiers.org/meddra/10020290)
  - [`MEDDRA:10020309`](http://identifiers.org/meddra/10020309)
  - [`MEDDRA:10020318`](http://identifiers.org/meddra/10020318)
  - [`MEDDRA:10020328`](http://identifiers.org/meddra/10020328)
  - [`MEDDRA:10020329`](http://identifiers.org/meddra/10020329)
  - [`MEDDRA:10020339`](http://identifiers.org/meddra/10020339)
  - [`MEDDRA:10025319`](http://identifiers.org/meddra/10025319)
  - [`MEDDRA:10063666`](http://identifiers.org/meddra/10063666)
  - [`NCIT:C164145`](http://purl.obolibrary.org/obo/NCIT_C164145) "Hodgkin's Sarcoma"
  - [`NCIT:C26956`](http://purl.obolibrary.org/obo/NCIT_C26956) "Hodgkin's Paragranuloma"
  - [`NCIT:C3517`](http://purl.obolibrary.org/obo/NCIT_C3517) "Mixed Cellularity Classic Hodgkin
    Lymphoma"
  - [`NCIT:C6913`](http://purl.obolibrary.org/obo/NCIT_C6913) "Lymphocyte-Rich Classic Hodgkin
    Lymphoma"
  - [`NCIT:C6914`](http://purl.obolibrary.org/obo/NCIT_C6914) "Hodgkin's Granuloma"
  - [`NCIT:C9283`](http://purl.obolibrary.org/obo/NCIT_C9283) "Lymphocyte-Depleted Classic Hodgkin
    Lymphoma"
  - [`NCIT:C9357`](http://purl.obolibrary.org/obo/NCIT_C9357) "Hodgkin Lymphoma"
  - [`SNOMEDCT:112687003`](http://snomed.info/id/112687003)
  - [`SNOMEDCT:1163005009`](http://snomed.info/id/1163005009)
  - [`SNOMEDCT:118599009`](http://snomed.info/id/118599009)
  - [`SNOMEDCT:118602004`](http://snomed.info/id/118602004)
  - [`SNOMEDCT:118605002`](http://snomed.info/id/118605002)
  - [`SNOMEDCT:118606001`](http://snomed.info/id/118606001)
  - [`SNOMEDCT:118607005`](http://snomed.info/id/118607005)
  - [`SNOMEDCT:118609008`](http://snomed.info/id/118609008)
  - [`SNOMEDCT:128799007`](http://snomed.info/id/128799007)
  - [`SNOMEDCT:14537002`](http://snomed.info/id/14537002)
  - [`SNOMEDCT:41529000`](http://snomed.info/id/41529000)
  - [`SNOMEDCT:46923007`](http://snomed.info/id/46923007)
  - [`SNOMEDCT:70600005`](http://snomed.info/id/70600005)
  - [`SNOMEDCT:74189002`](http://snomed.info/id/74189002)
  - [`SNOMEDCT:836276000`](http://snomed.info/id/836276000)
  - [`SNOMEDCT:836277009`](http://snomed.info/id/836277009)
  - `MEDGEN:224769`
  - [`ICD10:C81.2`](https://icd.codes/icd9cm/C81.2)
  - [`ICD10:C81.3`](https://icd.codes/icd9cm/C81.3)
  - [`ICD10:C81.4`](https://icd.codes/icd9cm/C81.4)
  - [`ICD9:201.4`](http://translator.ncats.nih.gov/ICD9_201.4)
  - [`ICD9:201.6`](http://translator.ncats.nih.gov/ICD9_201.6)
  - [`ICD9:201.7`](http://translator.ncats.nih.gov/ICD9_201.7)
  - [`HP:0012189`](http://purl.obolibrary.org/obo/HP_0012189) "Hodgkin lymphoma"
  - `GARD:19593` "Classic Hodgkin lymphoma, lymphocyte-rich type" **(new from GARD)**
    **(NOT emitted — prefix not registered in Biolink Model for `biolink:Disease`)**
  - `GARD:2714` "Hodgkins lymphoma" **(existing identifier, also added by GARD)**
    **(NOT emitted — prefix not registered in Biolink Model for `biolink:Disease`)**
  - `http://id.who.int/icd/entity/352299041`
- Clique with 65 identifiers — typed as `biolink:Disease` — gains 1 new member(s) from GARD:
  - [`MONDO:0005192`](http://purl.obolibrary.org/obo/MONDO_0005192) "exocrine pancreatic carcinoma"
    **(preferred)**
  - [`DOID:1793`](http://purl.obolibrary.org/obo/DOID_1793) "pancreatic cancer"
  - [`DOID:4905`](http://purl.obolibrary.org/obo/DOID_4905) "pancreatic carcinoma"
  - [`OMIM:260350`](http://purl.obolibrary.org/obo/OMIM_260350)
  - [`orphanet:1333`](http://www.orpha.net/ORDO/Orphanet_1333)
  - [`orphanet:217074`](http://www.orpha.net/ORDO/Orphanet_217074)
  - [`EFO:0002618`](http://www.ebi.ac.uk/efo/EFO_0002618) "obsolete_pancreatic carcinoma"
  - [`UMLS:C0030297`](http://identifiers.org/umls/C0030297) "Pancreatic Neoplasm"
  - [`UMLS:C0153458`](http://identifiers.org/umls/C0153458) "malignant neoplasm of head of pancreas"
  - [`UMLS:C0153459`](http://identifiers.org/umls/C0153459) "Malignant neoplasm of body of pancreas"
  - [`UMLS:C0153460`](http://identifiers.org/umls/C0153460) "Malignant neoplasm of tail of pancreas"
  - [`UMLS:C0153463`](http://identifiers.org/umls/C0153463) "Malignant neoplasm of other specified
    sites of pancreas"
  - [`UMLS:C0235974`](http://identifiers.org/umls/C0235974) "Pancreatic carcinoma"
  - [`UMLS:C0346647`](http://identifiers.org/umls/C0346647) "Malignant neoplasm of pancreas"
  - [`UMLS:C1306595`](http://identifiers.org/umls/C1306595) "Primary malignant neoplasm of body of
    pancreas"
  - [`UMLS:C1306596`](http://identifiers.org/umls/C1306596) "Primary malignant neoplasm of tail of
    pancreas"
  - [`UMLS:C1842408`](http://identifiers.org/umls/C1842408) "increased risk of pancreatic cancer"
  - [`UMLS:C5779842`](http://identifiers.org/umls/C5779842) "Pancreatic Acinar Carcinoma"
  - [`UMLS:C5966185`](http://identifiers.org/umls/C5966185) "Exocrine Pancreas Carcinoma"
  - [`MESH:D010190`](http://id.nlm.nih.gov/mesh/D010190) "Pancreatic Neoplasms"
  - [`MEDDRA:10007071`](http://identifiers.org/meddra/10007071)
  - [`MEDDRA:10025762`](http://identifiers.org/meddra/10025762)
  - [`MEDDRA:10025954`](http://identifiers.org/meddra/10025954)
  - [`MEDDRA:10026313`](http://identifiers.org/meddra/10026313)
  - [`MEDDRA:10026318`](http://identifiers.org/meddra/10026318)
  - [`MEDDRA:10026521`](http://identifiers.org/meddra/10026521)
  - [`MEDDRA:10033575`](http://identifiers.org/meddra/10033575)
  - [`MEDDRA:10033576`](http://identifiers.org/meddra/10033576)
  - [`MEDDRA:10033589`](http://identifiers.org/meddra/10033589)
  - [`MEDDRA:10033604`](http://identifiers.org/meddra/10033604)
  - [`MEDDRA:10033609`](http://identifiers.org/meddra/10033609)
  - [`MEDDRA:10033612`](http://identifiers.org/meddra/10033612)
  - [`MEDDRA:10033632`](http://identifiers.org/meddra/10033632)
  - [`MEDDRA:10049099`](http://identifiers.org/meddra/10049099)
  - [`MEDDRA:10050255`](http://identifiers.org/meddra/10050255)
  - [`MEDDRA:10061902`](http://identifiers.org/meddra/10061902)
  - [`NCIT:C194089`](http://purl.obolibrary.org/obo/NCIT_C194089) "Malignant Neoplasm of Head of
    Pancreas"
  - [`NCIT:C194090`](http://purl.obolibrary.org/obo/NCIT_C194090) "Malignant Neoplasm of Body of
    Pancreas"
  - [`NCIT:C194091`](http://purl.obolibrary.org/obo/NCIT_C194091) "Malignant Neoplasm of Tail of
    Pancreas"
  - [`NCIT:C207229`](http://purl.obolibrary.org/obo/NCIT_C207229) "Pancreatic Carcinoma"
  - [`NCIT:C3305`](http://purl.obolibrary.org/obo/NCIT_C3305) "Pancreatic Neoplasm"
  - [`NCIT:C3850`](http://purl.obolibrary.org/obo/NCIT_C3850) "Exocrine Pancreas Carcinoma"
  - [`SNOMEDCT:126859007`](http://snomed.info/id/126859007)
  - [`SNOMEDCT:154475002`](http://snomed.info/id/154475002)
  - [`SNOMEDCT:187791002`](http://snomed.info/id/187791002)
  - [`SNOMEDCT:187792009`](http://snomed.info/id/187792009)
  - [`SNOMEDCT:187796007`](http://snomed.info/id/187796007)
  - [`SNOMEDCT:363418001`](http://snomed.info/id/363418001)
  - [`SNOMEDCT:363419009`](http://snomed.info/id/363419009)
  - [`SNOMEDCT:372142002`](http://snomed.info/id/372142002)
  - [`SNOMEDCT:93715005`](http://snomed.info/id/93715005)
  - [`SNOMEDCT:93823001`](http://snomed.info/id/93823001)
  - [`SNOMEDCT:94082003`](http://snomed.info/id/94082003)
  - `MEDGEN:65917`
  - [`ICD10:C25.0`](https://icd.codes/icd9cm/C25.0)
  - [`ICD10:C25.1`](https://icd.codes/icd9cm/C25.1)
  - [`ICD10:C25.2`](https://icd.codes/icd9cm/C25.2)
  - [`ICD9:157.0`](http://translator.ncats.nih.gov/ICD9_157.0)
  - [`ICD9:157.1`](http://translator.ncats.nih.gov/ICD9_157.1)
  - [`ICD9:157.2`](http://translator.ncats.nih.gov/ICD9_157.2)
  - [`ICD9:157.8`](http://translator.ncats.nih.gov/ICD9_157.8)
  - [`KEGG.DISEASE:05212`](http://identifiers.org/kegg.disease/05212)
  - [`HP:0002894`](http://purl.obolibrary.org/obo/HP_0002894) "Neoplasm of the pancreas"
  - `GARD:27717` "Carcinoma of pancreas" **(new from GARD)**
    **(NOT emitted — prefix not registered in Biolink Model for `biolink:Disease`)**
  - `GARD:9364`
- Clique with 62 identifiers — typed as `biolink:Disease` — gains 1 new member(s) from GARD:
  - [`MONDO:0005380`](http://purl.obolibrary.org/obo/MONDO_0005380) "osteonecrosis" **(preferred)**
  - [`DOID:0080008`](http://purl.obolibrary.org/obo/DOID_0080008) "ischemic bone disease"
  - [`DOID:10159`](http://purl.obolibrary.org/obo/DOID_10159) "osteonecrosis"
  - [`orphanet:399158`](http://www.orpha.net/ORDO/Orphanet_399158)
  - [`EFO:0004259`](http://www.ebi.ac.uk/efo/EFO_0004259) "obsolete_osteonecrosis"
  - [`UMLS:C0003977`](http://identifiers.org/umls/C0003977) "Aseptic necrosis of head and/or neck of
    femur"
  - [`UMLS:C0027543`](http://identifiers.org/umls/C0027543) "Avascular necrosis of bone"
  - [`UMLS:C0029445`](http://identifiers.org/umls/C0029445) "Bone necrosis"
  - [`UMLS:C0085660`](http://identifiers.org/umls/C0085660) "Aseptic necrosis"
  - [`UMLS:C0158442`](http://identifiers.org/umls/C0158442) "Juvenile osteochondrosis of upper
    extremity"
  - [`UMLS:C0158449`](http://identifiers.org/umls/C0158449) "Osteonecrosis of head of humerus"
  - [`UMLS:C0158450`](http://identifiers.org/umls/C0158450) "Osteonecrosis of medial femoral
    condyle"
  - [`UMLS:C0158451`](http://identifiers.org/umls/C0158451) "Osteonecrosis of talus"
  - [`UMLS:C0520474`](http://identifiers.org/umls/C0520474) "Aseptic Necrosis of Bone"
  - [`UMLS:C0745048`](http://identifiers.org/umls/C0745048) "Osteonecrosis of humerus"
  - [`UMLS:C0877326`](http://identifiers.org/umls/C0877326) "Bone infarction"
  - [`MESH:D010020`](http://id.nlm.nih.gov/mesh/D010020) "Osteonecrosis"
  - [`MEDDRA:10003459`](http://identifiers.org/meddra/10003459)
  - [`MEDDRA:10003460`](http://identifiers.org/meddra/10003460)
  - [`MEDDRA:10003461`](http://identifiers.org/meddra/10003461)
  - [`MEDDRA:10003462`](http://identifiers.org/meddra/10003462)
  - [`MEDDRA:10003463`](http://identifiers.org/meddra/10003463)
  - [`MEDDRA:10003464`](http://identifiers.org/meddra/10003464)
  - [`MEDDRA:10003465`](http://identifiers.org/meddra/10003465)
  - [`MEDDRA:10003467`](http://identifiers.org/meddra/10003467)
  - [`MEDDRA:10003861`](http://identifiers.org/meddra/10003861)
  - [`MEDDRA:10005994`](http://identifiers.org/meddra/10005994)
  - [`MEDDRA:10023264`](http://identifiers.org/meddra/10023264)
  - [`MEDDRA:10028855`](http://identifiers.org/meddra/10028855)
  - [`MEDDRA:10031239`](http://identifiers.org/meddra/10031239)
  - [`MEDDRA:10031264`](http://identifiers.org/meddra/10031264)
  - [`MEDDRA:10049824`](http://identifiers.org/meddra/10049824)
  - [`NCIT:C27220`](http://purl.obolibrary.org/obo/NCIT_C27220) "Bone Infarction"
  - [`NCIT:C34404`](http://purl.obolibrary.org/obo/NCIT_C34404) "Aseptic Necrosis of Head and Neck
    of Femur"
  - [`NCIT:C34841`](http://purl.obolibrary.org/obo/NCIT_C34841) "Avascular Necrosis of Bone"
  - [`NCIT:C34880`](http://purl.obolibrary.org/obo/NCIT_C34880) "Bone Necrosis"
  - [`NCIT:C35226`](http://purl.obolibrary.org/obo/NCIT_C35226) "Aseptic Necrosis of Head of
    Humerus"
  - [`NCIT:C35476`](http://purl.obolibrary.org/obo/NCIT_C35476) "Aseptic Necrosis of Bone"
  - [`NCIT:C35517`](http://purl.obolibrary.org/obo/NCIT_C35517) "Avascular Necrosis of Humerus"
  - [`SNOMEDCT:1335749006`](http://snomed.info/id/1335749006)
  - [`SNOMEDCT:156837008`](http://snomed.info/id/156837008)
  - [`SNOMEDCT:203475004`](http://snomed.info/id/203475004)
  - [`SNOMEDCT:203478002`](http://snomed.info/id/203478002)
  - [`SNOMEDCT:240196003`](http://snomed.info/id/240196003)
  - [`SNOMEDCT:268030004`](http://snomed.info/id/268030004)
  - [`SNOMEDCT:29281007`](http://snomed.info/id/29281007)
  - [`SNOMEDCT:398199007`](http://snomed.info/id/398199007)
  - [`SNOMEDCT:62100001`](http://snomed.info/id/62100001)
  - [`SNOMEDCT:72756009`](http://snomed.info/id/72756009)
  - `MEDGEN:45249`
  - [`ICD10:M87`](https://icd.codes/icd9cm/M87)
  - [`ICD10:M87.9`](https://icd.codes/icd9cm/M87.9)
  - [`ICD9:732.3`](http://translator.ncats.nih.gov/ICD9_732.3)
  - [`ICD9:733.41`](http://translator.ncats.nih.gov/ICD9_733.41)
  - [`ICD9:733.42`](http://translator.ncats.nih.gov/ICD9_733.42)
  - [`ICD9:733.43`](http://translator.ncats.nih.gov/ICD9_733.43)
  - [`ICD9:733.44`](http://translator.ncats.nih.gov/ICD9_733.44)
  - [`HP:0010885`](http://purl.obolibrary.org/obo/HP_0010885) "Avascular necrosis"
  - `GARD:21657` "Bone necrosis" **(new from GARD)**
    **(NOT emitted — prefix not registered in Biolink Model for `biolink:Disease`)**
  - `ICD10CM:M87`
  - `http://id.who.int/icd/entity/536467755`
  - `https://icd.who.int/browse10/2019/en#/M87`
