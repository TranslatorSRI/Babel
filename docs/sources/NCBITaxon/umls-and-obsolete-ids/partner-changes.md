# NCBITaxon partner changes

- Before: main at f6fbc10b
- After: this branch (e4273f40)

| prefix | what happened to its NCBITaxon partner | identifiers |
|---|---|---|
| MESH | gained an NCBITaxon | 53 |
| MESH | obsolete partner removed | 1,238 |
| MESH | obsolete partner replaced by current | 672 |
| MESH | same partner | 68,689 |
| UMLS | current partner lost (regression) | 54 |
| UMLS | current partner replaced (regression) | 27 |
| UMLS | gained an NCBITaxon | 712,491 |
| UMLS | obsolete partner removed | 543 |
| UMLS | obsolete partner replaced by current | 712 |
| UMLS | same partner | 100,950 |

A random sample of 10 of the 81 regressions (all of them are in `partner-changes-regressions.csv`):

| identifier | before partner | after partner | before partner is now with |
|---|---|---|---|
| UMLS:C5434195 Ceratocystis pycnanthiformis | NCBITaxon:1580865 Huntiella pycnanthi |   | NCBITaxon:1580865; MESH:C000665962; UMLS:C3989250 |
| UMLS:C5434451 Hermatomyces thailandica | NCBITaxon:1837981 Hermatomyces thailandicus |   | NCBITaxon:1837981; MESH:C000693254; UMLS:C4381940 |
| UMLS:C0319623 Geotrichum galactomycetum | NCBITaxon:1173061 Geotrichum candidum | NCBITaxon:27317 Geotrichum galactomycetum | NCBITaxon:1173061; MESH:C000682231; UMLS:C4049071 |
| UMLS:C5227205 Staphylococcus saprophyticus subsp. bovis | NCBITaxon:29385 Staphylococcus saprophyticus |   | NCBITaxon:29385; MESH:C000638665; UMLS:C0318112 |
| UMLS:C5434710 Sporothrix guttiliformis | NCBITaxon:1892480 Sporothrix guttuliformis |   | NCBITaxon:1892480; MESH:C000702560; UMLS:C4420948 |
| UMLS:C5434584 Teratosphaeria tinara | NCBITaxon:665071 Teratosphaeria tinarooa |   | NCBITaxon:665071; MESH:C000683320; UMLS:C2994020 |
| UMLS:C5434345 Colletotrichum caudasporum | NCBITaxon:1349655 Colletotrichum caudisporum |   | NCBITaxon:1349655; MESH:C000668125; UMLS:C3747940 |
| UMLS:C5401014 Rhizophagus prolifer | NCBITaxon:2650738 Rhizophagus prolifer |   | NCBITaxon:2650738; MESH:C000701329; UMLS:C5259456 |
| UMLS:C5434573 Diaporthe thunbergii | NCBITaxon:1214579 Diaporthe thunbergiae |   | NCBITaxon:1214579; MESH:C000683071; UMLS:C3721495 |
| UMLS:C5434110 Acanthostigma patagonica | NCBITaxon:2719178 Camporesiomyces patagonicus |   | NCBITaxon:2719178; MESH:C000659300; UMLS:C3392649 |
