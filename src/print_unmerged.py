import jsonlines

def assess_singles(compendium,prefix):
    nwrote = 0
    maxwrote = 100
    with jsonlines.open(compendium,'r') as inf:
        for j in inf:
            if len(j['equivalent_identifiers']) == 1:
                if j['id']['identifier'].startswith(prefix):
                    print(j)
                    nwrote += 1
                    if nwrote >= maxwrote:
                        exit()

def assess_big(compendium,mx=100):
    nwrote = 0
    maxwrote = 20
    with jsonlines.open(compendium,'r') as inf:
        for j in inf:
            if len(j['equivalent_identifiers']) > mx:
                print(j)
                nwrote += 1
                if nwrote >= maxwrote:
                    exit()


if __name__ == '__main__':
    #assess_singles('../babel_outputs/compendia/PhenotypicFeature.txt','HP')
    assess_big('../babel_outputs/compendia/MolecularActivity.txt',5)

