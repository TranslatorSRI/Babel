import jsonlines

def assess(compendium,prefix):
    nwrote = 0
    maxwrote = 20
    with jsonlines.open(compendium,'r') as inf:
        for j in inf:
            if len(j['equivalent_identifiers']) == 1:
                if j['id']['identifier'].startswith(prefix):
                    print(j)
                    nwrote += 1
                    if nwrote >= maxwrote:
                        exit()

if __name__ == '__main__':
    assess('../babel_outputs/compendia/Disease.txt','MONDO')