import jsonlines

def filter_compendium(inputcompendium,outputcompendium):
    with jsonlines.open(inputcompendium,'r') as inf, jsonlines.open(outputcompendium,'w') as outf:
        for j in inf:
            if len(j['equivalent_identifiers']) == 1:
                continue
            outf.write(j)