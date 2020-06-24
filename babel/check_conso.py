from collections import defaultdict

groups = defaultdict( lambda: defaultdict(lambda:defaultdict(list)))

def printsome(p1,p2,g,n=10000):
    for curie in g[p1]:
        if len(g[p1][curie][p2]) > 1:
            print(curie, g[p1][curie][p2])

with open('../../babel_downloads/meddra_umls_sets.txt','r') as inf:
    for line in inf:
        l = line.split("'")
        c1 = l[1]
        c2 = l[3]
        p1 = c1.split(':')[0]
        p2 = c2.split(':')[0]
        groups[p1][c1][p2].append(c2)
        groups[p2][c2][p1].append(c1)

pg = groups['UMLS']
bads = defaultdict(int)
for curie in pg:
    for prefix in pg[curie]:
        if len(pg[curie][prefix]) > 1:
            bads[prefix] += 1
for prefix,pg in groups.items():
    if prefix != 'UMLS':
        nbad = 0
        print(prefix,len(pg))
        for curie in pg:
            if len(pg[curie]['UMLS']) > 1:
                nbad += 1
        print(f' number of {prefix} with more than one UMLS: {nbad} / {len(pg)} ({nbad/len(pg)})')
        print(f' number of UMLS with more than one {prefix}: {bads[prefix]} / {len(groups["UMLS"])} ({bads[prefix]/len(groups["UMLS"])})')

printsome('UMLS','MESH',groups)

