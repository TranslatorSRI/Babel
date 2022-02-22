from src.babel_utils import make_local_name, pull_via_ftp
import pyoxigraph
from collections import defaultdict
from src.prefixes import MESH

def pull_mesh():
    pull_via_ftp('ftp.nlm.nih.gov', '/online/mesh/rdf', 'mesh.nt.gz', decompress_data=True, outfilename='MESH/mesh.nt')

class Mesh:
    """Load the mesh rdf file for querying"""
    def __init__(self):
        ifname = make_local_name('mesh.nt', subpath='MESH')
        from datetime import datetime as dt
        print('loading mesh.nt')
        start = dt.now()
        self.m= pyoxigraph.SledStore('/tmp/mesh.sled')
        with open(ifname,'rb') as inf:
            self.m.load(inf,'application/n-triples')
        end = dt.now()
        print('loading complete')
        print(f'took {end-start}')
    def get_terms_in_tree(self,top_treenum):
        s=f"""   PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                PREFIX meshv: <http://id.nlm.nih.gov/mesh/vocab#>
                PREFIX mesh: <http://id.nlm.nih.gov/mesh/>

                SELECT DISTINCT ?term
                WHERE {{ ?term meshv:treeNumber ?treenum .
                         ?treenum meshv:parentTreeNumber* mesh:{top_treenum}
                }}
                ORDER BY ?term
        """
        qres = self.m.query(s)
        meshes = []
        for row in list(qres):
            iterm = str(row['term'])
            meshid = iterm[:-1].split('/')[-1]
            meshes.append( f'{MESH}:{meshid}' )
        return meshes
    def get_terms_with_type(self,termtype):
        s=f"""  PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                PREFIX rdfns: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                PREFIX meshv: <http://id.nlm.nih.gov/mesh/vocab#>
                PREFIX mesh: <http://id.nlm.nih.gov/mesh/>

                SELECT DISTINCT ?term
                WHERE {{ ?term rdfns:type meshv:{termtype} }}
                ORDER BY ?term
        """
        qres = self.m.query(s)
        meshes = []
        for row in list(qres):
            iterm = str(row['term'])
            meshid = iterm[:-1].split('/')[-1]
            meshes.append( f'{MESH}:{meshid}' )
        return meshes
    def get_registry(self):
        """Based on stuff like
        <http://id.nlm.nih.gov/mesh/M0391958>	<http://id.nlm.nih.gov/mesh/vocab#registryNumber>	"8A1O1M485B" .
        <http://id.nlm.nih.gov/mesh/D000068877>	<http://id.nlm.nih.gov/mesh/vocab#preferredConcept>	<http://id.nlm.nih.gov/mesh/M0391958> ."""
        s="""   PREFIX meshv: <http://id.nlm.nih.gov/mesh/vocab#>
                PREFIX mesh: <http://id.nlm.nih.gov/mesh/>

                SELECT DISTINCT ?term ?reg
                WHERE {{ ?mthing meshv:registryNumber ?reg . 
                         ?term meshv:preferredConcept ?mthing }}
                ORDER BY ?term
        """
        qres = self.m.query(s)
        res = []
        for row in list(qres):
            iterm = str(row['term'])
            label = str(row['reg'])[1:-1] #strip quotes
            if label == '0':
                #wtf is this dumbness?
                continue
            meshid = f"{MESH}:{iterm[:-1].split('/')[-1]}"
            res.append( (meshid,label) )
        return res
    def print_tree_labels(self):
        s="""   PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                PREFIX meshv: <http://id.nlm.nih.gov/mesh/vocab#>
                PREFIX mesh: <http://id.nlm.nih.gov/mesh/>

                SELECT DISTINCT ?label ?treenum
                WHERE { ?term meshv:treeNumber ?treenum .
                    ?term rdfs:label ?label
                }
                ORDER BY ?treenum
        """
        qres = self.m.query(s)
        with open('mesh_tree_labels', 'w', encoding='utf8') as outf:
            for row in list(qres):
                iterm = str(row['treenum'])
                ilabel = str(row['label'])
                meshid = iterm[:-1].split('/')[-1]
                label = ilabel.strip().split('"')[1]
                outf.write(f'{meshid}\t{label}\n')
    def pull_mesh_labels(self):
        s="""   PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                PREFIX meshv: <http://id.nlm.nih.gov/mesh/vocab#>
                PREFIX mesh: <http://id.nlm.nih.gov/mesh/>

                SELECT DISTINCT ?term ?label
                WHERE { ?term rdfs:label ?label }
                ORDER BY ?term
        """
        ofname = make_local_name('labels', subpath='MESH')
        qres = self.m.query(s)
        with open(ofname, 'w', encoding='utf8') as outf:
            for row in list(qres):
                iterm = str(row['term'])
                ilabel = str(row['label'])
                meshid = iterm[:-1].split('/')[-1]
                label = ilabel.strip().split('"')[1]
                outf.write(f'{MESH}:{meshid}\t{label}\n')

def pull_mesh_labels():
    m = Mesh()
    m.pull_mesh_labels()

def pull_mesh_registry():
    m = Mesh()
    return m.get_registry()

def write_ids(meshmap,outfile,order=['biolink:CellularComponent','biolink:Cell','biolink:AnatomicalEntity'],extra_vocab={}):
    """Write the mesh identifiers from a particular set of hierarchies to an output directory.
    This might be a mixed list of types (for instance anatomy and cell).  Also, the same term
    may appear in multiple trees, perhaps with different types."""
    m = Mesh()
    terms2type=defaultdict(set)
    for treenum,category in meshmap.items():
        mesh_terms = m.get_terms_in_tree(treenum)
        for mt in mesh_terms:
            terms2type[mt].add(category)
    for k,v in extra_vocab.items():
        mesh_terms = m.get_terms_with_type(k)
        for mt in mesh_terms:
            terms2type[mt].add(v)
    with open(outfile, 'w') as idfile:
        for term,typeset in terms2type.items():
            l = list(typeset)
            l.sort(key=lambda k:order.index(k))
            if l[0] == 'EXCLUDE':
                continue
            idfile.write(f'{term}\t{l[0]}\n')



#    ifname = make_local_name('mesh.nt', subpath='MESH')
#    ofname = make_local_name('labels', subpath='MESH')
#    badlines = 0
#    with open(ofname, 'w') as outf, open(ifname,'r') as data:
#        for line in data:
#            if line.startswith('#'):
#                continue
#            triple = line[:-1].strip().split('\t')
#            try:
#                s,v,o = triple
#                if v == '<http://www.w3.org/2000/01/rdf-schema#label>':
#                    meshid = s[:-1].split('/')[-1]
#                    label = o.strip().split('"')[1]
#                    outf.write(f'MESH:{meshid}\t{label}\n')
#            except ValueError:
#                badlines += 1
#    print(f'{badlines} lines were bad')

if __name__ == '__main__':
    mesh = Mesh()
    mesh.print_tree_labels()
    #mesh.pull_mesh_labels()
