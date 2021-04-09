from src.ubergraph import UberGraph
from src.util import Text
from src.babel_utils import write_compendium,glom,get_prefixes

#The BTO and BAMs identifiers promote over-glommed nodes
def build_sets(iri, outf, ignore_list = ['PMID','BTO','BAMS']):
    """Given an IRI create a list of sets.  Each set is a set of equivalent LabeledIDs, and there
    is a set for each subclass of the input iri"""
    uber = UberGraph()
    uberres = uber.get_subclasses_and_xrefs(iri)
    for k,v in uberres.items():
        for x in v:
            if Text.get_curie(x) not in ignore_list:
                outf.write(f'{k[0]}\txref\t{x}\n')

def build_anatomy_relationships(fname):
    with open(fname,'w') as outf:
        build_sets('UBERON:0001062',outf)
        build_sets('GO:0005575',outf)


def build_concordance(infiles):
    """:infiles: a list of files from which to read relationships
        The names of outfiles are derived from the biolink model.   They need to match what's in config.json."""
    dicts = {}
    for infile in infiles:
        newpairs = []
        for infile in infiles:
            with open(infile,'r') as inf:
                for line in inf:
                    x = line.strip().split('\t')
                    newpairs.append( set([x[0], x[2]]))
        glom(dicts,newpairs, unique_prefixes=['UBERON','GO'])
    anat_sets, cell_sets, cc_sets = create_typed_sets(set([frozenset(x) for x in dicts.values()]))
    #Because of the way that we're loading synonyms / labels in write_compendium we end up loading and reloading
    # e.g. MESH.  But I don't know that I care very much.
    write_compendium(anat_sets, 'AnatomicalEntity.txt', 'biolink:AnatomicalEntity', {})
    write_compendium(cell_sets, 'Cell.txt', 'biolink:Cell', {})
    write_compendium(cc_sets, 'CellularComponent.txt', 'biolink:CellularComponent', {})

#def load_anatomy():
#    anatomy_sets,labels = build_sets('UBERON:0001062')
#    cellular_component_sets,labels_b = build_sets('GO:0005575')
#    dicts = {}
#    print('put it all together')
#    glom(dicts, anatomy_sets, unique_prefixes=['UBERON','GO'])
#    glom(dicts, cellular_component_sets, unique_prefixes=['UBERON','GO'])
#    labels.update(labels_b)
#    anat_sets, cell_sets, cc_sets = create_typed_sets(set([frozenset(x) for x in dicts.values()]))
#    write_compendium(anat_sets,'anatomy.txt','biolink:AnatomicalEntity',labels)
#    write_compendium(cell_sets,'cell.txt','biolink:Cell',labels)
#    write_compendium(cc_sets,'cellular_component.txt','biolink:CellularComponent',labels)

def create_typed_sets(eqsets):
    """Given a set of sets of equivalent identifiers, we want to type each one into
    being either a disease or a phenotypic feature.  Or something else, that we may want to
    chuck out here.
    Current rules: If it has an UBERON, it's an anatomy, if it has a CL it's a cell, and if it has GO it's a cellular
    component.  There are things that have none of the above, but it's not clear to me if we want them or not.
    """
    anatomies = set()
    cells = set()
    components=set()
    for equivalent_ids in eqsets:
        #prefixes = set([ Text.get_curie(x) for x in equivalent_ids])
        prefixes = get_prefixes(equivalent_ids)
        if 'GO' in prefixes:
            components.add(equivalent_ids)
        elif 'CL' in prefixes:
            cells.add(equivalent_ids)
        elif 'UBERON' in prefixes or 'BSPO' in prefixes:
            anatomies.add(equivalent_ids)
        else:
            #print(equivalent_ids)
            pass
    return anatomies,cells,components

#def relabel_entities(sets):
#    curie_to_labeledID = {}
#    for s in sets:
#        for si in s:
#            if isinstance(si,LabeledID):
#                curie_to_labeledID[si.identifier] = si
#    for s in sets:
#        for si in s:
#            if si in curie_to_labeledID:
#                s.remove(si)
#                s.add(curie_to_labeledID[si])
#

#if __name__ == '__main__':
#    load_anatomy()
