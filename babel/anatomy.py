from babel.ubergraph import UberGraph
from src.LabeledID import LabeledID
from src.util import Text
from babel.babel_utils import write_compendium,glom,get_prefixes

def build_sets(iri, ignore_list = ['PMID']):
    """Given an IRI create a list of sets.  Each set is a set of equivalent LabeledIDs, and there
    is a set for each subclass of the input iri"""
    uber = UberGraph()
    uberres = uber.get_subclasses_and_xrefs(iri)
    results = []
    for k,v in uberres.items():
        #if k[1] is not None and k[1].startswith('obsolete'):
        #    continue
        dbx = set([ x for x in v if not Text.get_curie(x) in ignore_list ])
        dbx.add(LabeledID(identifier=k[0],label=k[1]))
        results.append(dbx)
    return results

def load_anatomy():
    anatomy_sets = build_sets('UBERON:0001062')
    cellular_component_sets = build_sets('GO:0005575')
    #There can be some nastiness where we need to glom together different entities that came back
    # from this call, so we need to run a glom.
    #There's actually a special problem that happens in this case.  Because A) the subclass crosses
    # ontologies from uberon to cl and go, and because xrefs are strings, not iris, it can
    # happen that the same entity shows up as a subject with a label as well as a curie string
    # with no label.  And glom, as nice as it is, isn't smart enough to figure that out.
    # Sooo, we gotta clean that up.
    # Actually, I think we should probably just keep labels separate until output, but that's
    # a change for the future.
    relabel_entities(anatomy_sets)
    relabel_entities(cellular_component_sets)
    dicts = {}
    print('put it all together')
    glom(dicts, anatomy_sets)
    glom(dicts, cellular_component_sets)
    anat_sets, cell_sets, cc_sets = create_typed_sets(set([frozenset(x) for x in dicts.values()]))
    write_compendium(anat_sets,'anatomy.txt','anatomical_entity')
    write_compendium(cell_sets,'cell.txt','cell')
    write_compendium(cc_sets,'cellular_component.txt','cellular_component')

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
            print(equivalent_ids)
    return anatomies,cells,components

def relabel_entities(sets):
    curie_to_labeledID = {}
    for s in sets:
        for si in s:
            if isinstance(si,LabeledID):
                curie_to_labeledID[si.identifier] = si
    for s in sets:
        for si in s:
            if si in curie_to_labeledID:
                s.remove(si)
                s.add(curie_to_labeledID[si])


if __name__ == '__main__':
    load_anatomy()
