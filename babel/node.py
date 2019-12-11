def create_node(identifiers,node_type):
    node = {
        'equivalent_identifiers': list(identifiers),
        'type': node_type
           }
    #This is where we will normalize, i.e. choose the best id, and add types in accord with BL.
    #Do we talk to BL via a service, or load it up?
    #maybe we should also include provenance information for the node set build.
    return node