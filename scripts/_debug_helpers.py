'''Helpers for quick bug locating'''
#this script contains helper functions for debugging network models by removing specific components such as interconnectors and storage units.
def remove_interconnectors(n):
    '''helper function for debugging'''
    # remove all interconnectors, local market generators, and non-electricity loads from the network model 'n'. 
    for l in n.links.index[n.links.carrier == 'interconnector']:
        n.remove("Link", l)
    
    for g in n.generators.index[n.generators.carrier == 'local_market']:
        n.remove("Generator", g)
    
    for l in n.loads.index[n.loads.carrier != 'electricity']:
        n.remove("Load", l)


def remove_storage_units(n):
    '''helper function for debugging'''

    for s in n.storage_units.index:
        n.remove("StorageUnit", s)