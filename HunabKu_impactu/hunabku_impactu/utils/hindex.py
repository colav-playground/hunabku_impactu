



def hindex(citation_list):
    ''' Calculates the h index of a list of citations.
    Parameters
    ----------
    citation_list: list
        List of citations.
        
    Returns
    -------
    int
        The h index of the list of citations.
    '''
    return sum(x >= i + 1 for i, x in enumerate(sorted(list(citation_list), reverse=True)))

