"""Common functions, etc"""


import os
from itertools import izip_longest


def check_create_dir(directory, info=False):
    """Check to see if directory exists, if not make it.

    Can optionally display message to user.

    Parameters
    ----------
    directory : str
        Name of directory
    info : bool, optional
        If True, prints out message if dir is created.

    Raises
    ------
    RuntimeError
        If file with same name already exists.
    """
    if not os.path.isdir(directory):
        if os.path.isfile(directory):
            raise RuntimeError("Cannot create directory %s, already "
                               "exists as a file object" % directory)
        os.makedirs(directory)
        if info:
            print "Making dir %s" % directory


def grouper(iterable, n, fillvalue=None):
    """Iterate through iterable in groups of size n.
    If < n values available, pad with fillvalue.

    Taken from the itertools cookbook.

    Parameters
    ----------
    iterable : TYPE
        Description
    n : int
        Size of group
    fillvalue : TYPE, optional
        Object to pad group with incase there are fewer than n members.
    """
    args = [iter(iterable)] * n
    return izip_longest(fillvalue=fillvalue, *args)


def frange(start, stop, step=1.0):
    """Generate an iterator to loop over a range of floats.

    Parameters
    ----------
    start : float
        Start value
    stop : float
        End value (inclusive)
    step : float, optional
        Interval size
    """
    i = start
    while i <= stop:
        yield i
        i += step
