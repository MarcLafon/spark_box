# -*- coding: utf-8 -*-

from time import time
import numpy as np
from functools import wraps

__all__ = ['timed','count_items','add_list','add_dict']


def timed(f):
    """
    Decorateur temps d'execution
    """
    @wraps(f)
    def wrapper(*args, **kwds):
        start = time()
        result = f(*args, **kwds)
        elapsed = time() - start
        print("%s took %.2fs to finish" % (f.__name__, elapsed))
        return result
    return wrapper


toUpper = lambda x: x.upper() if isinstance(x,str) or isinstance(x,unicode) else x

def count_items(mylist):

    """
    Compte le nombre d'occurences par modalités non null d'une liste

    Parameters
    ----------
    mylist : liste

    Returns
    -------
    Dictionnaire dont les clés sont les modalités de la liste et les valeurs le nombre d'occurences

    Exemple :

    > mylist = ["a","a","a","b",1,1,"c"]
    > count_items(mylist)
    Out[]: {1: 2, 'A': 0, 'B': 0, 'C': 0}

    """
    return {i : mylist.count(i) for i in set(map(toUpper,mylist)) if (i is not None)and(i is not np.NaN)  }

def add_list(l1,l2):
    '''
    Fonction concatenant deux listes, deux éléments en une liste


    Parameters
    ----------
    l1 : liste ou élément
    l2 : liste ou élément

    Returns
    -------
    liste

    Exemples :

    > l1=2

    > l2=[1,4]

    > add_list(l1,l2)
    Out[]: [1,4,2]


    '''
    if isinstance(l1,list):
        if isinstance(l2,list):
            return l1+l2
        else:
            return l1+[l2]
    else:
        if isinstance(l2,list):
            return [l1]+l2
        else:
            return [l1]+[l2]


def add_dict(d1,d2,mode="list"):
    '''
    Fonction concatenant deux dictionnaires, en concatenant les valeurs d'une même clé dans un dictionnaire ou une liste


    Parameters
    ----------
    l1 : dictionnaire
    l2 : dictionnaire

    Returns
    -------
    dictionnaire

    Exemples :

    > d2={"k1":2,"k2":"b"}

    > d1={"k1":1,"k2":"a"}

    > add_dict(d1,d2)
    Out[]: {'k1': [1, 2], 'k2': ['a', 'b']}

    > add_dict(d1,d2,mode="plus")
    Out[]: {'k1': 3, 'k2': 'ab'}

    '''
    if mode=="list":
        return({key:add_list(d1[key],d2[key]) for key in d1.keys()})

    if mode=="np.ndarray":
        raise NotImplementedError
#        return({key:add_array(d1[key],d2[key]) for key in d1.keys()})

    return({key:d1[key]+d2[key] for key in d1.keys()})
