# -*- coding: utf-8 -*-
from toolbox.utils import *
from pyspark.sql.dataframe import DataFrame as sparkDataFrame
import numpy as np
import pandas as pd
from scipy.stats import f
from pyspark.mllib.stat import Statistics
from pyspark.mllib.linalg import DenseMatrix
from itertools import chain
from tqdm import tqdm
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window
import sys

__all__ = ['multi_anova',
           'reportAnovaCsv',
           'multi_Chisq',
           'reportChisqCsv',
           'twoSampleKSTest',
           'reportTwoSampleKSTestCsv']

##################
##################
## Common
##################
##################

def __setkey__(x,pivot,*args):
    """
    Usage interne. Set la clé d'une row à la valeur de la variable pivot
    """
    return(x[pivot],{v:x[v] for v in args})

def __reducer__(x,y,**kwargs):
    """
    Usage interne. Reducer de Row de la forme (k,dict)
    """
    return  add_dict(x,y,**kwargs)

def __distance__(x1,x2):
    """
    Calculate the distance between two float

    Parameters
    ----------
    x1 : float, first number
    x2 : float, second number

    Returns
    -------
    float, distance between x1 and x2
    """
    return abs(x1-x2)

##################
##################
## Anova :
##################
##################

def __anovametrics__(x):
    """
    Usage interne. Calcul des metrics pour l'anova
    """
    return (x[0],len(x[1][list(x[1].keys())[0]]),
            {var: {'mui'   : np.nanmean( np.array( values, dtype = np.float ) ),

                   'Si'    : np.nansum( np.power( np.array(values,dtype=np.float) - np.nanmean( np.array(values,dtype=np.float)) ,2) ),

                   'Sum'   : np.nansum( np.array( values , dtype = np.float ) ),

                   'ni'    : np.count_nonzero(~np.isnan( np.array( values , dtype = np.float ) ) )

                   } for var,values in x[1].items()
            })


def _getfisher_(res,var):
    """
    Usage interne. Retourne les pValues et la stats de fisher
    """
    n =  np.nansum(np.array([x[2][var]["ni"] for x in res],dtype=np.float))

    k =  np.count_nonzero(np.array([x[2][var]["ni"] for x in res],dtype=np.float))

    mu_var = (1.0/n)*np.nansum([ x[2][var]["Sum"] for x in res])

    SR = np.nansum(np.array([x[2][var]["Si"] for x in res],dtype=np.float))
    SA = np.nansum(np.array([ x[2][var]["ni"] * np.power( x[2][var]["mui"]-mu_var , 2 ) for x in res],dtype=np.float))

    fisher = (SA/(k-1))/(SR/(n-k))

    pValue = f.sf(fisher,k-1,n-k)

    return fisher,pValue

def _getMeans_(res,var):
    """
    Usage interne. Retourne les moyennes par variables par facteur
    """
    return {x[0]+"_moy":x[2][var]["mui"] for x in res}

def _getCounts_(res,var):
    """
    Usage interne. Retourne le nombre d'individus renseignés pour chaque variables par facteur
    """
    return {x[0]+"_count":x[2][var]["ni"] for x in res}


def _formatAnovaResults_(res):
    """
    Usage interne. Mise en forme des resultats de l'anova
    """
    stats = {var : (_getfisher_(res,var)[0],_getfisher_(res,var)[1],_getMeans_(res,var),_getCounts_(res,var)) for var in res[0][2].keys()}
    return stats

@timed
def multi_anova(rdd,pivot,*args):
    '''
    Fonction permettant d'effectue plusieurs one-way Anova simultanément


    Parameters
    ----------
    rdd : rdd of Rows (important)
    pivot : variable définissant les facteurs
    args : liste de variables quantitatives

    Returns
    -------
    res : dictionnaire dont les clès sont les variables quantitatives
          et les valeurs de la statistique de test, la pValue, les moyennes par facteurs et le nombre de lignes renseignées par facteurs

    '''
    if isinstance(rdd,sparkDataFrame):
        rdd=rdd.rdd
    res = rdd.map(lambda x: __setkey__(x,pivot,*args))\
              .reduceByKey(lambda x,y:__reducer__(x,y))\
              .map(__anovametrics__)\
              .collect()
    return _formatAnovaResults_(res)

def reportAnovaCsv(anova_res,vs,filename,threshold=0.05):
    '''
    Fonction permettant d'exporter les résultats de la fonction multi_anova au format csv


    Parameters
    ----------
    anova_res : resultat de la fonction multi_anova
    vs : nom de la variable définissant les facteurs
    filename : chemin vers le fichier csv d'export
    threshold : seuil permettant de conserver uniquement les variables ayant un effet significatif,
                défaut = 0.05 (threshold = 1 pour tout conserver)

    Returns
    -------
    None

    '''
    with open(filename,"wb") as log:
        vs_moys_head = anova_res.values()[0][2].keys()
        vs_count_head = anova_res.values()[0][3].keys()
        head="ARM;Variable;Test;pValue;statistic"
        for s in vs_moys_head+vs_count_head:
            head +=";"+s
        tqdm.write(head,log)
        for var in tqdm(iter(anova_res.keys())):
            statistic = anova_res[var][0]
            pValue    = anova_res[var][1]
            moys      = anova_res[var][2].values()
            counts    = anova_res[var][3].values()
            if pValue <threshold:
                line = "%s;%s;anova;%1.2E;%1.2f"%(vs,var,pValue,statistic)
                for v in moys:
                    if v<0.005:
                        line+=";%1.2E"%v
                    else:
                        line+=";%1.2f"%v
                for v in counts:
                    line+=";%d"%int(v)
                tqdm.write(line,log)

##################
##################
## Chisq :
##################
##################



def __counter__(x):
    """
    Usage interne. Compte pour chaque facteur le nombre d'occurences par modalités pour chaque variable
    """
    return (x[0],{var: count_items(values) for var,values in x[1].items()})

#def __getValues__(res,var):
#    return list(chain(*[x[1][var].values() for x in res]))


def __getNbMod__(x,var,mod):
    """
    Usage interne. Retourne le nombre d'occurences d'une modalité d'une variable dans un facteur
    """
    try:
        return x[var][mod]
    except KeyError:
        return 0

def __getMods__(res,var):
    """
    Usage interne. Retourne l'ensemble des modalités d'une variable
    """
    return set(chain(*[x[1][var].keys() for x in res]))

def __getcrosstab__(res,var):
    """
    Usage interne. Fait un crosstab
    """
    return pd.DataFrame({e: {mod: __getNbMod__(x,var,mod) for mod in __getMods__(res,var) } for e,x in res})


def __getStats__(res,var):
    """
    Usage interne. Calcul les stats du Chi2 pour une variable donnée
    """
    df = __getcrosstab__(res,var)
    values = df.values.transpose().flatten()
    matrix = DenseMatrix(df.shape[0],df.shape[1],values)
    try:
        chi2 = Statistics.chiSqTest(matrix)
        return chi2.statistic, chi2.pValue, matrix
    except Exception as e:
        return np.NaN, 0.0, matrix

def __formatChisqResults__(res):
    """
    Usage interne. Mise en forme des resultats du Chisq
    """
    stats = {var : (__getStats__(res,var)[0],
                    __getStats__(res,var)[1],
                    __getStats__(res,var)[2])  for var in res[0][1].keys()}
    return stats

@timed
def multi_Chisq(rdd,pivot,*args):
    '''
    Fonction permettant d'effectue plusieurs tests d'indépendances du Chi2


    Parameters
    ----------
    rdd : rdd of Rows (important)
    pivot : variable définissant les facteurs
    args : liste de variables qualitatives

    Returns
    -------
    res : dictionnaire dont les clès sont les variables qualitatives
          et les valeurs de la statistique de test, la pValue

    '''

    if isinstance(rdd,sparkDataFrame):
        rdd=rdd.rdd
    res = rdd.map(lambda x:__setkey__(x,pivot,*args))\
              .reduceByKey(lambda x,y:__reducer__(x,y))\
              .map(__counter__).collect()

    return __formatChisqResults__(res)


def reportChisqCsv(chisq_res,vs,filename,threshold=0.05):
    '''
    Fonction permettant d'exporter les résultats de la fonction multi_Chisq au format csv


    Parameters
    ----------
    chisq_res : resultat de la fonction multi_Chisq
    vs : nom de la variable définissant les facteurs
    filename : chemin vers le fichier csv d'export
    threshold : seuil permettant de conserver uniquement les variables ayant un effet significatif,
                défaut = 0.05 (threshold = 1 pour tout conserver)

    Returns
    -------
    None

    '''
    with open(filename,"wb") as log:
        head="ARM;Variable;Test;pValue;statistic"
        tqdm.write(head,log)
        for var in tqdm(iter(chisq_res.keys())):
            statistic = chisq_res[var][0]
            pValue    = chisq_res[var][1]
            matrix    = chisq_res[var][2]
            if pValue <threshold:
                line = "%s;%s;Chisq;%1.2E;%1.2f"%(vs,var,pValue,statistic)
                tqdm.write(line,log)

########################
########################
## Kolmogorov-Smirnov
########################
########################

def __calcD__(df1, df2, col1, col2):
    """
    Calculate the D value which represents the max difference of "theorical cumulative distribution" between the two variables

    Parameters
    ----------
    df1 : sqlDataframe, first dataframe, contains the variable col1 and the TCD variable
    df2 : sqlDataframe, second dataframe, contains the variable col2 and the TCD variable
    col1 : String, name of the column variable in df1
    col2 : String, name ot the column variable in df2

    Returns
    -------
    D_max : float, the max difference of "theorical cumulative distribution" between col1 and col2
            return -1.0 if we can't calculate TCD

    """
    windowSpec = Window.orderBy(df1[col1])
    df1 = df1.withColumn("lag1", F.lag(df1[col1]).over(windowSpec))
    windowSpec = Window.orderBy(df2[col2])
    df2 = df2.withColumn("lag2", F.lag(df2[col2]).over(windowSpec))

    df1_join = df1.join(df2, (df2[col2]<=df1[col1])\
                      & (df2[col2]>df1.lag1), how='inner')
    df1_join = df1_join.withColumn("D", F.udf(__distance__, T.FloatType())(df1_join.TCD_1, df1_join.TCD_2))

    df2_join = df2.join(df1, (df1[col1]<=df2[col2])\
                      & (df1[col1]>df2.lag2), how='inner')
    df2_join = df2_join.withColumn("D", F.udf(__distance__, T.FloatType())(df2_join.TCD_2, df2_join.TCD_1))

    D_max1 = df1_join.agg({"D":"max"}).collect()[0][0]
    D_max2 = df2_join.agg({"D":"max"}).collect()[0][0]
    if(D_max1 is None and D_max2 is None):
        return -1.0
    if(D_max1 >= D_max2 and D_max1 is not None):
        return D_max1
    if(D_max2 >= D_max1 and D_max2 is not None):
        return D_max2
    return -1.0

def __calcTCD__(vect, c, name_tcd):
    """
    Calculate the "therorical cumulative distribution" of c in vect

    Parameters
    ----------
    vect : sqlDataframe, dataframe with only one column named c
    c : String, name of the variable
    name_tcd : String, the name given to the new variable TCD calculated

    Returns
    -------
    vect : sqlDataframe, the same as the beginning with TCD variable added
    """
    windowSpec = Window.orderBy(vect[c])\
                       .rangeBetween(-sys.maxsize, 0)
    vect = vect.withColumn(name_tcd, F.sum(vect['1_on_n']).over(windowSpec)).drop('1_on_n').persist()
    vect.count()
    return vect

def __addtwoSampleKSTestResults__(res,c1,c2,alpha,threshold,D):
    """
    Add in res list the result of the test with specific variables

    Parameters
    ----------
    res : list, list of results for the statistic test
    c1 : String, name of the first variable
    c2 : String, name of the second variable
    alpha : float, level of acceptation (p-value)
    threshold : float, it depends of the p-value and number of samples in the dataframes
    D : the max difference of "theorical cumulative distribution"

    Returns
    -------
    res : list, list of result for each test
    """
    if(D==-1.0 or D > threshold):
        null_hypothesis = False
    else:
        null_hypothesis = True
    res.append([c1, c2, alpha, threshold, D,  null_hypothesis])
    return res

def __twoSampleKSTestSameDataFrame__(df, cols, alpha):
    """
    This function is called by twoSampleKSTest when the variables are in the same dataframe.

    Parameters
    ----------
    df : sqlDataframe, the dataframe where we want to compute the two sample K-S test
    cols : list, names of variables in df
    alpha : float, level of acceptation (p-value)

    Returns
    -------
    res : list, list of result for each test
    """
    n = df.count()
    c_alpha = np.sqrt((-1./2.)*np.log(alpha/2.))
    threshold = c_alpha*np.sqrt((n+n)*1./(n*n))
    df = df.withColumn("1_on_n", F.lit(1./n).cast(T.FloatType()))
    res = []
    for i in range(len(cols)):
        for j in range(i,len(cols)):
            c1 = cols[i]
            c2 = cols[j]
            if(c1==c2):
                continue
            vect1 = __calcTCD__(df.select(c1,"1_on_n"),c1, "TCD_1")
            vect2 = __calcTCD__(df.select(c2,"1_on_n"),c2, "TCD_2")
            D_max = __calcD__(vect1,vect2,c1,c2)
            res = __addtwoSampleKSTestResults__(res, c1, c2, alpha, threshold, D_max)
    return res

def __twoSampleKSTestDifferentDataFrame__(df1, cols1, df2, cols2, alpha):
    """
    This function is called by twoSampleKSTest when the variables are in different dataframes.

    Parameters
    ----------
    df1 : sqlDataframe, the first dataframe where we want to compute the two sample K-S test
    cols1 : list, names of variables in df1
    df2 : sqlDataframe, the second dataframe where we want to compute the two sample K-S test
    cols2 : list, names of variables in df2
    alpha : float, level of acceptation (p-value)

    Returns
    -------
    res : list, list of result for each test.
    """
    n1 = df1.count()
    n2 = df2.count()
    c_alpha = np.sqrt((-1./2.)*np.log(alpha/2.))
    threshold = c_alpha*np.sqrt((n1+n2)*1./(n1*n2))
    df1 = df1.withColumn("1_on_n", F.lit(1./n1).cast(T.FloatType()))
    df2 = df2.withColumn("1_on_n", F.lit(1./n2).cast(T.FloatType()))
    res = []
    for c1 in cols1:
        for c2 in cols2:
            vect1 = __calcTCD__(df1.select(c1,"1_on_n"),c1, "TCD_1")
            vect2 = __calcTCD__(df2.select(c2,"1_on_n"),c2, "TCD_2")
            if(n1 < n2):
                D_max = __calcD__(vect1,vect2,c1,c2)
            else:
                D_max = __calcD__(vect2,vect1,c2,c1)
            res = __addtwoSampleKSTestResults__(res, c1, c2, alpha, threshold, D_max)
    return res

def twoSampleKSTest(df1, cols1, df2=None, cols2=None, alpha=0.05):
    """
    Compute de two sample KS test statistic between different variable in same or two different dataframes
    col1 should be a list and contains 2 elements minimum if col2 is None. If df2 is defined col2 should be too.
    if df2 and cols2 are None the test is calculated in the same dataframe.

    Parameters
    ----------
    df1 : sqlDataframe, the first dataframe where we want to compute the two sample K-S test
    cols1 : list, names of variables in df1
    df2 : sqlDataframe, (default=None) the second dataframe where we want to compute the two sample K-S test
    cols2 : list, (default=None) names of variables in df2
    alpha : float, (default 0.05) level of acceptation (p-value)

    Returns
    -------
    res : list, list of result for each test.
          scheme :
         [1st elem=name variable1 ,
          2nd elem=name variable2 ,
          3d elem=p-value ,
          4th elem=threshold ,
          5th elem=D ,
          6th elem=boolean (True=null hyphotesis accepted, False=null hypothesis rejected the variables don't come from the same distribution)]
    """
    assert (cols2 is None and len(cols1) > 1) or (cols2 is not None and isinstance(cols1, list)) or (df2 is not None and cols2 is None),\
           "col1 should be a list and contains 2 elements minimum if col2 is None. If df2 is defined col2 should be too."
    if(df2 is None):
        return __twoSampleKSTestSameDataFrame__(df1, cols1, alpha)
    else:
        return __twoSampleKSTestDifferentDataFrame__(df1, cols1, df2, cols2, alpha)

def reportTwoSampleKSTestCsv(twoSampleKSTest_result,filename):
    with open(filename,"wb") as log:
        head="Variable1;Variable2;pValue;Threshold;D;Result"
        tqdm.write(head,log)
        for result in tqdm(iter(twoSampleKSTest_result)):
            Variable1 = result[0]
            Variable2 = result[1]
            pValue = result[2]
            Threshold = result[3]
            D =result[4]
            if(result[5]==False):
                Result = "Null hypothesis is rejected at level %s, %s and %s come from differents distributions."%(pValue,Variable1,Variable2)
            else:
                Result = "Null hypothesis is accepted at level %s. %s and %s come from the same distribution."%(pValue,Variable1,Variable2)
            line = "%s;%s;%1.2f;%1.2f;%1.2f;%s"%(Variable1,Variable2,pValue,Threshold,D,Result)
            tqdm.write(line,log)
