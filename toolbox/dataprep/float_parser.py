# -*- coding: utf-8 -*-

from pyspark.sql.types import *
from pyspark.sql.functions import udf
from datetime import datetime
import numpy as np
import re
from pyspark.sql import functions as F

#TODO: parsing de float du type '123,456', '123.456', '1E-3', '1.333e-3' etc..
SAMPLING_FRACTION = 0.2
PROBA_THRESHOLD = 0.51
FLOAT_FORMAT = ['\d+[.,]\d+', '\d+e[+-]\d+']

def parse_float(df, columns=None, use_double=False, sampling_fraction=None, proba_threshold=None, new_col=False, error_column=False):
    udf_parsing_error = F.udf(__parsing_error__, IntegerType())
    if use_double:
        udf_parse_float = F.udf(__parse_float__, DoubleType())
    else:
        udf_parse_float = F.udf(__parse_float__, FloatType())

    if (columns is None):
        columns = [f.name for f in df.schema.fields if f.dataType == StringType()]
    else:
        columns = [f.name for f in df.schema.fields if (f.dataType == StringType())&(f.name in columns)]
    if (sampling_fraction is None):
        sampling_fraction = SAMPLING_FRACTION
    elif (sampling_fraction==1):
        sampling_fraction=0.99
    if (proba_threshold is None):
        proba_threshold = PROBA_THRESHOLD
    df_sample = df.sample(False, sampling_fraction, 42)
    for col in columns:
        sample = [r[col] for r in df.select(col).collect() if r[col]]
        is_float_col = __is_float__(sample, proba_threshold)
        if is_float_col:
            if error_column:
                df = df.withColumn(col+'_parsed', udf_parse_float(col))
                df = df.withColumn(col+'_err', udf_parsing_error(col,col+'_parsed'))
                if new_col:
                    print("Column %s has been parsed. The new parsed column is %s_parsed and the error column is %s_err."%(col, col, col))
                else:
                    df = df.drop(col+'_parsed')
                    df = df.withColumn(col, udf_parse_float(col))
                    print("Column %s has been parsed. The error column is available as %s_err."%(col, col))
            else:
                if new_col:
                    df = df.withColumn(col+'_parsed', udf_parse_float(col))
                    print("Column %s has been parsed. The new parsed column is %s_parsed."%(col, col))
                else:
                    df = df.withColumn(col, udf_parse_float(col))
                    print("Column %s has been parsed."%(col))
        else:
            pass
    return df

def __get_sample__(df, col, sampling_fraction):
    """
    For a given spark dataframe, a column of the dataframe and a sampling fraction between 0 and 1, the function return a set of element of df[col] of
    length len(df)*sampling_fraction
    """
    if sampling_fraction==None:
        sampling_fraction == SAMPLING_FRACTION
    elif sampling_fraction==1:
        sampling_fraction=0.99 #Weird but there is an issue when using pyspark sample function with fraction=1
    return [r[col] for r in df.select(col).collect() if r[col]]


def __match_regex__(s, reg_expr):
    """
    parameters:
            s: string
            reg_expr: date format ()
    return:
            boolean telling if the string s matches the regex pattern reg_expr
    """
    if not (s is None):
        if (re.match(reg_expr, s)):
            return True
        else:
            return False
    else:
        return False

def __is_float__(sample, proba_threshold): #TODO a tester
    #Return True is at least proba_threshold of the sample match one of the FLOAT_FORMAT available.
    nb_float = 0
    for s in sample:
        b = False
        cpt = 0
        while (not b) and (cpt<len(FLOAT_FORMAT)):
            #print(__match_regex__(s, FLOAT_FORMAT[cpt]))
            b = __match_regex__(s, FLOAT_FORMAT[cpt])
            #print(s, FLOAT_FORMAT[cpt], b)
            cpt += 1
        if b:
            nb_float += 1
    if len(sample)>0:
        return (float(nb_float)/len(sample))>proba_threshold
    else:
        return None

def __parse_float__(x):
    try:
        return float(x.replace(',', '.'))
    except ValueError:
        return None

def __parsing_error__(x_g, x_g_parsed):
    if (x_g is None) and (x_g_parsed is None):
        return 0
    elif (x_g is None) and (not (x_g_parsed is None)):
        return 1
    elif (not (x_g is None)) and (x_g_parsed is None):
        return 1
    else:
        return 0

