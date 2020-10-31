# -*- coding: utf-8 -*-
from pyspark.sql import functions as F

__all__ = ['aggregate','dummify']

def aggregate(df, key, pivot=None, **kwargs):
    """
    Function that aggregates spark dataframe to the chosen key and apply a set of spark functions
    to the aggregation.
    Warning : this function requires a version of pyspark >= 2.1.0

    Parameters
    ----------

    df: spark dataframe
    key: list of df columns that are used for aggregating the dataframe
    **kwargs: any aggregate spark functions* is a key argument withvalues a list of variables
    -> available aggregate spark functions :
        - sum,
        - avg,
        - mean,
        - max,
        - min,
        - count,
        - countDistinct,
        - approxCountDistinct,
        - collect_list,
        - collect_set
    -> Custom :
        - count_modal : count_modal = [(col1,modalite1),(col2,modalite2)]

    Returns
    -------
    df: the aggregated spark dataframe

    Exemple :

    > sc,sqlContext = tbx.spark_utils.sparkConnect(4,2,"aggregate",False)


    > df_rows = [Row(a='id1', b='v1', c='small', d=1,e=2,f=0),
                 Row(a='id1', b='v2', c='large', d=9,e=3,f=1),
                 Row(a='id1', b='v3', c='large', d=2,e=10,f=1),
                 Row(a='id2', b='v1', c='small', d=3,e=21,f=0),
                 Row(a='id2', b='v2', c='small', d=3,e=3,f=1),
                 Row(a='id2', b='v4', c='large', d=4,e=2,f=0),
                 Row(a='id3', b='v1', c=None, d=5,e=1,f=1),
                 Row(a='id3', b='v2', c='small', d=6,e=2,f=1),
                 Row(a='id4', b='v1', c='large', d=7,e=None,f=0),
                 Row(a='id4', b='v2', c='enormous', d=6,e=2,f=1),
                 Row(a='id4', b='v3', c='enormous', d=7,e=None,f=0),
                 Row(a='id5', b='v1', c='small', d=1,e=2,f=0),
                 Row(a='id5', b='v2', c='enormous', d=9,e=3,f=1) ]

    >  df.show()
    +---+----+--------+---+----+---+
    |  a|   b|       c|  d|   e|  f|
    +---+----+--------+---+----+---+
    |id1|  v1|   small|  1|   2|  0|
    |id1|  v2|   large|  9|   3|  1|
    |id1|  v3|   large|  2|  10|  1|
    |id2|  v1|   small|  3|  21|  0|
    |id2|  v2|   small|  3|   3|  1|
    |id2|  v4|   large|  4|   2|  0|
    |id3|  v1|    null|  5|   1|  1|
    |id3|  v2|   small|  6|   2|  1|
    |id4|  v1|   large|  7|null|  0|
    |id4|  v2|enormous|  6|   2|  1|
    |id4|  v3|enormous|  7|null|  0|
    |id5|  v1|   small|  1|   2|  0|
    |id5|  v2|enormous|  9|   3|  1|
    |id1|  v1|   small|  1|   2|  0|
    |id1|  v2|   large|  9|   3|  1|
    |id1|  v3|   large|  2|  10|  1|
    |id2|  v1|   small|  3|  21|  0|
    |id2|  v2|   small|  3|   3|  1|
    |id2|  v4|   large|  4|   2|  0|
    |id3|  v1|    null|  5|   1|  1|
    +---+----+--------+---+----+---+
    only showing top 20 rows

    > df = sc.parallelize(df_rows*50).toDF()

    > aggregate(df,["a","b"],mean=["d","f"],sum=["e"],count=["c"]).show()

    +---+----+-------+-----+------+------+
    |  a|   b|c_count|e_sum|d_mean|f_mean|
    +---+----+-------+-----+------+------+
    |id4|  v3|     50| null|   7.0|   0.0|
    |id3|  v2|     50|  100|   6.0|   1.0|
    |id2|  v2|     50|  150|   3.0|   1.0|
    |id4|  v2|     50|  100|   6.0|   1.0|
    |id5|  v1|     50|  100|   1.0|   0.0|
    |id1|  v2|     50|  150|   9.0|   1.0|
    |id5|  v2|     50|  150|   9.0|   1.0|
    |id1|  v3|     50|  500|   2.0|   1.0|
    |id3|  v1|      0|   50|   5.0|   1.0|
    |id2|  v1|     50| 1050|   3.0|   0.0|
    |id2|  v4|     50|  100|   4.0|   0.0|
    |id4|  v1|     50| null|   7.0|   0.0|
    |id1|  v1|     50|  100|   1.0|   0.0|
    +---+----+-------+-----+------+------+

    > aggregate(df,"a","b",mean=["d"],count=["d"]).show()
    +---+----------+---------+----------+---------+----------+---------+----------+---------+
    |  a|v1_d_count|v1_d_mean|v2_d_count|v2_d_mean|v3_d_count|v3_d_mean|v4_d_count|v4_d_mean|
    +---+----------+---------+----------+---------+----------+---------+----------+---------+
    |id3|        50|      5.0|        50|      6.0|      null|     null|      null|     null|
    |id5|        50|      1.0|        50|      9.0|      null|     null|      null|     null|
    |id1|        50|      1.0|        50|      9.0|        50|      2.0|      null|     null|
    |id2|        50|      3.0|        50|      3.0|      null|     null|        50|      4.0|
    |id4|        50|      7.0|        50|      6.0|        50|      7.0|      null|     null|
    +---+----------+---------+----------+---------+----------+---------+----------+---------+
    """
#    TODO : add a checking step to verify if the keywords are valid agregation functions
    standard = ["sum","avg","mean","max","min","count","countDistinct","approxCountDistinct","collect_list","collect_set"]
    exprs = [eval("F."+k+"(df."+x+").alias('"+x+"_"+k+"')") for k,v in kwargs.items() for x in v if k in standard]
    vars_ = ["'"+x+"_"+k+"'" for k,v in kwargs.items() for x in v if k in standard]
    if "count_modal" in kwargs.keys():
        
        exprs += [eval("F.sum(F.when(F.col('"+x+"')=='"+m+"',1).otherwise(0)).alias('"+x+"_count_"+m+"')") for x,m in kwargs["count_modal"]]
        vars_ += ["'"+x+"_count_"+m+"'" for x,m in kwargs["count_modal"]]
    
    if pivot:
        return df.groupBy(*key).pivot(pivot).agg(*exprs),vars_

    return df.groupBy(*key).agg(*exprs),vars_




def dummify(df,key,*varD):
    """
    Function that dummifies colonnes of categorical variables of a spark dataframe.
    Warning : this function requires a version of pyspark >= 2.1.0

    Parameters
    ----------

    df: spark dataframe
    key: list of ids of the spark dataframe
    *varD: list of categorical variables to dummify

    Returns
    -------
    A tuple : (df,vars_cat) where       
        - df: the dummified spark dataframe
        - vars_cat : list of variables created by the dummification

    Exemple :

    > sc,sqlContext = tbx.spark_utils.sparkConnect(4,2,"aggregate",False)


    > df_rows = [Row(a='id1', b='v1', c='small', d=1,e=2,f=0),
                 Row(a='id1', b='v2', c='large', d=9,e=3,f=1),
                 Row(a='id1', b='v3', c='large', d=2,e=10,f=1),
                 Row(a='id2', b='v1', c='small', d=3,e=21,f=0),
                 Row(a='id2', b='v2', c='small', d=3,e=3,f=1),
                 Row(a='id2', b='v4', c='large', d=4,e=2,f=0),
                 Row(a='id3', b='v1', c=None, d=5,e=1,f=1),
                 Row(a='id3', b=None, c='small', d=6,e=2,f=1),
                 Row(a='id4', b='v1', c='large', d=7,e=None,f=0),
                 Row(a='id4', b='v2', c='enormous', d=6,e=2,f=1),
                 Row(a='id4', b='v3', c='enormous', d=7,e=None,f=0),
                 Row(a='id5', b='v1', c='small', d=1,e=2,f=0),
                 Row(a='id5', b='v2', c='enormous', d=9,e=3,f=1) ]

    >  df.show()
    +---+---+--------+---+----+---+
    |  a|  b|       c|  d|   e|  f|
    +---+---+--------+---+----+---+
    |id1| v1|   small|  1|   2|  0|
    |id1| v2|   large|  9|   3|  1|
    |id1| v3|   large|  2|  10|  1|
    |id2| v1|   small|  3|  21|  0|
    |id2| v2|   small|  3|   3|  1|
    |id2| v4|   large|  4|   2|  0|
    |id3| v1|    null|  5|   1|  1|
    |id3| v2|   small|  6|   2|  1|
    |id4| v1|   large|  7|null|  0|
    |id4| v2|enormous|  6|   2|  1|
    |id4| v3|enormous|  7|null|  0|
    |id5| v1|   small|  1|   2|  0|
    |id5| v2|enormous|  9|   3|  1|
    |id1| v1|   small|  1|   2|  0|
    |id1| v2|   large|  9|   3|  1|
    |id1| v3|   large|  2|  10|  1|
    |id2| v1|   small|  3|  21|  0|
    |id2| v2|   small|  3|   3|  1|
    |id2| v4|   large|  4|   2|  0|
    |id3| v1|    null|  5|   1|  1|
    +---+---+--------+---+----+---+
    only showing top 20 rows

    > df = sc.parallelize(df_rows*50).toDF()

    > dummify(df,"a","c","b").show()
    +---+----------+-------+-------+----+----+----+----+
    |  a|c_enormous|c_small|c_large|b_v2|b_v1|b_v3|b_v4|
    +---+----------+-------+-------+----+----+----+----+
    |id1|         0|      1|      0|   0|   1|   0|   0|
    |id1|         0|      0|      1|   1|   0|   0|   0|
    |id1|         0|      0|      1|   0|   0|   1|   0|
    |id2|         0|      1|      0|   0|   1|   0|   0|
    |id2|         0|      1|      0|   1|   0|   0|   0|
    |id2|         0|      0|      1|   0|   0|   0|   1|
    |id3|         0|      0|      0|   0|   1|   0|   0|
    |id3|         0|      1|      0|   1|   0|   0|   0|
    |id4|         0|      0|      1|   0|   1|   0|   0|
    |id4|         1|      0|      0|   1|   0|   0|   0|
    |id4|         1|      0|      0|   0|   0|   1|   0|
    |id5|         0|      1|      0|   0|   1|   0|   0|
    |id5|         1|      0|      0|   1|   0|   0|   0|
    |id1|         0|      1|      0|   0|   1|   0|   0|
    |id1|         0|      0|      1|   1|   0|   0|   0|
    |id1|         0|      0|      1|   0|   0|   1|   0|
    |id2|         0|      1|      0|   0|   1|   0|   0|
    |id2|         0|      1|      0|   1|   0|   0|   0|
    |id2|         0|      0|      1|   0|   0|   0|   1|
    |id3|         0|      0|      0|   0|   1|   0|   0|
    +---+----------+-------+-------+----+----+----+----+
    only showing top 20 rows

   """
    if isinstance(key,str) or isinstance(key,unicode):
        key=[key]
        
    exprs = key
    vars_cat = []
    for var in varD:
        categories = df.select(var).distinct().collect()
        categories = map(lambda c: c[0],categories)
        categories = filter(lambda x: x is not None,categories)
        exprs+=[F.when(F.col(var) == category, 1).otherwise(0).alias(str(var)+"_"+str(category)) for category in categories]
        vars_cat+=[str(var)+"_"+str(category) for category in categories]
    return df.select(*exprs),vars_cat
