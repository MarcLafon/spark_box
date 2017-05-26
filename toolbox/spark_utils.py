# -*- coding: utf-8 -*-


from pyspark import SparkConf,SparkContext
from pyspark.sql import SQLContext
import sys

__all__ = ['sparkClose','sparkConnect']

def sparkClose():
    '''
    Fonction permettant de se fermer la connection au cluster spark


    Parameters
    ----------
    sc : context spark
    sqlContext : sqlContext

    Returns
    -------

    '''
    try:
        sc.stop()
    except NameError:
        pass
    except AttributeError:
        pass

def sparkConnect(nb_cores,nb_memory,name,cluster=True,*args,**kwargs):
    '''
    Fonction permettant de se connecter au cluster spark

    Parameters
    ----------
    nb_cores : Nombre de CPU
    nb_memory : Nombre de Go de mémoire
    name : Nom de l'appli spark
    **kwargs : dictionnaire de confs accéptées par SparkConf()

    Returns
    -------
    sc : Spark Context
    sqlContext : sqlContext

    '''
    global sc
    global sqlContext

    conf = SparkConf()

    conf.setAppName("%s"%name)
    conf.set("spark.cores.max"                    , "%s"%nb_cores)
    conf.set("spark.executor.memory"              , "%sg"%nb_memory)

    conf.set("spark.serializer"                   , "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryoserializer.buffer.max"    , "1024m")
    conf.set("spark.driver.maxResultSize"         , "4g")
    if cluster:
        conf.set("spark.mesos.coarse", "True")
    else:
        conf.set("spark.master","local[%s]"%nb_cores)


    for k,v in kwargs.items():
        try:
            conf.set(k,v)
        except:
            pass

    sparkClose()
    sc = SparkContext(conf = conf)
    sqlContext = SQLContext(sc)
    sys.stdout.write("Spark has been initialized successfully ! SparkContext Reference :  {} ".format(sc))

    return sc,sqlContext
