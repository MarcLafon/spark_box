# -*- coding: utf-8 -*-
"""
Created on Wed Apr 19 18:09:14 2017

@author: vmasdeu
"""
from pyspark.sql.functions import udf
from pyspark.sql.types import *

def check_consistancy_of_mappers(df, mappers):
    """
    Parameters:
    df: a dataframe spark
    mappers: a list (or a single mapper) of dictionnary mapper
    """
    boolean = True
    error = ''
    # Check if all the mappers have all their keys corresponding to a df column and if they have an output key
    cpt = 0
    mappers_key_set = set(mappers[0].keys())
    #mappers_key_list = mappers[0].keys()
    while boolean & (cpt<len(mappers)):
        mapper = mappers[cpt]
        # Check if the mapper has an output key
        boolean &= ('output' in mapper.keys())
        if (not boolean):
            error = "One of the mappers has not 'output' key"
            return boolean, error
        # Check if each key in the mapper corresponds to a df column name
        keys = [x for x in mapper.keys() if (x != 'output')]
        for key in keys:
            boolean &= (key in df.columns)
        if (not boolean):
            error = "One of the mappers has a key that does not exist in df columns"
            return boolean, error
        # Check if the mapper involved the same df columns as the others
        boolean &= (set(mapper.keys())==mappers_key_set)
        if (not boolean):
            error = "All the mappers don't have the same keys"
            return boolean, error
        cpt += 1
    return boolean, error

def order_mappers(mappers):
    """
    Function that return a list of mappers made of list of tuples from a list of dictionnary mappers.
    The tuple (key,value) are in the same oreder whatever the mapper in the output list of mappers
    parameters:
        mappers: list of dictionnary mappers
    """
    m0 = mappers[0]
    common_keys = m0.keys()
    m0_new = [(x, m0[x]) for x in common_keys]
    ordered_mappers = [m0_new]
    if (len(mappers)>1):
        for m in mappers[1:]:
            ordered_mappers.append([(x, m[x]) for x in common_keys])
    return ordered_mappers

def check_matching(mods_list, mapper):
    """
    Function that check if the mapper matches the current dataframe modalities
    parameters:
        mods_list: list of modality in the dataframe (in the same order as the mapper's keys)
        mapper: a tuple mapper (list of tuple, each tuple of the form (key, value))
    """
    mapper_without_output = [x for x in mapper if x[0]!='output']
    boolean = True
    mod_cpt = 0
    while boolean & (mod_cpt<len(mods_list)):
        boolean = (mods_list[mod_cpt]==mapper_without_output[mod_cpt][1])
        mod_cpt += 1
    return boolean

def get_output_of_mapper(mapper):
    """
    Function that return the output value of a mapper.
    parameter:
        mapper: a tuple mapper
    return:
        the output value of a mapper
    """
    return [y for (x,y) in mapper if x=='output'][0]

def assume_spark_types(x):
    """
    Function that return a spark type from a python object.
    Warning not all the types are managed. Please refer to source code to get the available types.
    parameters:
        x: python object
    return:
        output: spark type (IntegerType(), StringType())
    """
    if isinstance(x, basestring):
        return StringType()
    elif isinstance(x, bool):
        return BooleanType()
    elif isinstance(x, float):
        return FloatType()
    elif isinstance(x, int):
        return IntegerType()

def get_matching_mapper(mods_list, mappers):
    """
    Given a list of value, return the mapper of a mappers list that match the mods_list in the same order.
    parameters:
        mods_list: list of value (int, string...)
    return:
        boolean: True if one of the mappers matched the list of value. False otherwise.
        mapper: The mapper that match de list of value (in case of a match). None otherwise.
    """
    for mapper in mappers:
        boolean = check_matching(mods_list, mapper)
        if boolean:
            return boolean, mapper
        else:
            pass
    return False, None

def create_udf_from_mappers(mappers, default_value, output_type):
    """
    Create a udf pyspark function that depends on the matching mapper
    parameters:
        mappers: tuple mappers (list of list of tuple (key, value)) all in the same order
        default_value: value that must be return if there is no matching mapper
        output_type: the type of the output value in the mappers (if None it will be assume)
    return:
        A udf function.
    """
    # Return an udf that can be applied to a dataframe
    output_expl = get_output_of_mapper(mappers[0])
    if not output_type:
        output_type = assume_spark_types(output_expl)
    def udf_mapping(*mods):
        mods = list(mods)
        # Look for the mapper that matches the modalities (mods) of the dataframe
        boolean, mapper_found = get_matching_mapper(mods, mappers)
        if boolean:
            # If there has been a match then we return the corresponding output modality
            return get_output_of_mapper(mapper_found)
        else:
            # If not the default_value
            return default_value
    return udf(udf_mapping, output_type)

def mapping_d2d(df, mappers, output_var_name, default_value=None, output_type=None):
    """
    Function that creates a new categorial variable from a set of p input catagorial variables.
    parameters:
        df: spark dataframe
        mappers: list of mapper.
        A mapper is a dictionnary or a list of tuple or a list of tuples that associate a modality (value) to each variable (key) and a special modality for the output variable 'output':output_modality
        ex: {var1:'apple', var2:'France', output:'european fruit'} or [(var1,'apple'), (var2,'France'), (output,'european fruit')] or [[var1,'apple'], [var2,'France'], [output,'european fruit']].
        It must be read as if var1='apple' and var2='France' then my output variable is 'european fruit'
        output_var_name: the name of the new variable
        default_value: Optional. Default value that must be associate if no combinaison of the input variable are not define. Default None
        output_type: the spark type (IntegerType(), StringType()...) of the output variable. Default None, it then will be assume with the function assume_spark_type
    return:
        df: a new spark dataframe with the new column 'output_var_name'
    """
    #TODO: manage the case where mappers is only a dict without list, and all the derived cases where dictionary are replace
    #by either tuple of list
    mappers = [dict(mapper) for mapper in mappers]
    boolean, err = check_consistancy_of_mappers(df, mappers)
    if (not boolean):
        print "Error: %s"%err
        return None
    else:
        ordered_mappers = order_mappers(mappers)
        udf_to_apply = create_udf_from_mappers(ordered_mappers, default_value, output_type)
        df_col = [x for (x,y) in ordered_mappers[0] if x!='output']
        return df.withColumn(output_var_name, udf_to_apply(*df_col))
