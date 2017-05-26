# -*- coding: utf-8 -*-

from pyspark.sql.types import *
from pyspark.sql.functions import udf
from datetime import datetime
import numpy as np
import re
from pyspark.sql import functions as F

SAMPLING_FRACTION = 0.2
PROBA_THRESHOLD_1 = 0.51
PROBA_THRESHOLD_2 = 0.33
DAY_FIRST_DATE_FORMAT3 = ['%d/%m/%y', '%d-%m-%y', '%d%m%y', '%d/%b/%y', '%d-%b-%y', '%d%b%y', '%d/%B/%y', '%d-%B-%y', '%d%B%y',\
                                                  '%d/%m/%Y', '%d-%m-%Y', '%d%m%Y', '%d/%b/%Y', '%d-%b-%Y', '%d%b%Y', '%d/%B/%Y', '%d-%B-%Y', '%d%B%Y']
MONTH_FIRST_DATE_FORMAT3 = ['%m/%d/%y', '%m-%d-%y', '%m%d%y', '%b/%d/%y', '%b-%d-%y', '%b%d%y', '%B/%d/%y', '%B-%d-%y', '%B%d%y',\
                                                        '%m/%d/%Y', '%m-%d-%Y', '%m%d%Y', '%b/%d/%Y', '%b-%d-%Y', '%b%d%Y', '%B/%d/%Y', '%B-%d-%Y', '%B%d%Y']
MONTH_FIRST_DATE_FORMAT2 = ['%m/%y', '%m-%y', '%m%y', '%b/%y', '%b-%y', '%b%y', '%B/%y', '%B-%Y', '%B%y']
YEAR_FIRST_DATE_FORMAT2 = ['%y/%m', '%y-%m', '%y%m', '%y/%b', '%y-%b', '%y%b', '%y/%B', '%y-%B', '%y%B']
DAY_FIRST_REGEX3 = ['^[0-9]{1,2}/[a-zA-Z]{4,8}/[0-9]{4}$', '^[0-9]{1,2}-[a-zA-Z]{4,8}-[0-9]{4}$', '^[0-9]{1,2}[a-zA-Z]{4,8}[0-9]{4}$',\
                                        '^[0-9]{1,2}/[a-zA-Z]{4,8}/[0-9]{2}$', '^[0-9]{1,2}-[a-zA-Z]{4,8}-[0-9]{2}$', '^[0-9]{1,2}[a-zA-Z]{4,8}[0-9]{2}$']
MAPPING_REGEX = {'jan':'jan',\
                                'fev':'feb',\
                                'mar':'mar',\
                                'avr':'apr',\
                                'mai':'may',\
                                'juin':'jun',\
                                'juillet':'jul', 'juil':'jul',\
                                'aou':'aug',\
                                'sep':'sep',\
                                'oct':'oct',\
                                'nov':'nov',\
                                'dec':'dec'}



def parse_dates(df, columns=None, sampling_fraction=None, proba_threshold_1=None, proba_threshold_2=None, new_col=False, error_column=False):
    """
     Function that search for column of a dataframe that are likely to be dates record as string and parse them with the schema that matches the most of the element
     parameters:
             df: The spark dataframe we want to parse
             columns: the subset of df columns we want to parse. Selecting few columns improve the speed. If None, all the stringType() columns are parsed.
             sampling_fraction: the fraction of rows we want to use to guess the date schema use for parsing. If None it will be set to 0.2. Must be less than 1.
             proba_threshold_1: the proportion of the element in the column being parsed that must matches any of the possible date format to assume the column is of the type date. If None, set to 0.51
             proba_threshold_2: The minimum value for a proportion of representation of a schema in a list of possible schema to assume that this proportion
             is enough to say it is the main representation format (then all the column will be parse with this schema). If None set to 0.33
             new_col: Set True if the column parsed must appear in a new column
             error_column: Set True if each parsing error must appear in a new column of 0/1 (1=parsing error, 0=no parsing error)
     return:
             a dataframe with all the new columns parsed. No columns have been deleted, only new columns added with name of the forme 'colName_parsed'
    """
    
    udf_parsing_error = F.udf(__parsing_error__, IntegerType())
    
    if columns==None:
        columns = [f.name for f in df.schema.fields if (f.dataType == StringType())]
    else:
        columns = [f.name for f in df.schema.fields if ((f.dataType == StringType())&(f.name in columns))]
    if (sampling_fraction is None):
        sampling_fraction = SAMPLING_FRACTION
    elif (sampling_fraction==1):
        sampling_fraction=0.99
    df_sample = df.sample(False, sampling_fraction, 42)
    for col in columns:
        s = [r[col] for r in df.select(col).collect() if r[col]] # Sampling
        possible_schemas = __get_possible_schemas__(s) #Of the form [("mm/dd/yyyy", proba), ('mm/dd/YY', proba)...]
        main_schema = __get_main_schema__(possible_schemas, proba_threshold_1, proba_threshold_2)
        if main_schema==-1: # The current column is probably not a date column
            pass
        elif main_schema==-2:
            possible_schemas_gt0 = [ps for ps in possible_schemas if ps[1]>0]
            print("No significant schema has been found for the column %s. Possible schemas are\n: %s \nPlease parse it separatly."%(col, possible_schemas_gt0))
            pass
        elif main_schema==-3:
            possible_schemas_gt0 = [ps for ps in possible_schemas if ps[1]>0]
            print("Several schemas are possible for column %s: \n %s \nyou should parse this column specifically using sparse_date_with_schema function."%(col, possible_schemas))
            pass
        else:
            schema_type = __get_schema_type__(main_schema)
            udf_parse_date = __create_udf_parse_date__(main_schema, schema_type)
            if error_column:
                df = df.withColumn(col+'_parsed', udf_parse_date(col))
                df = df.withColumn(col+'_err', udf_parsing_error(col,col+'_parsed'))
                if new_col:
                    print("Column %s has been parsed. The new parsed column is %s_parsed and the error column is %s_err."%(col, col, col))
                else:
                    df = df.drop(col+'_parsed')
                    print("Column %s has been parsed. The error column is available as %s_err."%(col, col))
            else:
                if new_col:
                    df = df.withColumn(col+'_parsed', udf_parse_date(col))
                    print("Column %s has been parsed. The new parsed column is %s_parsed."%(col, col))
                else:
                    df = df.withColumn(col, udf_parse_date(col))
                    print("Column %s has been parsed."%(col))
    return df

def __get_possible_schemas__(s):
    """
    For a given list of string dates, the function return a dictionary with all the schemas as keys and values corresponding to the proportion of
    string dates in s that match the schema set in key
    """
    if len(s)>0:
        dict_output = __find_date_format__(s[0])
        if len(s)>1:
            for string_date in s[1::]:
                dict_tmp = __find_date_format__(string_date)
                for key in dict_tmp:
                    dict_output[key] += dict_tmp[key]
        #max_value = max(dict_output.values())
        #Return object if of the form [("mm/dd/yyyy", proba), ('mm/dd/YY', proba)...]
        return [(key, dict_output[key]/float(len(s))) for key in dict_output]
    else:
        return []

def __find_date_format__(string_date):
    """
    Given a string date it returns the dictionnary of all the available schemas as keys and a value set to 0 or 1 depending if the string
    matches the schema set in the key
    """
    date_format = DAY_FIRST_DATE_FORMAT3+MONTH_FIRST_DATE_FORMAT3+MONTH_FIRST_DATE_FORMAT2+YEAR_FIRST_DATE_FORMAT2
    regex_format = DAY_FIRST_REGEX3
    all_format = date_format+regex_format
    output_dict = dict(zip(all_format, np.zeros(len(all_format))))
    if not (string_date is None):
        for pattern in date_format:
            if __match_format__(string_date, pattern):
                output_dict[pattern] += 1
        for pattern in regex_format:
            if __match_regex__(string_date, pattern):
                output_dict[pattern] += 1
    return output_dict

def __match_format__(s, d_format):
    """
    parameters:
            s: string
            d_format: date format (%d/%m/%y, %d-%m-%y...)
    return:
            boolean telling if the string s matches the date format d_format
    """
    if not (s is None):
        try:
            test = datetime.strptime(s, d_format)
            return True
        except ValueError:
            return False
    else:
        return False

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

def __get_main_schema__(possible_schemas, proba_threshold_1=None, proba_threshold_2=None):
    """
    Given all the possibles schemas and there corresponding probability the function return the schema that is the more represented
    parameters:
            possible_schemas: List of the possibles schemas, of the form [("mm/dd/yyyy", proba), ('mm/dd/YY', proba)...]
            proba_threshold_1: the proportion of the element in the column that must match one of the available date schema (format or regex) to
            assume the column is of the type date.
            proba_threshold_2: The minimum value for a proportion of representation of a schema in a list of possible schema to assume that this proportion
            is enough to say it the main representation format (then all the column will be parse with this schema)
    return:
            The more reprensented schema in the possible schemas (according to the probabilities) as soon as this schemas has a probability of
            occurrence higher than proba_threshold_2
    """
    if proba_threshold_1 is None:
        proba_threshold_1=PROBA_THRESHOLD_1
    if proba_threshold_2 is None:
        proba_threshold_2=PROBA_THRESHOLD_2
    probas = [x[1] for x in possible_schemas]
    max_value = max(probas)
    sum_proba = sum(probas)
    if (max_value==0) or (sum_proba<proba_threshold_1):
        #It means the columns is probably not a date column
        return -1
    elif max_value<proba_threshold_2:
        # It means that several schemas are available but none of them is over represented
        return -2
    else:
        main_schemas = [x[0] for x in possible_schemas if x[1]==max_value]
        if len(main_schemas)>1:
            return -3
        else:
            return main_schemas[0]

def __get_schema_type__(schema):
    """
    Function that return a schema type given a date schema
    """
    if schema in DAY_FIRST_DATE_FORMAT3:
        return 'DAY_FIRST_DATE_FORMAT3'
    if schema in MONTH_FIRST_DATE_FORMAT3:
        return 'MONTH_FIRST_DATE_FORMAT3'
    if schema in MONTH_FIRST_DATE_FORMAT2:
        return 'MONTH_FIRST_DATE_FORMAT2'
    if schema in YEAR_FIRST_DATE_FORMAT2:
        return 'YEAR_FIRST_DATE_FORMAT2'
    if schema in DAY_FIRST_REGEX3:
        return 'DAY_FIRST_REGEX3'
    return None

def __get_consistant_schema_with_schema_type__(possible_schemas, schema_type):
    """
    Return all the date schemas that have the same schema type as schema_type
    parameters:
            possible_schemas: list of all the possible schemas (only the schemas not the proba)
            schema_type: the schema type we want to match
    return:
            all the date schemas that have the same schema type as schema_type
    """
    if (schema_type=='DAY_FIRST_DATE_FORMAT3') or (schema_type=='DAY_FIRST_REGEX3'):
        return np.intersect1d(possible_schemas, DAY_FIRST_DATE_FORMAT3+DAY_FIRST_REGEX3)
    if schema_type=='MONTH_FIRST_DATE_FORMAT3':
        return np.intersect1d(possible_schemas, MONTH_FIRST_DATE_FORMAT3)
    if schema_type=='MONTH_FIRST_DATE_FORMAT2':
        return np.intersect1d(possible_schemas, MONTH_FIRST_DATE_FORMAT2)
    if schema_type=='YEAR_FIRST_DATE_FORMAT2':
        return np.intersect1d(possible_schemas, YEAR_FIRST_DATE_FORMAT2)

def __regex_date_2_date_format__(x):
    x_split = []
    if (__match_regex__(x, DAY_FIRST_REGEX3[0])) or (__match_regex__(x, DAY_FIRST_REGEX3[3])):
        x_split = x.split('/')
    elif (__match_regex__(x, DAY_FIRST_REGEX3[1])) or (__match_regex__(x, DAY_FIRST_REGEX3[4])):
        x_split = x.split('-')
    elif (__match_regex__(x, DAY_FIRST_REGEX3[2])) or (__match_regex__(x, DAY_FIRST_REGEX3[5])):
        if __match_regex__(x[0:2], '^\d{2}'):#Starts with 2 digits
            x_split.append(x[0:2])
            if __match_regex__(x[-4::], '\d{4}$'): #Ends with 4 digit
                x_split.append(x[2:(len(x)-4)]) #Month
                x_split.append(x[-4::]) #Year
            else:#Ends with 2 digits
                x_split.append(x[2:(len(x)-2)]) #Month
                x_split.append(x[-2::]) #Year
        else: #Starts with 1 digit
            x_split.append(x[0:1])
            if __match_regex__(x[-4::], '\d{4}$'): #Ends with 4 digit
                x_split.append(x[1:(len(x)-4)]) #Month
                x_split.append(x[-4::]) #Year
            else:#Ends with 2 digits
                x_split.append(x[1:(len(x)-2)])#Month
                x_split.append(x[-2::])#Year
    if len(x_split)>0:
        x_month = x_split[1]
        try:
            new_month_format = MAPPING_REGEX[x_month]
        except KeyError:
            new_month_format = x_month.lower()[0:3]
            try:
                new_month_format = MAPPING_REGEX[new_month_format]
            except KeyError:
                return None
        x_split[1] = new_month_format
        new_x = '/'.join(x_split)
        try:
            return datetime.strptime(new_x, '%d/%b/%y')
        except ValueError:
            try:
                return datetime.strptime(new_x, '%d/%b/%Y')
            except ValueError:
                return None
    else:
        return None

def __create_udf_parse_date__(main_schema, schema_type):
    """
    Function that creates a udf to map a string date to a given date format
    parameters:
            main_schema: the final date format (%d/%m/%y...)
            schema_type: the schema type of the main schema
    return:
            a udf
    """
    def udf_parse_date(x):
        if not (x is None):
            try:
                return datetime.strptime(x, main_schema)
            except ValueError:
                dict_output = __find_date_format__(x)
                max_value = max(dict_output.values())
                if max_value>0:
                    possible_schemas = [key for key in dict_output if dict_output[key]==max_value]
                    possible_schemas = __get_consistant_schema_with_schema_type__(possible_schemas, schema_type)
                    if len(possible_schemas)==1:
                        schema_type_of_x = __get_schema_type__(possible_schemas[0])
                        if schema_type_of_x in ['DAY_FIRST_DATE_FORMAT3', 'MONTH_FIRST_DATE_FORMAT3', 'MONTH_FIRST_DATE_FORMAT2', 'YEAR_FIRST_DATE_FORMAT2']:
                            return datetime.strptime(x, possible_schemas[0])
                        elif schema_type_of_x in ['DAY_FIRST_REGEX3']:
                            return __regex_date_2_date_format__(x)
                    elif len(possible_schemas)>1:
                        return None
    return F.udf(udf_parse_date, DateType())

def __parsing_error__(x_g, x_g_parsed):
    if (x_g is None) and (x_g_parsed is None):
        return 0
    elif (x_g is None) and (not (x_g_parsed is None)):
        return 1
    elif (not (x_g is None)) and (x_g_parsed is None):
        return 1
    else:
        return 0

