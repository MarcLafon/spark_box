# -*- coding: utf-8 -*-

from tqdm import tqdm
import subprocess
import re




def delFileHDFS(f, path_hdfs):
    """
    Delete a file on hdfs

    Parameters
    ----------
    f : String, name of the file to delete on hdfs
    path_hdfs : String, path where we want to delete f on hdfs

    Returns
    -------

    Example :
    __delFileHDFS__('test.csv', '/foo/bar/')
    """
    bash_cmd = ["hdfs","dfs","-rm",path_hdfs+f]
    process = subprocess.Popen(bash_cmd, stdout=subprocess.PIPE)
    process.wait()


def addFileHDFS(f, path_dir, path_hdfs):
    """
    Add a file on hdfs

    Parameters
    ----------
    f : String, name of the file to put on hdfs
    path_dir : String, path where f is located on the machine
    path_hdfs : String, path where we want to put f on hdfs

    Returns
    -------

    Example
    -------
    __addFileHDFS__('test.csv', './' ,'/foo/bar/')
    """
    bash_cmd = ["hdfs","dfs","-put",path_dir+f,path_hdfs]
    process = subprocess.Popen(bash_cmd, stdout=subprocess.PIPE)
    process.wait()


def cleanDataframeColName(df, **kwargs):
    """
    Clean the columns' name in order to obtain the right format

    Parameters
    ----------
    df : sqlDataframe, dataframe we want to clean
    kwargs : dictionary, key=string to replace / value=string to put

    Returns
    -------
    df : sqlDataframe, dataframe cleaned

    Example
    -------
    df = sqlContext.createDataFrame(...)
    df = __cleanDataframeColName__(df, **{'-':'_',' ':'_'})
    """
    for k,v in kwargs.items():
        df=df.select(*[df[x].alias(x.replace(k,v)) for x in df.columns])
    return df


def cleanDataframeColType(df, **kwargs):
    """
    Clean the columns' type in order to obtain the format we want

    Parameters
    ----------
    df : sqlDataframe, dataframe we want to clean
    kwargs : dictionary, key=regex to match a column name / value=pyspark sql type

    Returns
    -------
    df : sqlDataframe, dataframe cleaned

    Example
    -------
    df = sqlContext.createDataFrame(...)
    df = __cleanDataframeColName__(df, **{'ID':StringType()})
    """
    for k,v in kwargs.items():
        for field in df.schema.fields:
            # Check if the column name match the regex
            if(re.search(k,field.name)):
                df = df.withColumn(field.name, df[field.name].cast(v))
    return df


def __writeParquet__(file_type, f, sqlContext, path_dir, path_hdfs,
                      remove_old_hdfs, replace_string, regex_col_to_cast, **kwargs):
    """
    Add a file in hdfs, read the file and write it in parquet file on hdfs

    Parameters
    ----------
    file_type : String, "csv" only available for the moment. Format of the original file

    f : String, name of the file

    path_dir : String, path where f is located on the machine

    path_hdfs : String, path where we want to put f on hdfs

    remove_old_hdfs : Boolean, remove pre-existing file before adding it

    replace_string : dictionary, key=string to replace / value=string to put

    regex_col_to_cast : dictionary, key=regex to match a column name / value=pyspark sql type

    kwargs : dictionary, parameters for the read function, key=parameter's name / value=parameter's value

    Returns
    -------

    Example
    -------
    dict = {"header":"true", "sep":";", "quote":"'", "inferSchema":True}
    replace_string = {'-':'_',' ':'_'}
    regex_col_to_cast = {'ID':StringType()}
    __writeParquetFileHDFS__(file_type="csv", f="test.csv", path_dir="./", path_hdfs="/foo/bar/",
                      remove_old_hdfs=True, replace_string, regex_col_to_cast, **dict)
    """
    # Add file in hdfs (original format)
    if(remove_old_hdfs):
        delFileHDFS(f, path_hdfs)
    addFileHDFS(f, path_dir, path_hdfs)

    # Select the right reader
    if(file_type=="csv"):
        read_func = sqlContext.read.csv
    if(file_type=="json"):
        read_func = sqlContext.read.json
    # Read the file with the sqlContext
    df = read_func(path_hdfs+f,**kwargs)

    # Cleaning
    if(replace_string is not None):
        df = cleanDataframeColName(df, **replace_string)
    if(regex_col_to_cast is not None):
        df = cleanDataframeColType(df, **regex_col_to_cast)

    # Create the new name for the parquet file
    f_new = f.replace("."+file_type,".parquet")

    # Write the dataframe into parquet
    try:
        df.write.parquet(path_hdfs+f_new, mode='overwrite')
    except Exception, e:
        if(file_type=="csv"):
            mySchema = df.schema
            kwargs["inferSchema"]=False
            df = read_func(path_hdfs+f,**kwargs)
            for field in mySchema:
                df = df.withColumn(field.name, df[field.name].cast(field.dataType))
            df.write.parquet(path_hdfs+f_new, mode='overwrite')
        else:
            print e


def writeParquetsFromCSV(files, sqlContext, path_dir, path_hdfs, remove_old_hdfs=True, sep=",", quote = '"', inferSchema=True, replace_string={' ':'_', '-':'_'}, regex_col_to_cast=None):
    """
    Add a parquet csv file in HDFS and write it in parquet file

    Parameters
    ----------
    files : list string, list of all files we want to write on hdfs
    sqlContext : sqlContext, sqlContext we have to use
    path_dir : String, path where f is located on the machine
    path_hdfs : String, path where we want to put f on hdfs
    remove_old_hdfs : Boolean, remove pre-existing file before adding it
    sep : String, csv file separator
    quote : String, quote delimiter in csv file
    inferSchema : Boolean, True=spark try to define the type, False=All features are String
    replace_string : dictionary, key=string to replace / value=string to put
    regex_col_to_cast : dictionary, key=regex to match a column name / value=pyspark sql type

    Returns
    -------

    Example
    -------
    writeParquetFileHDFSFromCSVFile(['test.csv'], path_dir="./", path_hdfs="/foo/bar/", sep=';')
    """
    for f in tqdm(files):
        __writeParquet__("csv",\
                        f, sqlContext, path_dir, path_hdfs, remove_old_hdfs, replace_string, regex_col_to_cast,\
                        **{"header":"true", "sep":sep, "quote":quote, "inferSchema":inferSchema})

# TODO : import JSON
"""
def writeParquetFileHDFSFromJSONFileHDFS(files, path_dir, path_hdfs, remove_old_hdfs=True, replace_string={' ':'_', '-':'_'}, regex_col_to_cast=None):

    for f in tqdm(files):
        __writeParquetFileHDFS__("json",\
                        f, sqlContext, path_dir, path_hdfs, remove_old_hdfs, replace_string, regex_col_to_cast)
"""
