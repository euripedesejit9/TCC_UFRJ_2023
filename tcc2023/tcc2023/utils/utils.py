# Databricks notebook source
from urllib.request import urlopen, urlretrieve, Request
from datetime import date, timedelta, datetime
from urllib.error import URLError, HTTPError
from pyspark.sql.window import Window
from pyspark.sql.functions import *
from pyspark.sql.types import *
from bs4 import BeautifulSoup
from functools import reduce
import pandas as pd
import os

# COMMAND ----------

class Utils:

    @staticmethod
    def read_file(format, path, **params):
        """
        Read spark supported files.
        """
        return spark.read.format(format).options(**params).load(path)
    

    @staticmethod
    def write_file(df, format, mode, path, **params):
        """
        Write spark supported files.
        """
        df.write.format(format).options(**params).mode(mode).save(path)
        print(f"writed into: {path}")

    
    @staticmethod
    def check_delta_table(table_path):
        """
        Check if a delta table exists
        """
        return True if DeltaTable.isDeltaTable(spark, table_path) else False
    

    @staticmethod
    def create_empty_df(schema):
        """
        Create an empty dataframe based on a given schema.
        """
        return spark.createDataFrame([], schema)
    
    
    @staticmethod
    def collect_col_values(dataframe, col_name, ignore_list="-"):
        """
        Collect distinct values from a dataframe col to a list
        """
        if dataframe:
            values = dataframe \
                .select(col_name) \
                .distinct() \
                .filter(~col(col_name).isin(ignore_list))
            result = [col[col_name] for col in values.collect()]
            result.sort()
            return result
        else:
            return dataframe
        

    @staticmethod
    def union_all_df(dataframes_list, distinct=False):
        """
        Concatenate dataframes inside dataframes_list params into a unique df.
        """
        try:
            df = reduce(DataFrame.unionByName, dataframes_list)
            if distinct:
                df = df.distinct()
            return df
        except Exception as e:
            print("DataFrame UnionByName function failed.")
            print(e)


    @staticmethod
    def load_delta_table(path):
        """
        Load delta tables from path.
        """
        return spark.read.format("delta").load(path)