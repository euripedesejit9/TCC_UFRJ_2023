# Databricks notebook source
from delta.tables import *
import os

class DeltalakeManager: 

    def __init__(self, table, database=None, merge_key=None, parent_path=None, *partition_params):
        self.table = table
        self.database = "ufrj_tcc_2023" if database == None else database
        self.merge_key = "" if merge_key == None else merge_key
        self.parent_path = "" if parent_path is None else parent_path.lower().replace(" ", "")
        self.partition_params = partition_params
        
  
    @property
    def table_name(self):
        parent = ""
        if self.parent_path != "":
            parent = self.parent_path + "__"
        return parent + self.table
    
    
    @property
    def location_table_path(self):
        return os.path.join(
            "/mnt/gold",
            self.database,
            self.parent_path,
            self.table)
        
      
    def __check_merge_key(self):
        if self.merge_key is None:
            raise Exception("Merge key not found.")


    def __set_source_df(self, dataframe):
        target_cols = set(self.target_table.columns)
        source_cols = set(dataframe.columns)
        return dataframe.select(list(set.intersection(target_cols, source_cols)))


    @property
    def target_table(self):
        return spark.read.load(self.location_table_path)
  

    def __set_cols_expr(self, cols):
        list_to_update = ["{}{}".format("update.", c) for c in cols]
        return dict(zip(cols, list_to_update))
        

    def merge(self, dataframe, cols_to_update=None, ignore_target_columns=False):
        self.__check_merge_key()
            
        if DeltaTable.isDeltaTable(spark, self.location_table_path):
            
            if (cols_to_update is not None) & (ignore_target_columns):
                raise Exception("Merge not accepted with two parameters filled.")

            if ignore_target_columns == False:
                dataframe = self.__set_source_df(dataframe)

            if cols_to_update:
                spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", False)
                DeltaTable \
                    .forPath(spark, self.location_table_path) \
                    .alias("target") \
                    .merge(dataframe.alias("update"), self.merge_key) \
                    .whenMatchedUpdate(set = self.__set_cols_expr(cols_to_update)) \
                    .whenNotMatchedInsert(values = self.__set_cols_expr(cols_to_update)) \
                    .execute()
            else:
                spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", True)
                DeltaTable \
                    .forPath(spark, self.location_table_path) \
                    .alias("target") \
                    .merge(dataframe.alias("update"), self.merge_key) \
                    .whenMatchedUpdateAll() \
                    .whenNotMatchedInsertAll() \
                    .execute()
        else:
            self.save(dataframe)
        self.__check_external_table()

   
    def save(self, dataframe):
        dataframe \
            .write \
            .format("delta") \
            .partitionBy(self.partition_params) \
            .mode("ErrorIfExists") \
            .save(self.location_table_path)
        self.__check_external_table()


    def append(self, dataframe):
        if DeltaTable.isDeltaTable(spark, self.location_table_path):
            dataframe.write \
                .format("delta") \
                .mode("append") \
                .option("mergeSchema", "true") \
                .partitionBy(self.partition_params) \
                .save(self.location_table_path)
        else:
            self.save(dataframe)
        self.__check_external_table()

  
    def __check_external_table(self):
        if self.database not in [db.name for db in spark.catalog.listDatabases()]:
            self.__check_database()
        
        if self.table_name in sqlContext.tableNames(self.database):
            if ~self.__check_external_table_schema():
                self.recreate_external_table()
        else:
            self.create_external_table()
    

    def overwrite(self, dataframe):
        if DeltaTable.isDeltaTable(spark, self.location_table_path):
            dataframe.write \
                .format("delta") \
                .mode("overwrite") \
                .option("overwriteSchema", "true") \
                .partitionBy(self.partition_params) \
                .save(self.location_table_path)
        else:
            self.save(dataframe)
        self.__check_external_table()
            
            
    def __check_external_table_schema(self):
        table_cols = self.target_table.columns.sort()
        ext_table_cols = spark.table(f"{self.database}.{self.table_name}").columns.sort()
        return True if table_cols == ext_table_cols else False
      
    
    def __check_database(self):
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.database}")
        

    def create_external_table(self):
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.database}")
        spark.sql(f"DROP TABLE IF EXISTS {self.database}.{self.table_name}")
        spark.sql(f"CREATE TABLE IF NOT EXISTS {self.database}.{self.table_name} USING DELTA LOCATION '{self.location_table_path}'")


    def recreate_external_table(self):
        spark.sql(f"DROP TABLE IF EXISTS {self.database}.{self.table_name}")
        self.create_external_table()