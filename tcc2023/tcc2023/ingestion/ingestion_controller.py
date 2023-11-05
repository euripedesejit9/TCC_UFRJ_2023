# Databricks notebook source
# MAGIC %run /Users/euripedesdesenvolvedor@gmail.com/tcc2023/utils/delta_lake_manager

# COMMAND ----------

# MAGIC %run /Users/euripedesdesenvolvedor@gmail.com/tcc2023/utils/mount_manager

# COMMAND ----------

# MAGIC %run /Users/euripedesdesenvolvedor@gmail.com/tcc2023/utils/utils

# COMMAND ----------

class IngestionController:
    
    LAYERS_PATH = {
        "bronze" : f"/mnt/bronze/",
        "silver" : "/mnt/silver/",
        "gold" : "/mnt/gold/"
    }

    def __init__(self, folder_name, ingestion_date=None):
        self.folder_name = folder_name
        self.ingestion_date = ingestion_date if ingestion_date is not None else date.today()
    
   
    def get_dates_path(self):
        """
        Make dates string path formated to delta lake
        """
        return os.path.join(
            self.ingestion_date.strftime("%Y"),
            self.ingestion_date.strftime("%Y%m"),
            self.ingestion_date.strftime("%Y%m%d")
        )
        
        
    def default_layer_path(self, layer):
        return os.path.join(self.LAYERS_PATH[layer],
            self.folder_name,
            self.get_dates_path()
        )
    
    
    def read_bronze(self, **options):
        return Utils.read_file("parquet", self.default_layer_path("bronze"))
    
    
    def read_silver(self, **options):
        return Utils.read_file("parquet", self.default_layer_path("silver"))
    

    def read_gold(self, database):
        path = os.path.join(self.LAYERS_PATH["gold"], database, self.folder_name)
        return Utils.load_delta_table(path)


    def write_bronze(self, dataframe, mode="overwrite"):
        Utils.write_file(dataframe, "parquet", mode, self.default_layer_path("bronze"))


    def write_silver(self, dataframe):
        Utils.write_file(dataframe, "parquet", "overwrite", self.default_layer_path("silver"))


    def write_gold(self,dataframe, schema, table, database=None, merge_key=None, parent_path=None, *partition_params):
        dlm = DeltalakeManager(table, database, merge_key, parent_path, *partition_params)

        if Utils.check_delta_table(dlm.location_table_path) == False:
            empty_df = Utils.create_empty_df(schema)
            dlm.overwrite(empty_df)

        dlm.merge(dataframe, None, True)
        print("merge sucessfully.")