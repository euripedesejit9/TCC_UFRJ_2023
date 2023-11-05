# Databricks notebook source
class MountManager:

    ADLS_EJIT_PROD_CONFIG = {
        "fs.azure.account.auth.type": "OAuth",
        "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
        "fs.azure.account.oauth2.client.id": dbutils.secrets.get("adb-akv-tcc-2023", "client-id"),
        "fs.azure.account.oauth2.client.secret": dbutils.secrets.get("adb-akv-tcc-2023", "secret-id"),
        "fs.azure.account.oauth2.client.endpoint": dbutils.secrets.get("adb-akv-tcc-2023", "tenant-id")
    }
    
    def __init__(self, layer):
        self.layer = layer
        self.mounts = dbutils.fs.mounts()
        self.mount_point =f"/mnt/{self.layer}"


    def create_mount_point(self): 
        """
        This Method is responsible to create mount when not to exist
        """        
        if self.mount_point in [mount.mountPoint for mount in self.mounts]:
            pass
        else:
            dbutils.fs.mount(
                source = f"abfss://{self.layer}@ejitprod.dfs.core.windows.net/",
                mount_point = self.mount_point,
                extra_configs = self.ADLS_EJIT_PROD_CONFIG)
            print(f"mount -> {self.layer} was created.")
    
    
    def unmount_point(self, mount=None):
        """
        This Method is responsible to unmount
        """ 
        if mount:
            dbutils.fs.unmount(mount)
        else:
            print("is necessary give a valid mount_point")

# COMMAND ----------

MountManager("bronze").create_mount_point()
MountManager("silver").create_mount_point()
MountManager("gold").create_mount_point()