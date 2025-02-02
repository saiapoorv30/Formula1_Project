# Databricks notebook source
# MAGIC %md
# MAGIC ### mount azure data lake using storage principal
# MAGIC

# COMMAND ----------

storage_account_name = "formaula1dl"
client_id = dbutils.secrets.get(scope='formula1-scope', key='formula1-app-client-id')
tenant_id = dbutils.secrets.get(scope='formula1-scope', key='formula1-tenant-id')
client_secret = dbutils.secrets.get(scope='formula1-scope', key='formula1-client-secret')

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

def mount_adls(container_name):
    dbutils.fs.mount(
      source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
      mount_point = f"/mnt/{storage_account_name}/{container_name}",
      extra_configs = configs)

# COMMAND ----------

mount_adls("processed")

# COMMAND ----------

dbutils.fs.ls("/mnt/formaula1dl/processed")

# COMMAND ----------

mount_adls("presentation")

# COMMAND ----------

dbutils.fs.ls("/mnt/formaula1dl/presentation")

# COMMAND ----------

