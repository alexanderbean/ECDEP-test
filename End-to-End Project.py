# Databricks notebook source
#Go to app registrations, register an application, and then record the following info

Application (client) ID: b8af1f18-41f9-45a5-a647-eb24a880c372
Directory (tenant) ID: d5afdef0-f7d5-487e-b3e7-0caf0a9fefe7

#Then create a secret, and record the secret value (NOT the ID)

Secret value: rew8Q~AVjMCPpHH9C6Dp-U486pS2rHzkFBfAOcty

# Import libraries
#from pyspark.sql.functions import col
#from pyspark.sql.types import IntegerType, DoubleType, BooleanType, DateType

# Unmount the directory if it's already mounted
#try:
#    dbutils.fs.unmount("/mnt/<<PICK A NAME>>")
#except:
#    print("Directory not mounted, proceeding with mounting.")

# Mounting Databricks on top of Data Lake (creating connection between Databricks & Data Lake)
#configs = {"fs.azure.account.auth.type": "OAuth",
#           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
#           "fs.azure.account.oauth2.client.id": "<<CLIENT ID LISTED ABOVE>>",
#           "fs.azure.account.oauth2.client.secret": "<<SECRET VALUE LISTED ABOVE>>",
#           "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/<<TENANT ID LISTED ABOVE>>/oauth2/token"}

#dbutils.fs.mount(
#    source = "abfss://<<CONTAINER>>@<<STORAGEACCOUNT>>.dfs.core.windows.net", #container@storageaccount
#    mount_point = "/mnt/<<PICK A NAME>>",
#    extra_configs = configs
#)





# COMMAND ----------

# Import libraries
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, DoubleType, BooleanType, DateType

# Unmount the directory if it's already mounted
try:
    dbutils.fs.unmount("/mnt/dataEngineering")
except:
    print("Directory not mounted, proceeding with mounting.")

# Mounting Databricks on top of Data Lake (creating connection between Databricks & Data Lake)
configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": "b8af1f18-41f9-45a5-a647-eb24a880c372",
           "fs.azure.account.oauth2.client.secret": "rew8Q~AVjMCPpHH9C6Dp-U486pS2rHzkFBfAOcty",
           "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/d5afdef0-f7d5-487e-b3e7-0caf0a9fefe7/oauth2/token"}

dbutils.fs.mount(
    source = "abfss://raw-data@endtoendproject0624.dfs.core.windows.net", #container@storageaccount
    mount_point = "/mnt/dataEngineering",
    extra_configs = configs
)


# COMMAND ----------

dim_customers_raw.show()

# COMMAND ----------

from pyspark.sql.functions import col, when, desc, regexp_replace, trim
from pyspark.sql.types import IntegerType, DoubleType, BooleanType, DateType

# COMMAND ----------

# CUSTOMERS

dim_customers_raw = spark.read.format("csv").option("header","true").load("/mnt/dataEngineering/dim_customers_raw")

dim_customers_raw = dim_customers_raw.withColumn("Type", regexp_replace(col("Type"), "Type-", ""))

dim_customers_raw = dim_customers_raw.dropDuplicates(["Customer ID"])

dim_customers_raw = dim_customers_raw.orderBy("Customer ID")

dim_customers_raw = dim_customers_raw.filter(col("Age").cast("int").isNotNull())

dim_customers_raw = dim_customers_raw.withColumn("Gender", when(col("Gender") == "M", "Male").when(col("Gender") == "F", "Female").otherwise(col("Gender")))

# Unmount the directory if it's already mounted
try:
    dbutils.fs.unmount("/mnt/dim_customers_raw")
except:
    print("Directory not mounted, proceeding with mounting.")

# Mounting Databricks on top of Data Lake (creating connection between Databricks & Data Lake)
configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": "b8af1f18-41f9-45a5-a647-eb24a880c372",
           "fs.azure.account.oauth2.client.secret": "rew8Q~AVjMCPpHH9C6Dp-U486pS2rHzkFBfAOcty",
           "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/d5afdef0-f7d5-487e-b3e7-0caf0a9fefe7/oauth2/token"}

dbutils.fs.mount(
    source = "abfss://transformed-data@endtoendproject0624.dfs.core.windows.net", #container@storageaccount
    mount_point = "/mnt/dim_customers_raw",
    extra_configs = configs
)

dim_customers_raw.write.option("header","true").csv("/mnt/dim_customers_raw/dim_customers_transformed")

# COMMAND ----------

# PAYMENT METHOD

dim_payment_method_raw = spark.read.format("csv").option("header","true").load("/mnt/dataEngineering/dim_payment_method_raw")

dim_payment_method_raw = dim_payment_method_raw.na.drop()

# Unmount the directory if it's already mounted
try:
    dbutils.fs.unmount("/mnt/dim_payment_method_raw")
except:
    print("Directory not mounted, proceeding with mounting.")

# Mounting Databricks on top of Data Lake (creating connection between Databricks & Data Lake)
configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": "b8af1f18-41f9-45a5-a647-eb24a880c372",
           "fs.azure.account.oauth2.client.secret": "rew8Q~AVjMCPpHH9C6Dp-U486pS2rHzkFBfAOcty",
           "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/d5afdef0-f7d5-487e-b3e7-0caf0a9fefe7/oauth2/token"}

dbutils.fs.mount(
    source = "abfss://transformed-data@endtoendproject0624.dfs.core.windows.net", #container@storageaccount
    mount_point = "/mnt/dim_payment_method_raw",
    extra_configs = configs
)

dim_payment_method_raw.write.option("header","true").csv("/mnt/dim_payment_method_raw/dim_payment_method_transformed")

# COMMAND ----------

# SALES CHANNEL

dim_sales_channel_raw = spark.read.format("csv").option("header","true").load("/mnt/dataEngineering/dim_sales_channel_raw")

dim_sales_channel_raw = dim_sales_channel_raw.na.drop()

dim_sales_channel_raw = dim_sales_channel_raw.dropDuplicates(["Sales Channel ID"])

dim_sales_channel_raw = dim_sales_channel_raw.orderBy("Sales Channel ID")

# Unmount the directory if it's already mounted
try:
    dbutils.fs.unmount("/mnt/dim_sales_channel_raw")
except:
    print("Directory not mounted, proceeding with mounting.")

# Mounting Databricks on top of Data Lake (creating connection between Databricks & Data Lake)
configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": "b8af1f18-41f9-45a5-a647-eb24a880c372",
           "fs.azure.account.oauth2.client.secret": "rew8Q~AVjMCPpHH9C6Dp-U486pS2rHzkFBfAOcty",
           "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/d5afdef0-f7d5-487e-b3e7-0caf0a9fefe7/oauth2/token"}

dbutils.fs.mount(
    source = "abfss://transformed-data@endtoendproject0624.dfs.core.windows.net", #container@storageaccount
    mount_point = "/mnt/dim_sales_channel_raw",
    extra_configs = configs
)

dim_sales_channel_raw.write.option("header","true").csv("/mnt/dim_sales_channel_raw/dim_sales_channel_transformed")


# COMMAND ----------

# SALES

fact_sales_raw = spark.read.format("csv").option("header","true").load("/mnt/dataEngineering/fact_sales_raw")

fact_sales_raw = fact_sales_raw.drop("Discount FLAG")
fact_sales_raw = fact_sales_raw.drop("Customer Feedback")
fact_sales_raw = fact_sales_raw.drop("Warranty ID")
fact_sales_raw = fact_sales_raw.drop("Employee ID")
fact_sales_raw = fact_sales_raw.drop("Store ID")
fact_sales_raw = fact_sales_raw.drop("Progress Status ID")
fact_sales_raw = fact_sales_raw.drop("Delivery Channel ID")

fact_sales_raw = fact_sales_raw.withColumn("Total Amount", regexp_replace(col("Total Amount"), "\\$", ""))
fact_sales_raw = fact_sales_raw.withColumn("Tax Amount", regexp_replace(col("Tax Amount"), "\\$", ""))
fact_sales_raw = fact_sales_raw.withColumn("Manufactory Costs", regexp_replace(col("Manufactory Costs"), "\\$", ""))
fact_sales_raw = fact_sales_raw.withColumn("Shipping Cost", regexp_replace(col("Shipping Cost"), "\\$", ""))
fact_sales_raw = fact_sales_raw.withColumn("Profit", regexp_replace(col("Profit"), "\\$", ""))

fact_sales_raw = fact_sales_raw.withColumn("Total Amount", trim(col("Total Amount")))
fact_sales_raw = fact_sales_raw.withColumn("Tax Amount", trim(col("Tax Amount")))
fact_sales_raw = fact_sales_raw.withColumn("Manufactory Costs", trim(col("Manufactory Costs")))
fact_sales_raw = fact_sales_raw.withColumn("Shipping Cost", trim(col("Shipping Cost")))
fact_sales_raw = fact_sales_raw.withColumn("Profit", trim(col("Profit")))

# Unmount the directory if it's already mounted
try:
    dbutils.fs.unmount("/mnt/fact_sales_raw")
except:
    print("Directory not mounted, proceeding with mounting.")

# Mounting Databricks on top of Data Lake (creating connection between Databricks & Data Lake)
configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": "b8af1f18-41f9-45a5-a647-eb24a880c372",
           "fs.azure.account.oauth2.client.secret": "rew8Q~AVjMCPpHH9C6Dp-U486pS2rHzkFBfAOcty",
           "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/d5afdef0-f7d5-487e-b3e7-0caf0a9fefe7/oauth2/token"}

dbutils.fs.mount(
    source = "abfss://transformed-data@endtoendproject0624.dfs.core.windows.net", #container@storageaccount
    mount_point = "/mnt/fact_sales_raw",
    extra_configs = configs
)

fact_sales_raw = fact_sales_raw.repartition(1)
fact_sales_raw.write.option("header","true").csv("/mnt/fact_sales_raw/fact_sales_transformed")
