# Databricks notebook source
# MAGIC %md
# MAGIC # Welcome!

# COMMAND ----------

# MAGIC %md
# MAGIC This is an intro-level notebook to walk you through core ideas of working with Spark (particularly PySpark) on Databricks. 
# MAGIC 
# MAGIC To get started, click the "Clusters" drop down at top-left to attach to a running cluster (if there are none running, go ahead and create one)
# MAGIC 
# MAGIC To execute each cell, you can:
# MAGIC * Press the "play" button at top-right
# MAGIC * On Mac, press `Shift+Return`
# MAGIC 
# MAGIC More details on Notebooks: [Use notebooks](https://docs.databricks.com/notebooks/notebooks-use.html#develop-notebooks)

# COMMAND ----------

# MAGIC %md
# MAGIC # Working in Databricks

# COMMAND ----------

# MAGIC %md
# MAGIC ## Notebook Cells
# MAGIC Cells are a small code segment that provides simplified development and easy debugging 
# MAGIC You can click the round `(+)` button between existing cells to add a new cell, drag existing cells around, colapse/delete/hide/etc. cells as needed.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Magic Commands
# MAGIC Each cell can be a different supported language! This allows you to switch languages depending on the task at hand. 

# COMMAND ----------

print("This is a python cell")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT "This is a SQL cell"
# MAGIC FROM (VALUES (1))

# COMMAND ----------

# MAGIC %md
# MAGIC If you don't use a Magic command, it will run the notebook's default language, visible at the top of the screen. 
# MAGIC 
# MAGIC Other important magic commands:
# MAGIC * `%fs`: file system commands
# MAGIC * `%sh`: shell commands, e.g. `%sh ls`
# MAGIC * `%r`, `%scala`, `%sql`, `%python`: supported notebook languages
# MAGIC * `%run`: to run another notebook

# COMMAND ----------

# MAGIC %md
# MAGIC ## DBUtils
# MAGIC In addition to programming languages, you can use a set of useful "utility" functions for common tasks. These utilities are known as `dbutils`, and can be can access in notebooks via `dbutils.`

# COMMAND ----------

#Example: dbutils filesystem commands to see sample datasets 
dbutils.fs.ls("dbfs:/databricks-datasets")

# COMMAND ----------

#See all dbutils modules
dbutils.help()

# COMMAND ----------

# MAGIC %md
# MAGIC # Spark in Databricks

# COMMAND ----------

# MAGIC %md
# MAGIC ## Databricks Runtime
# MAGIC Each Databricks cluster comes pre-installed with most of what you will need to do your job. The collection of libraries, environment settings, and other configs running on your cluster are collectively known as the **Databricks Runtime** (a.k.a. "DBR"). 
# MAGIC 
# MAGIC As a managed platform, Databricks continuously releases new runtimes with performance improvements and new features. You can learn more about the Databricks Runtime and see release notes here: [DBR Notes](https://docs.databricks.com/release-notes/runtime/releases.html)
# MAGIC 
# MAGIC To find out the Runtime your cluster is running, simply click the cluster selector at top-left of a notebook. This will show you physical details of the cluster (amount of memory, etc.) and the DBR version. For example, it might say `DBR 7.3 LTS ML`. You can then go to the release notes for that version to see what Python/R libraries are pre-installed, and learn about performance improvements and new functionality.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Spark Session 
# MAGIC If you have set up Spark before, you have likely seen a similar command: `spark = SparkSession.builder.getOrCreate()`
# MAGIC 
# MAGIC In Databricks notebooks, each notebook **automatically** has a Spark context setup and ready after attaching to a cluster. Simply call the `spark` variable in Python to start interacting with Pyspark!

# COMMAND ----------

spark

# COMMAND ----------

# MAGIC %md
# MAGIC Different notebooks have different Spark contexts by default. If you are sharing a cluster for development purposes with your colleauges, you do not need to worry about conflicting Spark sessions.
# MAGIC 
# MAGIC You can also **view** and **set** Spark configs for your current session directly from a notebook. Let's try it with a common config, the number of "shuffle partitions" used by our Spark jobs (`spark.sql.shuffle.partitions`):

# COMMAND ----------

spark.conf.get("spark.sql.shuffle.partitions") #Get the default value

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions",360) #Set the new value

# COMMAND ----------

spark.conf.get("spark.sql.shuffle.partitions") #Get the new value after setting

# COMMAND ----------

# MAGIC %md
# MAGIC **Side Note:** Spark configs are very important to get the most out of Spark for big data processing. Although the defaults are a best attempt, you can drastically improve performance by tweaking these based on your data, cluster, and computing environment.
# MAGIC 
# MAGIC If you want to learn more, check out available Spark Tuning courses on academy.databricks.com

# COMMAND ----------

# MAGIC %md
# MAGIC # Interacting with Data

# COMMAND ----------

# MAGIC %md
# MAGIC ## Understanding Cloud-based filepaths
# MAGIC Similar to how you might interact with a filepath when working locally (e.g. `C:/...`), cloud-based storage systems provide similar functionality. These paths are often found in the cloud provider's console or provided to you by an admin or data engineer. For example, this path may start with `abfss:/` on Azure ADLS Gen 2 storage, or `s3:/` on AWS S3 storage.
# MAGIC 
# MAGIC ### DBFS
# MAGIC In addition to these cloud storage locations, **every Databricks workspace has a `dbfs:/` path**. This is the "Databricks Files System", and acts as a "default" location for working files (and is actually backed up by the same cloud storage as referenced above). You can use this for development purposes and shared file assets, but **DO NOT use DBFS for Production data** - instead, save it to a cloud storage path (e.g. ADLS or S3) where you can properly manage access and monitor. 

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/databricks-datasets"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Loading Data
# MAGIC In Pyspark, we can load data using the Spark DataFrame "Reader" methods. For the rest of the notebook, we will use one of the Databricks Datasets that comes loaded with every workspace. 
# MAGIC 
# MAGIC [pyspark.sql.DataFrameReader](https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrameReader)
# MAGIC 
# MAGIC Common reader methods:
# MAGIC * `csv`, `text`, `delta`
# MAGIC * Example: `spark.read.format('json').load('<path>')`

# COMMAND ----------

dbutils.fs.ls("dbfs:/databricks-datasets/wine-quality")

# COMMAND ----------

wine_wrong = spark.read.csv("dbfs:/databricks-datasets/wine-quality/winequality-red.csv")
display(wine_wrong)

# COMMAND ----------

# MAGIC %md
# MAGIC Uh-oh! We see this file has a header-row (e.g. Row #1) and is separated by `;` instead of commas. We can modify the CSV DataFrameReader to account for this. Add `header=True` and `sep=';'`

# COMMAND ----------

wine = spark.read.csv("dbfs:/databricks-datasets/wine-quality/winequality-red.csv", <TODO>)
display(wine)

# COMMAND ----------

# MAGIC %md 
# MAGIC Note that wrapping a DataFrame (or any tabular object) in `display()` will make it easier to explore and interactive than the standard Spark `.show()` method:

# COMMAND ----------

wine.show()

# COMMAND ----------

# MAGIC %md
# MAGIC We now have a Spark DataFrame that we can use in PySpark!

# COMMAND ----------

# MAGIC %md
# MAGIC ## Saving Data
# MAGIC While you may be familiar with some common data formats (`.csv`, `.txt`, etc.), **not all file formats are equivalent!** Keep in mind these factors: 
# MAGIC 
# MAGIC * **R - Row or Column Store **: Column-based formats allow more data to be skipped and are generally faster.
# MAGIC * **C - Compression **: Compressing files saves time and money during data transmission and storage.
# MAGIC * **E - Schema Evolution **: Files that are self-describing keep the schema in the same place as the data (e.g. column names, data types) and can evolve over time as new columns are added.
# MAGIC * **S - Splitability **: Files that can be broken down into smaller chunks can be processed in parallel by multiple machines, leading to better performance.
# MAGIC 
# MAGIC Databricks open-sourced [Delta Lake](https://databricks.com/product/delta-lake-on-databricks), a "format" that brings reliability, performance, and lifecycle management to data lakes. We recommend using Delta as your file format when working with Spark. This can be specified in read/write operations with: `format('delta')`
# MAGIC 
# MAGIC We will take the above data and save to a Delta table. But first, we must use a very common Pyspark method called `withColumnRenamed()` to remove spaces in the column headers:

# COMMAND ----------

# We could do something like this, replacing each colum name 1-by-1...
wine_slow = wine.withColumnRenamed("fixed acidity","fixed_acidity")
display(wine_slow)

# COMMAND ----------

# Or we can mix our Python knowledge with Spark to improve efficiency!
new_names = [column.replace(" ","_") for column in wine.columns]
new_wine = wine.toDF(*new_names)
display(new_wine)

# COMMAND ----------

# MAGIC %md
# MAGIC Let's practice by saving this file to a temporary DBFS location. 

# COMMAND ----------

#Extract the current users name from email address
user=dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user').split("@")[0].split(".")[0]
#Build filepath for temporary saving of data; we will clean this up later
save_path = "dbfs:/tmp/"+user

new_wine.write.format("delta").mode("overwrite").save(save_path)

# COMMAND ----------

# MAGIC %md
# MAGIC Note the use of `mode("overwrite")`. This will "overwrite" the existing table at that location. Supported write modes:
# MAGIC 
# MAGIC * `append`: Append contents of this DataFrame to existing data.
# MAGIC 
# MAGIC * `overwrite`: Overwrite existing data.
# MAGIC 
# MAGIC * `error` or `errorifexists`: Throw an exception if data already exists.
# MAGIC 
# MAGIC * `ignore`: Silently ignore this operation if data already exists.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise: List out filepath
# MAGIC Use the variable `save_path` and `dbutils` to look at your actual files in cloud storage.

# COMMAND ----------

dbutils.<TODO>

# COMMAND ----------

# MAGIC %md
# MAGIC ## (aside) "Mounts"/mnt
# MAGIC A Databricks "Mount" (aka `mnt`) is simply a shortcut to a cloud storage path. Instead of referencing a very long and complex file path on cloud storage, you simply call the mount. For example:
# MAGIC 
# MAGIC Instead of:
# MAGIC > `spark.read.csv("adl://<myadlsfolder>.azuredatalakestore.net/Path/To/My/Very/Nested/Files/MyData.csv")`
# MAGIC 
# MAGIC You can mount that location started at `Files`, then refer to it simply by:
# MAGIC > `spark.read.csv("/mnt/MyData.csv")`
# MAGIC 
# MAGIC To learn more about Mounts, refer here: [Databrick File System](https://docs.databricks.com/data/databricks-file-system.html). A workspace administrator will typically be required to setup mounts. 

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Manipulation Basics

# COMMAND ----------

# MAGIC %md
# MAGIC ## DataFrames Intros
# MAGIC Thinking tabular: if you have never worked with "dataframes" or tabular data, it helps to have a mental model. Think **rows** and **columns**!
# MAGIC 
# MAGIC ![tabular data](http://exceltutorialworld.com/wp-content/uploads/2017/10/Capture-30.png)
# MAGIC 
# MAGIC This is an Excel file, but many datasets (both relational and otherwise) can be thought of as tabular
# MAGIC 
# MAGIC If you have hear of Spark before, you may have heard of RDDs, or "Resilient distributed datasets". While these were important for early versions of Spark, almost all development now happens on Spark DataFrames, a higher-level abstraction of RDDs. **You should focus your learning efforts on the DataFrame APIs**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Differences between SQL and PySpark 
# MAGIC SQL is a syntax; PySpark is an interface to the Spark engine. 
# MAGIC 
# MAGIC Do you prefer SQL instead? Great! In Databricks you can complete many of the same operations with `%sql SELECT * FROM...` as you can on a traditional query tool. In Pyspark, you can also use `spark.sql("SELECT * FROM...)"` for a SQL-like interface from Python. 
# MAGIC 
# MAGIC Fun fact: performance between SQL, Pyspark, and Scala Spark are nearly identical in most circumstances. They all compile to the same thing!
# MAGIC ![DataFrame comparison](https://www.oreilly.com/library/view/learning-pyspark/9781786463708/graphics/B05793_03_03.jpg)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Differences between Pandas and Spark DataFrames
# MAGIC Many data practitioners start working with tabular data in Pandas, a popular Python library. Let's compare to PySpark:
# MAGIC 
# MAGIC 
# MAGIC |                | pandas DataFrame                  | Spark DataFrame                                                     |
# MAGIC | -------------- | --------------------------------- | ------------------------------------------------------------------- |
# MAGIC | Computation    | Eager                             | Lazy                                                                |
# MAGIC | Column         | df\['col'\]                       | df\['col'\]                                                         |
# MAGIC | Mutability     | Mutable                           | Immutable                                                           |
# MAGIC | Add a column   | df\['c'\] = df\['a'\] + df\['b'\] | df.withColumn('c', df\['a'\] + df\['b'\])                           |
# MAGIC | Rename columns | df.columns = \['a','b'\]          | df.select(df\['c1'\].alias('a'), df\['c2'\].alias('b'))             |
# MAGIC | Value count    | df\['col'\].value\_counts()       | df.groupBy(df\['col'\]).count().orderBy('count', ascending = False) |

# COMMAND ----------

# MAGIC %md
# MAGIC ## (aside) Koalas for Pandas on Spark ![Koalas](https://koalas.readthedocs.io/en/latest/_static/koalas-logo-docs.png)
# MAGIC Koalas is a "pandas-like" library to interact with data. It uses Spark as a backend, allowing massive datasets to be processed but with the convenience of **pandas** APIs. To learn more: https://koalas.readthedocs.io/en/latest/index.html

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transformations
# MAGIC To get started, we will do a few aggregations on our wine DataFrame. For consistency, let's load from our save path:

# COMMAND ----------

#Uncomment and re-run to create the path if needed
#user=dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user').split("@")[0].split(".")[0]
#save_path = "dbfs:/tmp/"+user

data = spark.read.format("delta").load(save_path)
display(data)

# COMMAND ----------

# MAGIC %md
# MAGIC Let's get a count of the number of the number of records for each `quality` value. We use the `groupBy()` method to return a grouped dataframe, then `count()` to simply count the number of rows in each group. 

# COMMAND ----------

display(data.groupBy("quality").count())

# COMMAND ----------

# MAGIC %md
# MAGIC However, if we look at the schema of our original DataFrame, we see that all the columns have been created as `string` types:

# COMMAND ----------

data.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC **Takeaway: always consider the data types you are operating on in Spark** 
# MAGIC 
# MAGIC To do further aggregations, let's build a quick function to convert every column to a numeric value of types `double`:

# COMMAND ----------

from pyspark.sql.functions import col
def cast_all_to_double(input_df):
  return input_df.select([col(col_name).cast("double") for col_name in input_df.columns])

data_num = data.transform(cast_all_to_double)
display(data_num)

# COMMAND ----------

# MAGIC %md
# MAGIC **Takeway: you may need to import additional functions (e.g. `pyspark.sql.functions import col`) for certain DataFrame operations.**
# MAGIC 
# MAGIC Now it is your turn. 
# MAGIC 
# MAGIC ### Exercise: Calculate the Average value of `pH` and `chlorides` by wine quality. 
# MAGIC Refer to Pyspark documentation to determine syntax: [Pyspark Sql Docs](https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#module-pyspark.sql.functions)
# MAGIC 
# MAGIC *Bonus: show the result in a single Dataframe, and rename the resulting columns as `avg_pH` and `avg_chlorides`.*

# COMMAND ----------

data_num.<TODO>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Spark Functions
# MAGIC If at all possible, it is best practice to leverage existing PySpark functions rather than writing your own. Each PySpark function is written to be highly parallelized and efficient. Any function written in generic Python will likely not be as performant, especially on large datasets. 
# MAGIC 
# MAGIC Common built-in functions:
# MAGIC * [Data type/conversion functions](https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#module-pyspark.sql.types)
# MAGIC * [Date time functions](https://docs.databricks.com/spark/latest/dataframes-datasets/dates-timestamps.html)
# MAGIC * [Math functions](https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.functions.sqrt) (e.g. square root)
# MAGIC * [Aggregate functions](https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#module-pyspark.sql.functions)
# MAGIC * [Data cleansing functions](https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrameNaFunctions) (e.g. remove nulls)
# MAGIC 
# MAGIC One option to parallelize functions that do not exist in PySpark are called Pandas_UDFs. These use efficient serialization techniques to parallelize processing across a Spark cluster. [Learn more here](https://databricks.com/blog/2017/10/30/introducing-vectorized-udfs-for-pyspark.html)

# COMMAND ----------

# MAGIC %md
# MAGIC # Automation & Monitoring

# COMMAND ----------

# MAGIC %md
# MAGIC ## DBU Types
# MAGIC 
# MAGIC When working in Databricks it is important to consider the **cluster type** you are working on: 
# MAGIC * `Interactive`: "submit a command, get an immediate response" for interactive development purposes. Charged at a *higher* DBU rate. This is what we're doing when working in this notebook!
# MAGIC * `Automated`: "run this code with no human in the loop" for recurring job runs. Charged at a *lower* DBU rate. 
# MAGIC 
# MAGIC In general, it is a best practice to first develop your code on `Interactive` clusters, then move them as recurring scheduled jobs to `Automated` clusters. To do this, you can scheduel directly from your Notebooks using the `Schedule` button at top-right, or use the `Jobs` tab on the left. 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Jobs Tab
# MAGIC When moving workloads to an `Automated` cluster, you can configure several parameters. Remember that an `Automated` cluster is "ephemeral", just to run your bit of code. This is a highly efficient way to rent cloud computing resources, and you have full control over the exact cluster parameters and configuration to get the best performance. 
# MAGIC 
# MAGIC * **Schedule**: on what recurring schedule should the code execute, using a simple schedule UI or CRON syntax.
# MAGIC * **Notifications**: set email alerts for job starting, completing, or errors
# MAGIC * **Virtual Machine types**: set cluster compute resources and determine the right hardware for your job
# MAGIC * **Autoscaling parameters**: you can select to allow the cluster to "autoscale" up and down depending on the amount of Spark tasks waiting
# MAGIC * **Logs**: after each run of a job, the Driver, Spark, and any custom logs are available to refer back to later
# MAGIC * **Initialization scripts**: a.k.a "init scripts", these allow you to highly customize your runtime environment with bash scripting
# MAGIC * Many more settings, including permissions, Container services, and runtimes...

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cost Efficiency Considerations
# MAGIC In addition to the parameters for a single "job", your team can achieve cost efficiency goals with several additional Databricks features:
# MAGIC 
# MAGIC * **Instance Pools**: clusters can pull from a "pool" of warm instances, drastically reducing cluster launch and scaling times.
# MAGIC * **High Concurrency Clusters**: multiple users can efficiently "share" cluster compute resources with features like task pre-emption and increased session isolation
# MAGIC * **Autoscaling**: Databricks has proprietary features that will autoscale clusters up and down depending on the work waiting to be completed
# MAGIC * **Autotermination**: clusters with no activity will self-terminate after a period of time. 
# MAGIC * **Tagging**: assign cluster "tags" that propagate back to reporting on the cloud provider to monitor and manage costs

# COMMAND ----------

# MAGIC %md
# MAGIC # Cleanup

# COMMAND ----------

# MAGIC %md
# MAGIC Thanks for going through this notebook! To learn more, be sure to check out customer training available at [Databricks Academy](https://academy.databricks.com/pathway/INT-AL-FREE-SP)!
# MAGIC Coupon Code: `DB_CE` at checkout

# COMMAND ----------

#File cleanup of Wine Delta table created
dbutils.fs.rm(save_path, recurse=True)
