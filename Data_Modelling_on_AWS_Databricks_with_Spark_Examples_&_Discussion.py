# Databricks notebook source

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType

spark = SparkSession.builder.master("local[*]") \
                    .appName('Data Modelling on AWS Databricks with Spark') \
                    .getOrCreate()

employee_data2 = [("Emmanuel", "Oyekanlu", "6111876", "M", 33,  237000,  "manuelbomi@yahoo.com", 11, "640-137-0000"),
                  ("Don", "Coder", "387654", "M", 30,  210000,  "python-Coder@gmail.com", 12, "540-543-3245"), 
                  ("Henry", "Charles", "3000127", "M", 42,  210000,  "massie@yahoo.com", 7, "127-196-5676"),
                  ("Stephen", "Smith", "9087655", "M", 38,  156000,  "miutss@karen.com", 5, "234-112-9812"),
                  ("Rose", "CarlyWiggle", "5609876", "F", 24,  237000,  "iutyrr@yahoo.com", 18, "876-137-0119"),
                  ("Diddier", "Thomas", "6347652", "M", 53,  237000,  "potyur@yahoo.com", 19, "653-239-9876"),
                  ("Carla", "Fisher", "9871234", "F", 28,  121000,  "tuyinmg@yahoo.com", 3, "555-666-9876"),
                  ("Yinka", "Eromonsele", "547863", "F", 29,  99500,  "eromonsele@yahoo.com", 12, "652-653-0987"),
                  ("Rod", "BiggerStewart", "698328", "M", 54,  76500,  "BiggerS@yahoo.com", 12, "640-137-0000"),
                  ("Oliver", "Twist", "7652423", "M", 33,  200000, "Twister@yahoo.com", 7, "764-129-9009"),
                  ("Moses", "Aaron", "9876543", "M", 23,  186000,  "Moses@cnn.com", 6, "456-987-2324"),
                  ("Molly", "Van Modeller", "6487653", "F", 39,  232000,  "preacher@yahoo.com", 8, "765-986-2345"),
                  ("Barry", "TightFisted", "7864556", "M", 38,  115000,  "boxer@yahoo.com", 5, "569-432-7654"),
                  ("Ken", "Chang", "9845376", "M", 26,  105890,  "bongbonyahoo.com", 10, "434-987-1200"),
                  ("Alhaji", "Kareem", "87565234", "M", 44,  65000,  "uytrew@yahoo.com", 9, "543-210-3400"),
                  ("Islam", "Aboubacar", "8719865", "M", 32,  186100,  "westerm@yahoo.com", 4, "540-872-1000"),
                  ("Meghan", "Markle", "7645348", "M", 44,  91000,  "Missyr@yahoo.com", 2, "569-349-1200")
                  ]


schema_employee = StructType([ \
    StructField("First_name",    StringType(), True), \
    StructField("Last_name",     StringType(), True), \
    StructField("Employee_ID",   StringType(), True), \
    StructField("Gender",        StringType(), True), \
    StructField("Age",           StringType(), True), \
    StructField("Salary_(USD)",     IntegerType(), True), \
    StructField("email_address",        StringType(), True), \
    StructField("Experience_(yrs)",  StringType(), True), \
    StructField("Phone_nos",           StringType(), True), \
                                                                             
])

df_employee = spark.createDataFrame(data=employee_data2 ,schema=schema_employee)
df_employee.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Creating Spark Dataframes
# MAGIC Pyspark DataFrames are generally preferred over RDDs for most data processing tasks because of High-level abstraction, Optimization, Structured API, Schema inference and enforcement, and Integration with Spark SQL, and support for various data source
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Using CreateOrReplaceTempView
# MAGIC
# MAGIC One of the main advantages of Apache PySpark is working with SQL along with DataFrame/Dataset API. So, if an engineer is comfortable with SQL, he/she can create a temporary view on DataFrame/Dataset by using createOrReplaceTempView() and using SQL to select and manipulate the data.
# MAGIC
# MAGIC createOrReplaceTempView creates (or replaces if that view name already exists) a lazily evaluated "view" that you can then # use like a hive table in Spark SQL. It does not persist to memory unless you cache the dataset that underpins the view.
# MAGIC

# COMMAND ----------

df_employee.createOrReplaceTempView("employee_data_table_name")


# COMMAND ----------

# View all data columns from temp view
spark.sql("show columns from employee_data_table_name").display()

# COMMAND ----------


# Run SQL Query
spark.sql("select First_Name, Last_Name from employee_data_table_name").display()


# COMMAND ----------

# MAGIC %md
# MAGIC If you want to have a temporary view that is shared among all sessions and keep alive until the PySpark application terminates, you can create a global temporary view using createGlobalTempView()

# COMMAND ----------

# Create a Global temp view and readh some data from it
# df_employee.createGlobalTempView("employee_GlobalViewdata_table_name")
# spark.sql("select Last_Name, Age from employee_GlobalViewdata_table_name").display()


# COMMAND ----------

# MAGIC %md
# MAGIC PySpark cache()
# MAGIC
# MAGIC Using the PySpark cache() method we can cache the results of transformations. Unlike persist(), cache() has no arguments to specify the storage levels because it stores in-memory only. Persist with storage-level as MEMORY-ONLY is equal to cache().
# MAGIC
# MAGIC
# MAGIC # Syntax
# MAGIC DataFrame.cache()
# MAGIC

# COMMAND ----------

# Get only male employee data and cache it
df_employee_male_only = df_employee.where(col("Gender") =="M").cache()

# Get the count of the male employees from the cached data
count_male = df_employee_male_only.count()
count_male



# COMMAND ----------

# Get the salary details of male employees from the cached data
df_employee_male_only = df_employee_male_only.where(col("Salary_(USD)") >= 100000)
count_salary = df_employee_male_only.count()
count_salary

# COMMAND ----------

df_employee_male_only.display()

# COMMAND ----------

# another employee data
another_employee_data = [("Casey", "Donner", "1876", "M", 44,  237000,  "maryl@yahoo.com", 2, "819-137-060"),
                  ("MariGold", "Joelyn", "387654", "F", 32,  219870,  "eryl@gmail.com", 34, "540-543-3785"), 
                  ("Katie", "Henshaw", "304127", "F", 39,  20340,  "Kahenshaw@yahoo.com", 5, "497-196-476"),
                  ("Derrick", "Smith", "87655", "M", 38,  100000,  "gh@karen.com", 5, "674-112-962"),
                  ("Debbie", "Aaron", "5667876", "F", 27,  115000,  "morel@yahoo.com", 34, "496-47-7919"),
                  ("Rex", "Migler", "6647652", "M", 23,  567000,  "pouhgr@yahoo.com", 56, "883-239-7776"),
                 
                  ]


schema_employee = StructType([ \
    StructField("First_name",    StringType(), True), \
    StructField("Last_name",     StringType(), True), \
    StructField("Employee_ID",   StringType(), True), \
    StructField("Gender",        StringType(), True), \
    StructField("Age",           StringType(), True), \
    StructField("Salary_(USD)",     IntegerType(), True), \
    StructField("email_address",        StringType(), True), \
    StructField("Experience_(yrs)",  StringType(), True), \
    StructField("Phone_nos",           StringType(), True), \
                                                                             
])

another_employee_data = spark.createDataFrame(data=another_employee_data ,schema=schema_employee)
another_employee_data.printSchema()

# COMMAND ----------

another_employee_data.display()

# COMMAND ----------

df_employee.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC Using User-Defined Functions (UDFs)

# COMMAND ----------

## Using User Defined Function
# Define some arbitrary function that will be used to encode a new column
# from pyspark.sql.functions import udf

def encode_employee_gender_age(gender, age):
    
    if gender == "M":
        return 20
    elif gender == "F":
        return 30
    elif age < 20 :
        return 40
    else:
        return 0
# Convert to udf function and model the new column as an integer type column
function_with_udf = udf(f= encode_employee_gender_age, returnType= IntegerType())
# Create new column
updated_employee_data = df_employee.withColumn("another_column_using_udf", 
                                             function_with_udf(df_employee["Gender"], 
                                                               df_employee["Age"]))

updated_employee_data.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Sorting, Ordering and Joining Data

# COMMAND ----------

# Sort the data with using a colum of the data
employee_data_sorted_df = df_employee.orderBy(["Age"])
employee_data_sorted_df.display()

# COMMAND ----------

# Join two datasets (Inner Join)
# Inner join
inner_join_df = df_employee.join(another_employee_data, 'Age')
inner_join_df.display()

# COMMAND ----------

# Right outer join on 'Last Name'
right_outer_join_df = df_employee.join(another_employee_data, 'Last_Name', 'rightouter')
right_outer_join_df.display()

# COMMAND ----------

# left outer join on 'Last Name'
left_outer_join_df = df_employee.join(another_employee_data, 'Last_Name', 'leftouter')
left_outer_join_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Collating/Imploding Multiple Data Columns into a Single Column

# COMMAND ----------

# Convert columns to Map
# Merge several columns to become a single column
# Merge the 'Salary, Experience, Age' colums to become a single column "EmployeeDetails"
from pyspark.sql.functions import col,lit,create_map
df_employee = df_employee.withColumn("EmployeesDetails",create_map(
        lit("salary"),col("Salary_(USD)"),
        lit("experience"),col("Experience_(yrs)"),
        lit("age"),col("Age")
        )).drop("Salary_(USD)","Experience_(yrs)", "Age")
df_employee.printSchema()



# COMMAND ----------

df_employee.show(truncate=False)

# COMMAND ----------

# spark.stop()