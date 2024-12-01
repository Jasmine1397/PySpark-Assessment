# Databricks notebook source
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("Assessment").getOrCreate()

# COMMAND ----------




employees_ = [
    (1, 'John', 34, 'IT', 75000, '2015-6-1'),
    (2, 'Sara', 28, 'HR', 58000, '2019-9-15'),
    (3, 'Michael', 45, 'Finance', 120000, '2010-1-10'),
    (4, 'Karen', 29, 'IT', 70000, '2020-2-19'),
    (5, 'David', 38, 'Finance', 90000, '2017-8-23'),
    (6, 'Linda', 33, 'HR', 60000, '2018-12-5'),
    (7, 'James', 41, 'IT', 110000, '2013-4-15'),
    (8, 'Emily', 27, 'HR', 52000, '2021-6-20'),
    (9, 'Robert', 36, 'Finance', 105000, '2016-11-30')
]

cols = ['id','name','age','department','salary','join_date']

df = spark.createDataFrame(data= employees_, schema= cols)

df.display()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **1. Display records of employees aged above 30.**
# MAGIC
# MAGIC **Use the DataFrame filter transformation to extract employees where age > 30.**
# MAGIC
# MAGIC

# COMMAND ----------

age_above_thirty = df.filter(col('age') > 30).orderBy(col('age').desc())

age_above_thirty.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **2. Find the average salary of employees in each department.**
# MAGIC
# MAGIC **Use the groupBy transformation on the department column and calculate the average salary.**
# MAGIC
# MAGIC

# COMMAND ----------

avg_salary_by_department = (
    df.groupBy(col('department'))
    .agg(round(avg('salary'), 2).alias('average_salary'))
)

avg_salary_by_department.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **3. Add a column experience indicating the number of years an employee has been working.**
# MAGIC
# MAGIC **Derive this using the join_date column and the current date.**
# MAGIC
# MAGIC

# COMMAND ----------

df_with_experience = df.withColumn('experience', floor(datediff(current_date(), col('join_date'))/365))

df_with_experience.display()


# COMMAND ----------

# MAGIC %md
# MAGIC **4. Find the top 3 highest-paid employees.**
# MAGIC
# MAGIC **Sort the DataFrame in descending order by salary and use the limit action.**
# MAGIC
# MAGIC

# COMMAND ----------

highest_paid_employees = df.sort(col('salary').desc()).limit(3)

highest_paid_employees.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **5. Identify the department with the highest total salary.**
# MAGIC
# MAGIC **Aggregate the salary data by department and find the department with the maximum total.**
# MAGIC
# MAGIC

# COMMAND ----------

total_salary_by_department = df.groupBy(col('department')).agg(max('salary').alias('Total_salary')).orderBy(col('Total_salary').desc()).limit(1)

total_salary_by_department.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **6. Create a new DataFrame with employees earning more than the average salary.**
# MAGIC
# MAGIC **First, calculate the average salary, then filter employees earning above that average.**
# MAGIC
# MAGIC

# COMMAND ----------

# Compute the average salary
avg_salary = df.agg(round(avg('salary'),2).alias('AVG_Salary'))

# Join the average salary with the original DataFrame
more_than_avg_salary = df.join(avg_salary).filter(col('salary') > col('AVG_Salary'))

# Display the result
more_than_avg_salary.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **7. Rename the column name to employee_name.**
# MAGIC
# MAGIC **Use the withColumnRenamed transformation.**
# MAGIC
# MAGIC

# COMMAND ----------

col_rename= df.withColumnRenamed('name', 'employee_name')

col_rename.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **8. Find the number of employees in each department.**
# MAGIC
# MAGIC **Use groupBy and count transformations.**
# MAGIC

# COMMAND ----------

count_of_employees = df.groupBy(col('department')).agg(count('id').alias('Count_of_Employees'))

count_of_employees.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **9. Select and display only the id and name columns.**
# MAGIC
# MAGIC **Use the select transformation to project only specific columns.**
# MAGIC
# MAGIC

# COMMAND ----------

id_name = df.select('id', 'name')

id_name.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **10. Check for any null values in the dataset.**
# MAGIC
# MAGIC **Use the isNull method to check for nulls across columns.**

# COMMAND ----------

null_check = df.select([col(c).isNull().alias(c) for c in df.columns])

null_check.display()
