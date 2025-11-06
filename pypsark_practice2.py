import wget

import findspark
findspark.init()

# PySpark is the Spark API for Python. In this lab, we use PySpark to initialize the SparkContext.   
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, IntegerType, StructField, StringType
from pyspark.sql.functions import col, avg, sum, count, max

# Creating a SparkContext object  
sc = SparkContext.getOrCreate()

# Creating a SparkSession  
spark = SparkSession \
    .builder \
    .appName("Python Spark DataFrames basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

#wget.download("https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-BD0225EN-SkillsNetwork/data/employees.csv")

emp_df = spark.read.csv('employees.csv', header=True)

# Task 2.1: Define a schema for the data
schema = StructType([
StructField('Emp_No', IntegerType(), False),
StructField('Emp_Name', StringType(), False),
StructField('Salary', IntegerType(), False),
StructField('Age', IntegerType(), False),
StructField('Department', StringType(),False)
])

# Task 2.2: Read the data with the created schema
employees_df = (spark.read\
                .format('csv')\
                .schema(schema)\
                .option('header', 'true')\
                .load('employees.csv')
)

# Task 3: display the df squema
employees_df.printSchema()

# Task 4: Create a temporary view
spark.sql("DROP VIEW IF EXISTS employees")
employees = employees_df.createTempView('employees')

# Task 5: Perform an SQL query and display it
query1 = spark.sql("SELECT * FROM employees WHERE Age > 30")
query1.show()

# Task 6: Calculate Average Salary by Department
query2 = spark.sql('SELECT Department, AVG(Salary) AS AVG_Salary_By_DPT FROM employees GROUP BY Department')
query2.show()

# Task 7 : Filter and Display IT Department Employees
filtered_df = employees_df.filter(employees_df['Department'] == 'IT')
filtered_df.show()

# Task 8: Add 10% Bonus to Salaries
'''Perform a transformation to add a new column named "SalaryAfterBonus" to the DataFrame. Calculate the new salary by adding a 10% bonus to each employee's salary.'''
employees_df = employees_df.withColumn('SalaryAfterBonus',1.1*employees_df['Salary'])
employees_df.show(10)

# Task 9: Find Maximum Salary by Age
# Group data by age and calculate the maximum salary for each age group
max_salary_by_age = employees_df.groupBy('Age').agg(max('SalaryAfterBonus')).orderBy('Age')
max_salary_by_age.show()

# Task 10: Self-Join on Employee Data
# Join the DataFrame with itself based on the "Emp_No" column
employees_df2 = employees_df
employees_df2 = employees_df2.join(employees_df2,'Emp_No', 'inner')
employees_df2.show()

# Task 11: Calculate Average Employee Age
avg_emp_age = employees_df.agg(avg(employees_df["Age"]))
avg_emp_age.show()

# Task 12: Calculate Total Salary by Department
# Calculate the total salary for each department. Hint - User GroupBy and Aggregate functions
total_salary_DPT = employees_df.groupBy('Department').agg(sum(employees_df["Salary"]).alias('Total_salary_by_DPT'))
total_salary_DPT.show()

# Task 13: Sort Data by Age and Salary
'''Apply a transformation to sort the DataFrame by age in ascending order and then by salary in descending order. Display the sorted DataFrame.'''
# Sort the DataFrame by age in ascending order and then by salary in descending order
employees_df.orderBy('Age').show()
employees_df.orderBy('Salary', ascending = False).show()

# Task 14: Count Employees in Each Department
'''Calculate the number of employees in each department. Display the result.'''
count_emp = employees_df.groupBy('Department').agg(count('Emp_No'))
count_emp.show()

# Task 15: Filter Employees with the letter o in the Name
'''Apply a filter to select records where the employee's name contains the letter 'o'. Display the filtered DataFrame.'''
new_df = employees_df.filter(col("Emp_Name").contains("o"))
new_df.show()

# Terminate spark session
spark.stop() 
