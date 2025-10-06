from pyspark.sql import SparkSession
# Must set this env variable to avoid warnings
import os
os.environ['PYARROW_IGNORE_TIMEZONE'] = '1'
import pyspark.pandas as ps  # Import pandas-on-Spark

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Transform and Apply in Pandas API on Spark") \
    .config("spark.sql.ansi.enabled", "false") \
    .config("spark.executorEnv.PYARROW_IGNORE_TIMEZONE", "1") \
    .getOrCreate()

# Create a pandas-on-Spark DataFrame
ps_df = ps.DataFrame({
    "id": [1, 2, 3, 4, 5],
    "name": ["Alice", "Bob", "Charlie", "David", "Emma"],
    "age": [25, 30, 35, 40, 45],
    "salary": [50000, 60000, 75000, 80000, 120000]
})

print("Original Pandas-on-Spark DataFrame:")
print(ps_df)

# 1. Using `transform()` for element-wise operations
ps_df["age_in_10_years"] = ps_df["age"].transform(lambda x: x + 10)
print("\nDataFrame after transform (age + 10 years):")
print(ps_df)

# 2. Using `apply()` on columns
# Define a custom function to categorize salary levels
def categorize_salary(salary):
    if salary < 60000:
        return "Low"
    elif salary < 100000:
        return "Medium"
    else:
        return "High"

# Apply the function to the 'salary' column
ps_df["salary_category"] = ps_df["salary"].apply(categorize_salary)
print("\nDataFrame after apply (Salary Category):")
print(ps_df)

# 3. Using `apply()` on rows
# Define a function to format name and age
def format_row(row):
    return f"{row['name']} ({row['age']} years old)"

# Apply the function across rows
ps_df["name_with_age"] = ps_df.apply(format_row, axis=1)
print("\nDataFrame after apply on rows (name_with_age):")
print(ps_df)

# Stop Spark session
spark.stop()
