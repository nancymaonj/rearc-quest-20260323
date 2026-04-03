# aws s3 cp s3://rearc-quest-20260323-876784288665-us-east-1-an/quest-part-2 C:\Users\user2026\Downloads --recursive
# Load both the csv file from Part 1 C:\Users\user2026\vscode-2026\20260323\rearc-quest-20260323\bls_pr_data\pr.data.0.Current 
# and the C:\Users\user2026\vscode-2026\20260323\rearc-quest-20260323\quest-part-2 file from Part 2 as dataframes 
# (Spark, Pyspark, Pandas, Koalas, etc).
# (.venv) pip install notebook
# pip install python-dotenv
# .venv) C:\Users\user2026\vscode-2026\20260323\rearc-quest-20260323>jupyter notebook

# quest-3-1
# 1. use pandas library, write a python script which Load below data as dataframes and show top 5 row of each dataframe.
# 2. file https://download.bls.gov/pub/time.series/pr/pr.data.0.Current and 
# 3. data json file population_data.json as below 
# ** change vscode terminal to cmd instead of powershell to run the script, and install pandas and requests library if you haven't already. You can do this by running `pip install pandas requests` in the terminal. **
 
# quest-3-2
# Using the below json which take "data" and load into a dataframe , 
# generate the mean and the standard deviation of the annual US population across the years [2013, 2018] inclusive.

# quest-3-3
# a python script Using  the dataframe from https://download.bls.gov/pub/time.series/pr/pr.series 
# and the dataframe from https://download.bls.gov/pub/time.series/pr/pr.data.0.Current,
# For every series_id, find the best year: the year with the max/largest sum of "value" for all quarters in that year. 
# Generate a report with each series id, the best year for that series, and the summed value for that year. 

# quest-3-4
# no need - Using both dataframes from https://download.bls.gov/pub/time.series/pr/pr.series 
# and the dataframe from https://download.bls.gov/pub/time.series/pr/pr.data.0.Current,
# and population from below json, 
# write a python function that generate a report that will provide the value for series_id = X and period = Y and year=Z the population 
# for that given year (if available in the population dataset). 


import pandas as pd
import json
import requests
from io import StringIO

import os
from dotenv import load_dotenv

# Load variables from .env file
load_dotenv()
S3 = os.getenv("S3")
HOMELOCAL = os.getenv("HOMELOCAL")
print(S3, HOMELOCAL)

# -----------------------------
# quest-3-1
# -----------------------------

# Load BLS data from the file downloaded from S3
full_path = f"{HOMELOCAL}\\vscode-2026\\20260323\\rearc-quest-20260323\\bls_pr_data\\pr.data.0.Current"
df_bls = pd.read_csv(full_path, sep='\t')

print("BLS Data (Top 5 rows):")
print(df_bls.head(5))
print("\n")


# Convert JSON 'data' section into DataFrame
with open(f"{HOMELOCAL}\\vscode-2026\\20260323\\rearc-quest-20260323\\quest-part-2\\population_data.json", 'r') as f:
    population = json.load(f)
df_population = pd.json_normalize(population["data"])
print("Population Data (Top 5 rows):")
print(df_population.head(15))


# -----------------------------
# quest-3-2
# -----------------------------

# Filter for years 2013–2018
df_filtered = df_population[(df_population["Year"] >= 2013) & (df_population["Year"] <= 2018)]

# Calculate mean and standard deviation
mean_population = df_filtered["Population"].mean()
std_population = df_filtered["Population"].std()

print("Mean Population:", mean_population)
print("Standard Deviation:", std_population)




# -----------------------------
# quest-3-3
# -----------------------------

# Load series datasets
full_path = f"{HOMELOCAL}\\vscode-2026\\20260323\\rearc-quest-20260323\\bls_pr_data\\pr.series"
df_series = pd.read_csv(full_path, sep='\t')
df_series.columns = df_series.columns.str.strip()

# Keep only quarterly periods (Q01–Q04)
df_bls.columns = df_bls.columns.str.strip()
df_bls = df_bls[df_bls["period"].str.startswith("Q")]
print(df_bls.head)

# Convert value to numeric
df_bls["value"] = pd.to_numeric(df_bls["value"], errors="coerce")
# print(
#     df_bls.loc[
#         # (df_bls["best_year"] == 2000) & (df_bls["series_id"] == "PRS30006012")
#         # (df_bls["year"] == 2000) & ( df_bls["series_id"] == "PRS30006011")
#         (df_bls["year"] == 2000) &  df_bls["series_id"].str.contains("PRS30006011")
#     ]
# )


# Aggregate: sum per year per series_id
df_yearly = (
    df_bls
    .groupby(["series_id", "year"], as_index=False)["value"]
    .sum()
)
# print(
#     df_yearly.loc[
#         # (df_best["best_year"] == 2000) & (df_best["series_id"] == "PRS30006012")
#         # (df_yearly["year"] == 2000) & ( df_yearly["series_id"] == "PRS30006011")
#         (df_yearly["series_id"].str.contains("PRS30006011"))
#     ]
# )


# Find best year per series_id
idx = df_yearly.groupby("series_id")["value"].idxmax()

df_best = df_yearly.loc[idx].reset_index(drop=True)

# Rename columns for clarity
df_best = df_best.rename(columns={
    "year": "best_year",
    "value": "total_value"
})
df_best = df_best[df_best["series_id"].isin(df_series["series_id"])]

# Final report
print(df_best.head(25))

print(
    df_best.loc[
        # (df_best["best_year"] == 2000) & (df_best["series_id"] == "PRS30006012")
        # (df_best["best_year"] == 2000)
        (df_best["series_id"].str.contains("PRS30006011") | df_best["series_id"].str.contains("PRS30006012"))
    ]
)

# output should look like this, with the best year and total value for each series_id:
#            series_id  best_year  total_value
# 0  PRS30006011             2022         20.5
# 1  PRS30006012             2022         17.1



# -----------------------------
# quest-3-4
# -----------------------------

# to have a CLEAN data set - reload BLS data from the file downloaded from S3
full_path = f"{HOMELOCAL}\\vscode-2026\\20260323\\rearc-quest-20260323\\bls_pr_data\\pr.data.0.Current"
df_bls = pd.read_csv(full_path, sep='\t')
df_bls.columns = df_bls.columns.str.strip()

print("BLS Data (Top 5 rows):")
print(df_bls.head(15))


# Load population data (JSON)
# population_json = {
#     "data": [
#         {"Nation ID": "01000US", "Nation": "United States", "Year": 2013, "Population": 316128839.0},
#         {"Nation ID": "01000US", "Nation": "United States", "Year": 2014, "Population": 318857056.0},
#         {"Nation ID": "01000US", "Nation": "United States", "Year": 2015, "Population": 321418821.0},
#         {"Nation ID": "01000US", "Nation": "United States", "Year": 2016, "Population": 323127515.0},
#         {"Nation ID": "01000US", "Nation": "United States", "Year": 2017, "Population": 325719178.0},
#         {"Nation ID": "01000US", "Nation": "United States", "Year": 2018, "Population": 327167439.0},
#         {"Nation ID": "01000US", "Nation": "United States", "Year": 2019, "Population": 328239523.0},
#         {"Nation ID": "01000US", "Nation": "United States", "Year": 2021, "Population": 331893745.0},
#         {"Nation ID": "01000US", "Nation": "United States", "Year": 2022, "Population": 333287562.0},
#         {"Nation ID": "01000US", "Nation": "United States", "Year": 2023, "Population": 334914896.0},
#         {"Nation ID": "01000US", "Nation": "United States", "Year": 2024, "Population": 340110990.0}
#     ]
# }
# df_population = pd.DataFrame(population_json["data"])
# Convert JSON 'data' section into DataFrame
with open(f"{HOMELOCAL}\\vscode-2026\\20260323\\rearc-quest-20260323\\quest-part-2\\population_data.json", 'r') as f:
    population = json.load(f)
df_population = pd.json_normalize(population["data"])
df_population.columns = df_population.columns.str.strip()

print("Population Data (Top 5 rows):")
print(df_population.head(15))

# Function to generate report
def report_value_population(series_id: str, period: str, year: int) -> pd.DataFrame:
    """
    Returns a DataFrame with value for the given series_id, period, year,
    and population for that year if available.
    """
    # Filter BLS data
    df_filtered = df_bls[
        (df_bls["series_id"].str.contains(series_id)) &
        (df_bls["period"] == period) &
        (df_bls["year"] == year)
    ].copy()
    
    if df_filtered.empty:
        print(f"No BLS data found for series_id={series_id}, period={period}, year={year}.")
        return None
    
    # Merge with population
    df_report = df_filtered.merge(
        df_population,
        left_on="year",
        right_on="Year",
        how="left"
    )
    
    # Select relevant columns
    df_report = df_report[["series_id", "year", "period", "value", "Population"]]
    
    return df_report

# -----------------------------
# Example usage
# -----------------------------
report = report_value_population("PRS30006032", "Q01", 2018)
print(report)

if __name__ == "__main__":
    print("from quest-part-3-3.py")