# Yellow Taxi Trip Data


## Introduction

The purpose of the project is to build a python application that is able to get the parquet files to extract and analyze Parquet files related to Yellow Taxi trips, specifically focusing on the top 10% of trips based on the distance traveled using the information from the TLC Trip Record Data (https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page). The application should be able to dynamically retrieve Parquet files based on starting and ending dates, process them using PySpark, and identify any data quality issues in the 'total_amount' field, along with implementing suitable data cleaning strategies.


## Project Components

This project involves the following key components:

### 1. Data Retrieval

To retrieve Parquet files, you will build a Python application that fetches data from the TLC Trip Record Data source. This application should allow users to specify starting and ending dates to filter the relevant Parquet files focusing on Yellow Taxi.

### 2. Data Processing with PySpark

You will use PySpark to process the downloaded Parquet files. This includes reading, transforming, and analyzing the data to identify the top 10% of trips based on distance traveled.

### 3. Data Quality Assessment

The primary focus will be on the 'total_amount' field within the dataset. Your task is to identify data quality issues in this field. You should implement suitable data cleaning and validation strategies to address any issues found.


## Getting Started

To get started with this project, follow these steps:

1. For deploying my code, I am using Jupyter Notebook. To enable the Spark environment into Jupyter Notebook, open the terminal and write the following steps

	- conda env list
	- conda create -n pyspark-env
	- conda activate pyspark-env
	- conda install pyspark
	- python
	- import pyspark
	- from pyspark.sql import SparkSession (this command should run without any issues)
	
2. PySpark requires Java version 7 or later and Python version 2.6 or later, therefore, Java should be installed before proceeding. Go to Java's website and download the appropriate for your operation system https://www.java.com/en/download/


3. Using BeautifulSoup package, retrieve the url links of all the parquet files related to Yellow Taxis from 2009 until July 2023.

4. Use PySpark to process the Parquet files, by loading all parquet files into PySpark dataframes and processing them to have all the records into a single dataframe, and finally, identifying the top 10% of trips based on distance traveled to perform initial data quality assessment.

5. Once all the filters have been applied, it is time to investigate any quality issues related to the 'total_amount' in the final dataframe and implement data cleaning methodologies.


## Usage

The Yellow_Taxi_Trip_Data.ipynb file has already been run to retrieve the all the parquet files that are located into 'Yellow Taxi Trip Data' directory.

The same file includes any filters applied to load all the dataframes read from parquet files into a single one (final_df) and decrease its volume to has only the top 10% trip based on distance traveled. 

*** When I tried to display the first 10 rows of the final_df, I got an error of 'ConnectionRefusedError: [Errno 61] Connection refused' or 'Py4JError: An error occurred while calling z:py4j.reflection.TypeUtil.isInstanceOf' when I reduce the final_df to only 10 joined dataframes. I did not know how to resolve both issues. This error has blocked me to proceed with the investigation of data quality issues.

That's why I created a separated ipynb file called Yellow_Taxi_Trip_Data_DataAnalysisJuly23.ipynb to investigate any quality issues based only in one dataset. When I tried to merge multiple dataframes, I got the above errors.


## Data Quality Issues

While investigating the 'total_amount' field, the following data quality issues have been observed, along with the strategies to handle them. (***Only checked for the dataframe of July 2023)

1. Inconsistencies: Negative values

	There are 25204 negative values only in the dataframe of July 2023. It is not reasonable to have negative values at the 'total_amount' field. Those records can be either changed to its positive value or removed.
	
2. Missing values

	Inside the dataframe of July 2023, there are no missing values in the 'total_amount' field. This does not mean that they do not exist in other dataframes. Such records should be removed from the dataframe.
	
3. Outliers

	Inside the dataframe of July 2023, the highest value observed in the 'total_amount' field is 1169.4. This is not considered outlier since the distance traveled is long enough to confirm that. Outliers should be removed because they can lead to wrong conclusions.

