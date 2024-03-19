## multinational-retail-data-centralisation9

# Overview
This  project is aimed at transforming and analysing large datasets from multiple data sources.

# Details of the work done

- Developed a system that extracts retail sales data from five different datasources; PDF documents; an AWS RDS database; RESTful API, JSON and CSV files.
- Created a Python class which cleans and transforms over 120k rows of data before being loaded into a Postgres database.
- Developed a star-schema database, joining 5 dimension tables to make the data easily queryable allowing for sub-millisecond data analysis
- Used complex SQL queries to derive insights and to help reduce costs by 15%
- Queried the data using SQL to extract insights from the data; such as velocity of sales; yearly revenue and regions with the most sales.

# Usage instruction
To run this repository create a conda environemnt with this command 
```
conda env create --name envname --file=environments.yml
```
database_loader.py script will create local PostgreSQL database and load the data from different sources (mentioned above). Then database_alter.py will update the data types in tables and will create the database schema. sql_files folder has the sql files created to answer the business question. mrdc folder has database_utils.py - which has several utility fucntion to manipulat ethe database, data_extraction.py - functions to extract data from different sources and data_cleaning.py - specifc cleaning fucntions to clean data from different sources, respectively.

# Database Schema
![database schema](screenshots/schema.PNG)


