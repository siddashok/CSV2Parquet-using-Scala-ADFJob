# CSVtoParquet using Databricks (Scala) and running Notebook inside Azure Data Factory
Converting CSV files to Parquet 

- Azure storage containers for CSV files and Parquet files are mounted
- Read the CSV files from source using defines schema
- convert the CSV files to parquet format 
- store the Parquet files to destination folder 

- Parameterization used so as to select which folder and sub folder to choose from the container for the CSV files to be converted
- Notebook can be ran from Azure Data factory. 

- Create an Azure Data factory and within it create a linked service for Databricks 
- Add Parameters to the pipeline 
- Add Databricks notebooks to the pipeline, and give the details ( Linked Service and Parameters to be used in the notebook) 
- JSON file in this repo has the ADF template 
