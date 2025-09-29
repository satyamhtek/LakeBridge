# Project README
 
This repository contains two main Python utilities for your data pipeline:
 
---
 
## 1. Extract SQL from Excel Files
 
### Overview
 
This script extracts SQL queries from Excel files (.xlsx) that contain a sheet named **"SQL Statements"**. Each SQL query is saved as an individual `.sql` file organized into subfolders named after the original Excel file.
 
### Features
 
- Reads all `.xlsx` files in a given folder.
- Extracts SQL queries from the "SQL Statements" sheet.
- Saves each SQL block into separate `.sql` files.
- Organizes output into subfolders named after the Excel filename.
- Handles duplicate filenames gracefully.
 
### Requirements
 
- Python 3.6+
- pandas
- openpyxl
 
Install dependencies:
 
```bash
pip install pandas openpyxl
 
 
Usage
 
Run the script:
 
python extract_sql_from_excel.py
 
 
When prompted, enter:
 
Path to the folder containing Excel files.
 
Output folder to save extracted SQL files.
 
Input Excel Format
 
Must have a sheet named "SQL Statements"
 
Sheet must contain columns: Item Name and SQL
 
Output Structure
output_folder/
├── ExcelFile1/
│   ├── Query1.sql
│   └── Query2.sql
├── ExcelFile2/
│   ├── InsertData.sql
│   └── UpdateRecords.sql
 
2. Lakebridge Analyze & Convert Pipeline
Overview
 
This Python script automates the process of:
 
Analyzing SSIS .dtsx files using Databricks Lakebridge Analyze CLI
 
Transpiling the extracted SQL code to target dialects
 
Post-processing SQL files (renaming, formatting)
 
Generating Databricks notebooks from SQL queries
 
Uploading notebooks to Databricks workspace
 
Requirements
 
Python 3.6+
 
databricks CLI installed and configured
 
sqlparse Python package
 
Install dependencies:
 
pip install sqlparse
 
 
Make sure databricks CLI is installed and authenticated:
 
databricks configure --token
 
Usage
 
Run the script:
 
python lakebridge_analyze_convert.py --source-path /path/to/dtsx_files --target-path /path/to/output --dialect synapse --profile your-databricks-profile --debug
 
 
Or run without arguments and follow interactive prompts.
 
Features
 
Analyzes each .dtsx file in the source folder with Lakebridge Analyze
 
Transpiles all found SQL files to the specified target dialect
 
Formats SQL code with consistent casing and indentation
 
Converts SQL files to Databricks Python notebooks for easy use
 
Uploads notebooks to the /Shared workspace folder on Databricks
 
Output Structure
target_folder/
├── lakebridge_analysis_<file>_<timestamp>.xlsx  # Analyzer reports
├── Converted_Code/                              # Transpiled and formatted SQL scripts
├── Final_Formatted/                             # Final formatted SQL scripts
├── Databricks_Notebooks/                        # Python notebooks for Databricks (auto-uploaded)
 
Troubleshooting & Notes
 
Verify paths and profiles when using CLI
 
Make sure .dtsx files are valid SSIS packages
 
The pipeline currently hardcodes --source-tech SSIS for Lakebridge Analyze
 
SQL replacements are tailored to your environment; adjust as needed
 
Logs and errors will print to console
 
License
 
MIT License
 
Contact
 
For issues or questions, please reach out.
