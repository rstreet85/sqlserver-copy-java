# SQL Server Table Extraction Utility
**Updated 09-18-2017**

A Java program to extract and insert data into MS SQL Server. Based on web service used to remotely & automatically update internal copy of Conservation Resources Database from an image updated biennially.

Password-protected databases not currently supported.


Input: `original_database_url original_tbl_schema original_tbl_name new_database_url new_tbl_schema new_tbl_name`
Output: `filename.csv`