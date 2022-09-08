# Databricks notebook source
#Python Function
def duplicates_func(dataframe,columns):
 #Some operations
 String_VARIABLE_To_Be_Executed=f"""{dataframe}.groupBy({columns}).agg(count("*").alias("duplicated")).where(col("duplicated") > 1).sort(col("duplicated").desc())""" 
 return String_VARIABLE_To_Be_Executed