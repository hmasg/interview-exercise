# Databricks notebook source
# MAGIC %sql
# MAGIC -- Adaptive Query Execution
# MAGIC set spark.sql.adaptive.enabled = true;
# MAGIC -- Dynamic Partition Pruning
# MAGIC set spark.sql.optimizer.dynamicPartitionPruning.enabled=true;