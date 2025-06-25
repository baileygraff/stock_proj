
description:
simple data engineering practice project. plan to use stock data.

goals:
- use DBT as transform tool

seeds (planned):
- kaggle datasets ("stonks" collection)

notes:
- using DBeaver as lightweight gui for now
snowflake will be data warehousing tool in the future
- keep in mind how this will change future work.

project considerations:

file-based vs in-memory loading:
- explored extracting stock data directly to in-memory data-structures to be loaded to database. currently extract from source to csv files in raw_data directory. selected this approach primarily because it avoids potential memory issues with loading large datasets. additional bonuses include easier inspection, reproducibility, and reusability.

de-duplication:
- currently handle de-duplication through a function from load.py. this is handled through a Python script rather than in-DBT SQL staging because the load.py function could otherwise load duplicate data, and this was the first successful approach to avoiding excess bloating of the raw_stockdata_all table. Initial in-DBT pure-SQL approaches created a de-duplicated functional copy of raw_stockdata_all without addressing bloating of the source table.



to do:
- configuration mgmt for scripts 
- building out mechanism for building selection of target stock data
- setup script for project directory creation?
- implement dagster (or similar) for orchestration