
import duckdb
from pathlib import Path


def load_csvs_to_rawdata(db_path: str, extracted_files: dict, file_dir: str = 'seeds'):
    #function for loading raw csv filedata to rawdata table in duckdb
    #note!!! > this does not currently prevent adding duplicate data.
    #note!!! > removing duplicates will be a first-priority cleanup task.

    conn = duckdb.connect(db_path)

    for key, csv_file in extracted_files.items():
        csv_path = Path(file_dir) / csv_file
        if not csv_path.exists():
            print(f"skipping missing file: {csv_path}")
            continue

        print(f"loading {csv_file}...")

        sql = f"""
        INSERT INTO raw_stockdata_all
        SELECT * 
        FROM read_csv(
            '{csv_path.as_posix()}',
            header=true,
            filename=true
        );
        """
        try:
            conn.execute(sql)

        except duckdb.CatalogException as e:
            #create table if it does not already exist
            if "Table with name raw_stockdata_all does not exist" in str(e):
                print("creating raw_stockdata_all table...")
                create_sql = f"""
                CREATE TABLE raw_stockdata_all AS
                SELECT * 
                FROM read_csv(
                    '{csv_path.as_posix()}',
                    header=true,
                    filename=true
                );
                """
                conn.execute(create_sql)
                #print(conn.execute("SELECT * FROM raw_stockdata_all LIMIT 5").fetchdf())
                #test printout, to be commented out
            else:
                raise

    conn.close()            


#test_run
###SUCCESS### > moving on to making main.py
#duckdb_path = "seeds/stock_projdb.duckdb"
#testfile_dict = {'username/visa_stock_data' : 'visa_stocks.csv'} #placeholder key b/c value is used, not key, for loading
#load_csvs_to_rawdata(duckdb_path, testfile_dict)

"""
###old -- to be deleted below###
def run_sql_script(db_path: str, sql_script: str):
    #current form needs full path+name for db and sql script
    #execute a SQL script against a duckdb file
    #intended for sql not meant for the DBT tool
    #TO DO: implement config mgmt

    try:
        sql_file = Path(sql_script)
        if not sql_script.exists():
            raise FileNotFoundError(f"SQL file not found: {sql_script}")
        
        with open(sql_script, "r") as script:
            sql = script.read()

        conn = duckdb.connect(db_path)
        conn.execute(sql)
        conn.close()
        print(f"executed {sql_script} int {db_path}")

    except Exception as e:
        print("sql script execution failed: ", e)
"""