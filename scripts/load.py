
import duckdb
import pandas as pd
from pathlib import Path


def load_csvs_to_rawdata0(db_path: str, extracted_files: dict, file_dir: str = 'raw_data'):
    #the original version of the load fn that does not dedup
    #function for loading raw csv filedata to rawdata table in duckdb
    #note!!! > this does not currently prevent adding duplicate data.
    #note!!! > removing duplicates will be a first-priority cleanup task.

    conn = duckdb.connect(db_path)

    for key, (_, csv_file) in extracted_files.items():
        csv_path = Path(file_dir) / csv_file

        #handle missing files
        if not Path(file_dir).exists():
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


def load_csvs_to_rawdata(db_path: str, extracted_files: dict, file_dir: str = 'raw_data'):
    #function for loading raw csv filedata to rawdata table in duckdb
    #note!!! > this does not currently prevent adding duplicate data.
    #note!!! > removing duplicates will be a first-priority cleanup task.

    conn = duckdb.connect(db_path)
    raw_data_path = Path(file_dir)

    for key, (stock, csv_file) in extracted_files.items():
        csv_path = raw_data_path / csv_file

        #handle missing files
        if not csv_path.exists():
            print(f"skipping missing file: {csv_path}")
            continue

        print(f"loading {csv_file}...")

        #load CSV into a pandas DataFrame
        try:
            df = pd.read_csv(csv_path)
            df["stock"] = stock #ensure the stock column exists
        except Exception as e:
            print(f"failed to read {csv_path}: {e}")
            continue

        #create a temporary duckDB table from pandas DF
        conn.register("new_data", df)

        #create target table if it does not exist
        try:
            conn.execute("SELECT * FROM raw_stockdata_all LIMIT 1")
        except duckdb.CatalogException:
            print("creating raw_stockdata_all table...")
            conn.execute("""
                CREATE TABLE raw_stockdata_all AS
                SELECT * FROM new_data
            """)
            continue #no need for dedup if this is the initial insert

        dedup_insert_sql = """
            INSERT INTO raw_stockdata_all
            SELECT n.*
            FROM new_data n
            LEFT JOIN raw_stockdata_all r
              ON n."Date" = r."Date" AND n.stock = r.stock
            WHERE r."Date" IS NULL
        """

        try:
            conn.execute(dedup_insert_sql)
        except Exception as e:
            print(f"failed dedup insert for {csv_file}: {e}")

    # Print row count for testing > commented out when not testing
    #row_count = conn.execute("SELECT COUNT(*) FROM raw_stockdata_all").fetchone()[0]
    #print(f"raw_stockdata_all row count: {row_count}")

    conn.close()            

#test_run
###SUCCESS### > moving on to making main.py
#duckdb_path = "seeds/stock_projdb.duckdb"
#testfile_dict = {'username/visa_stock_data' : 'visa_stocks.csv'} #placeholder key b/c value is used, not key, for loading
#load_csvs_to_rawdata(duckdb_path, testfile_dict)
