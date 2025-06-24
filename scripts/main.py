
import ext_stock_data as xt
import load as ld

#variables >> to be replaced with config mgmt soon
stock_dict = {
    #key: {'kaggle/address': ['STOCK SYMBOL', 'dataset_filename.csv']}
    #some items might be commented out to test deduplication after loading
    'umerhaddii/visa-stock-data-2024': ['V','visa_stocks.csv'],
    'umerhaddii/google-stock-data-2024': ['GOOG','GOOGL_2004-08-01_2024-12-18.csv'],
    'umerhaddii/oracle-stock-data-2024': ['ORCL', 'Oracle_stock.csv']
}

duckdb_path = "seeds/stock_projdb.duckdb"
rawdata_path = "raw_data"

#extract data from kaggle dataset files to seeds directory as csv of raw data
xt.extract_init_kagdata(stock_dict, rawdata_path)
#add column with stock symbol to extracted csv data
xt.add_stock_column(stock_dict, rawdata_path)

#load extracted data to duckdb raw_stockdata_all table, create if needed
#needs deduplication 
ld.load_csvs_to_rawdata(duckdb_path, stock_dict, rawdata_path)