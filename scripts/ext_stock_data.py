
#extract data on stocks

import pandas as pd
import duckdb
import kaggle
import zipfile
import io
from pathlib import Path

#dictionary of desired kaggle datasets > to be controlled by  configuration controls later
#datasets = {'umerhaddii/visa-stock-data-2024':'visa_stocks.csv', 'umerhaddii/google-stock-data-2024': 'GOOGL_2004-08-01_2024-12-18.csv', 'umerhaddii/oracle-stock-data-2024':'Oracle_stock.csv'}


#plan = use a dictionary as function input for target kaggle datasets?
#replace data if presented with duplicate set? > plan = yes
def extract_init_kagdata(dataset_dict: dict, filepath="raw_data"):
    #function with Kaggle as intended seed (source)
    try:
        print("extracting datasets from list")  #consider making this a more specific/descriptive message
        kaggle.api.authenticate()   #kaggle api token needs to be setup
        data_destination = Path(filepath)
        data_destination.mkdir(parents=True, exist_ok=True) 
        #does this address some of my need for creating directories if not already made? 

        for dataset_id, (_, target_file) in dataset_dict.items():
            kaggle.api.dataset_download_file(dataset_id, target_file, path=data_destination, force=True)
            print(f"{dataset_id} dataset extracted to {data_destination} directory")
    except Exception as e:
        print("Kaggle dataset extraction failed: ", e)
        #is there something to return that makes this fail in the most safe way?

def add_stock_column(dataset_dictionary: dict, csv_path: str = "raw_data"):
    #function to append "stock" column with stock symbol value
    #what if this is not needed and is run by mistake?

    data_path = Path(csv_path)

    try:
        for dataset_key in dataset_dictionary:
            stock, filename = dataset_dictionary[dataset_key]
            csv_filepath = data_path / filename
            
            if not csv_filepath.exists():
                print(f"Skipping missing file: {csv_filepath}")
                continue

            df = pd.read_csv(csv_filepath)

            if "stock" not in df.columns:
                df["stock"] = stock
                df.to_csv(csv_filepath, index=False)

    except Exception as e:
        print("failed adding stock column to csv: ", e)


"""
not working yet -- to be worked out later if possible
def extract_kagg_duckdb(dataset_dict: dict, db_directory: str, filepath="seeds"):
    #function to extract kaggle data to memory directly to duckdb, skipping csv download
    api = kaggle.KaggleApi()
    api.authenticate()

    try:
        for key in dataset_dict:

            #download zip file from kaggle
            target_dataset = str(key)  #select the key from the dict of datasets
            target_file = str(dataset_dict[key]) #select the value from the dict of datasets
            raw_data = api.dataset_download_file(target_dataset, file_name=target_file)
            zip_data = zipfile.ZipFile(io.BytesIO(raw_data))

            #extract csv file into memory as pandas dataframe
            with zip_data.open(target_file) as f:
                df = pd.read_csv(f)

            return df
            print(f"{df} loaded to memory as dataframe")
    except:
        raise
"""


######testing area below###########
#extract_init_kagdata(datasets)
#Success!!!