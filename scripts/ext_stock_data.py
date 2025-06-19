
#extract data on stocks

import pandas as pd
import kaggle
from pathlib import Path

#list of desired kaggle datasets > to be controlled by  configuration controls later
datasets = []

#function with Kaggle as intended seed (source)
#get the data from kaggle > does this create json or csv?
#plan = use a list as function input for target kaggle datasets?
#replace data if presented with duplicate set? > plan = yes
def extract_init_kagdata(dataset_list, filepath="seeds"):
    try:
        print("extracting datasets from list")  #consider making this a more specific/descriptive message
        kaggle.api.authenticate()   #kaggle api token needs to be setup
        data_destination = Path(filepath)     #double check the logic on this
        for item in dataset_list:
            kaggle.api.dataset_download_file(item, data_destination, unzip=True)
            print(f"{item} dataset extracted to {data_destination} directory")
    except Exception as e:
        print("Kaggle dataset extraction failed: ", e)
        #is there something to return that makes this fail in the most safe way?


######testing area below###########
extract_init_kagdata(datasets)