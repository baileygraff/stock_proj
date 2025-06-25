"""
place for python data vis functions defined to work with kaggle
stock data

plotly library:
offers a lot of options around interactively 
modifying visuals
"""

import duckdb
import plotly.express as px
import pandas as pd
import numpy as np
from pathlib import Path

def stockdata_vis(db_file: str, db_dir: str = "seeds"):
    db_path = Path(db_dir)
    conn = duckdb.connect(database = db_path / db_file)

    try:
        #select data from the duckdb database table stg_kaggle__stockdata
        raw_data_query = conn.sql("SELECT * FROM stg_kaggle__stockdata")
        #convert it to pandas dataframe >> beware, consider implications of memory limits
        raw_df = raw_data_query.df()

        fig = px.line(
            raw_df,
            x="date",
            y="adj_close",
            color="stock",  # creates one line per stock
            title="Adjusted Close Price Over Time",
        )

        fig.update_layout(
            xaxis=dict(
                rangeselector=dict(
                    buttons=list([
                        dict(count=1, label="1d", step="day", stepmode="backward"),
                        dict(count=7, label="1w", step="day", stepmode="backward"),
                        dict(count=1, label="1m", step="month", stepmode="backward"),
                        dict(step="all")
                    ])
                ),
                rangeslider=dict(visible=True),
            )
        )

        fig.show()

    except Exception as e:
        print("data visualization failed: ", e)

#testing area
test_filename = 'stock_projdb.duckdb'
test_dbpath = "seeds"
stockdata_vis(test_filename, test_dbpath)