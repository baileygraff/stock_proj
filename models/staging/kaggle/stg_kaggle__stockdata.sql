--staging for stock data sourced from kaggle datasets
/*
thoughts on staging transformations:
- removing filename column, if present
- standardize column name format
- standardizing date format
- limit decimal places in csv data? to 4?
*/

select
    --first, normalize date field
    case
        when try_cast("Date" as date) is not null then cast("Date" as date)
        when try_cast(strptime("Date", '%m/%d/%Y') as date) is not null then strptime("Date", '%m/%d/%Y')
        --this could still possibly run into issues where day/month/year formatted data comes in
        else null
    end as date,

    --standardize variables (decimal notation, capitalization & spacing)
    stock,
    round("Open", 4) as "open",
    round("High", 4) as high,
    round("Low", 4) as low,
    round("Close", 4) as "close",
    round("Adj Close", 4) as adj_close,
    "Volume" as volume

from {{ source('stock_source', 'raw_stockdata_all') }}
where "date" is not null
