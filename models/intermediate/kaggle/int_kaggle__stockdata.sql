/*
ideas for intermediate transformations:
- calculate a daily return (close - open)
- calculate daily percent change
- flag for especially good or bad days (big swings)
*/

select
    *,
    "close" - "open" as daily_change,
    round( (("close" - "open") / nullif("open", 0)) * 100, 2) as pct_change_day
from {{ ref('stg_kaggle__stockdata') }}