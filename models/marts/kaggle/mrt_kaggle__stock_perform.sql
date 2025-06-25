/*
filtered results, analytical views, etc.
centered around stock performance
initial plans:
- top performing stocks > lifetime change?
- averages, changes, pct_changes, etc > based on ranges from the most recent date?
*/

with earliest_open as (
    select
        stock,
        min_by(open, date) as first_open
    from {{ ref('int_kaggle__stockdata') }}
    group by stock
),

latest_close as (
    select
        stock,
        max_by(close, date) as last_close
    from {{ ref('int_kaggle__stockdata') }}
    group by stock
),

stock_perf as (
    select
        eo.stock,
        eo.first_open,
        lc.last_close,
        round(lc.last_close - eo.first_open, 4) as raw_stock_perf,
        round(((lc.last_close - eo.first_open) / eo.first_open) * 100, 2) as pct_stock_perf
    from earliest_open eo
    join latest_close lc on eo.stock = lc.stock
)

select
    *,
    rank() over (order by pct_stock_perf desc) as performance_rank
from stock_perf