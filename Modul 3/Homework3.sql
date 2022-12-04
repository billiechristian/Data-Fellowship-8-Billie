with total_trx as (
  select
    channelGrouping
    , geoNetwork_country
    , date
    , sum(totals_transactions) as sum_trx
  FROM `data-to-insights.ecommerce.rev_transactions`
  where geoNetwork_country != '(not set)'
  group by 1,2,3
)

select
  channelGrouping
  , array_agg(
    STRUCT(geoNetwork_country, date, sum_trx)
    order by geoNetwork_country, date desc
  )
from total_trx
group by 1
