from base import *
import json
import csv

# source credentials && python example.py
## to delete: $ bq rm data_warehouse.historical_prices


SQL = """
SELECT 
    DATE(CONVERT_TZ(NOW(), 'UTC', 'America/Los_Angeles')) AS date, 
    i.id instance_id, 
    i.master_id master_id,
    i.name,
    i.content_type_id, 
    i.release_year,
    i.provider_id,
    platform_type_id,
    territory_id,
    p.store_url,
    a.code,
    a.price,
    a.currency_code_id,
    a.is_future_release,
    DATE(i.date_inserted) AS date_inserted,
    COALESCE(p.provider_title_id, i.provider_title_id) provider_title_id,
    t.eidr_1,
    t.eidr_2,
    t.title_mpm mpm_id
FROM 
    main_iteminstance i 
    JOIN main_territorypricing p ON p.item_id=i.id 
    INNER JOIN main_territorypricingavail a ON a.tp_id=p.id
    LEFT OUTER JOIN _tracktitle t ON (i.provider_title_id=t.title_id)
WHERE 
    i.content_type_id IN ('movie', 'movie bundle', 'tv season')
#LIMIT 10
"""


if __name__ == '__main__':
    Warehouse().run(
        script_name = 'historical_prices',
        sql = SQL,
        sql_args = None,
        partition_on_field = 'date'
    )


