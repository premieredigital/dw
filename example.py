from base import *
# Make sure the os variables are added, for example: DB_USER, DB_PASS, DB_NAME, DB_HOST
# This example has a LIMIT applied, but this is only so it will run faster -- a LIMIT probably should not be applied.
if __name__ == '__main__':
    Warehouse().run(
        script_name = 'historical_prices',
        sql = """SELECT DATE(CONVERT_TZ(NOW(), 'UTC', 'America/Los_Angeles')) AS date,
                   name, content_type_id, release_year, i.provider_id, platform_type_id,
                   territory_id, p.store_url, a.code, a.price, a.currency_code_id
                 FROM main_iteminstance i JOIN main_territorypricing p ON p.item_id=i.id JOIN main_territorypricingavail a ON a.tp_id=p.id
                 WHERE content_type_id IN ('movie', 'movie bundle')""",
        sql_args = None,
        partition_on_field = 'date',
        incremental_field = 'date'

    )
