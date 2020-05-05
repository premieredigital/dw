from base import *
import json
import csv
# source credentials && python example.py
## to delete: $ bq rm data_warehouse.historical_prices


SQL = """SELECT DATE(CONVERT_TZ(NOW(), 'UTC', 'America/Los_Angeles')) AS date, 
        i.id instance_id, i.master_id master_id, name, content_type_id, 
        release_year, i.provider_id, platform_type_id, territory_id,
        p.store_url, a.code, a.price, a.currency_code_id, a.is_future_release
 FROM main_iteminstance i JOIN main_territorypricing p ON p.item_id=i.id JOIN main_territorypricingavail a ON a.tp_id=p.id
 WHERE content_type_id IN ('movie', 'movie bundle', 'tv season') LIMIT 1000"""


SQL_2 = """SELECT DATE(CONVERT_TZ(NOW(), 'UTC', 'America/Los_Angeles')) AS date, 
        i.id instance_id, i.master_id master_id, name, content_type_id, 
        release_year, i.provider_id, platform_type_id, territory_id,
        p.store_url, a.code, a.price, a.currency_code_id, a.is_future_release, date(i.date_inserted)
 FROM main_iteminstance i JOIN main_territorypricing p ON p.item_id=i.id LEFT OUTER JOIN main_territorypricingavail a ON a.tp_id=p.id
 WHERE content_type_id IN ('movie', 'movie bundle', 'tv season')"""

def batch():
    # For each date we have history for
    w = Warehouse()
    w.connect()
    w.setup()
    schema = w.schema
    cursor,conn=w.cursor,w.conn
    FIRST_DATE = datetime.date(2015,7,4) # first day we have entries in main_iteminstance
    LAST_DATE = datetime.date(2020, 5, 3) # first day we started historical prices
    DATES_TO_PARSE = sorted(set(range(FIRST_DATE, LAST_DATE+1)), reverse=True)
    # get the most-recent DB-data / map InstanceID+Territory+Offer (ignore date)
    cursor.execute(SQL)
    data = {'%s+%s+%s' % (i[1], i[8], i[10] or '') : list(i) for i in cursor.fechall()}
    for num,date in enumerate(DATES_TO_PARSE):
        print (date, num, len(DATES_TO_PARSE))
        date = str(date)

        # Get the historical data for that day
        print ('1. getting historical data for %s' % (date))
        cursor.execute('SELECT min(id) max(id) FROM main_history where date=%s', date)
        min_id, max_id = cursor.fetchone()
        # Note: we use the 'old_value' since we're going backwards from our current prices
        cursor.execute('''SELECT instance_id, territory_id, field, old_value, 
                           CASE when xpath like '%%uhdbuy%%' then 'UHDBUY' when xpath like '%%uhdrent%%' then 'UHDRENT' 
                                when xpath like '%%hdbuy%%' then 'HDBUY' when xpath like '%%hdrent%%' then 'HDRENT'
                                when xpath like '%%sdbuy%%' then 'SDBUY' when xpath like '%%sdrent%%' then 'SDRENT'
                            END offer
                            FROM main_history WHERE id between %s and %s and field in ('Price', 'IsFutureRelease', 'Currency' 
                        ''', (min_id, max_id))
        historical_data = cursor.fetchall()

        # Update the data with the history data for that day and all days after it
        print ('2. updating historical data with that data')
        for update in historical_data:
            key = '%s+%s+%s' % (update[0], update[1], update[4])
            pseudo_key = '%s+%s+' % (update[0], update[1])
            field, value = update[2], update[3]
            # a) Adding the data if that offer wasn't there
            if (key not in historical_price) and (pseudo_key in historical_prices):
                data[key] = data[pseudo_key]
            # b) Normal update of data
            if key in historical_data:
                if field == 'Price': 
                    update[key][11] = value # if the price is NULL, we skip it -- meaning it's not available
                elif field == 'IsFutureRelease':
                    update[key][13] = value
                elif field == 'Currency':
                    update[key][12] = value
                    if pseudo_key in historical_prices: update[pseudo_key][12] = value

        # for updating all the InstanceData -- remove items that are before date_inserted and update date
        for key in sorted(data):
            data[key][0] = date
            date_inserted = str(data[key][-1])
            if date_inserted > date:
                data[key][11] = None


        
        # save to csv
        print ('3. saving to local csv file')
        csv_filepath = 'historical/%s.csv' % date
        with open(csv_filepath, 'w') as f:
            writer = csv.writer(f, delimiter=',', quoting=csv.QUOTE_MINIMAL)
            for row_key in data.keys():
                row = [item or '' for item in data[row_key][:-1]] # set null = ''
                if row[11] is None: continue
                row[13] = 1 if str(value).lower() in ('1', 'true') else 0
                writer.writerow(row)

        # save to bq and enter in an entry in the DB
        print ('4. saving local csv to bq')
        schema = 
        cmd = subprocess.call('''bq load --source_format=CSV --time_partitioning_field=date 
                                          data_warehouse.historical_prices %s %s''' % (csv_filepath, schema), shell=True)
        if cmd == 0:
            print('Saved SQL results to BQ')
            os.remove(csv_filepath)
            cursor.execute('insert into bq_helper (table, value) values (%s, %s)' % ('historical_prices', date))
            conn.commit()
            print 'Done!'
        else:
            print('Failed BQ save')



if __name__ == '__main__':
    Warehouse().run(
        script_name = 'historical_prices',
        sql = SQL,
        sql_args = None,
        partition_on_field = 'date'
    )


