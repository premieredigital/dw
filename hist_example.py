import ast
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
    a.price * 1E0 price,
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
    LEFT OUTER JOIN main_territorypricingavail a ON a.tp_id=p.id
    LEFT OUTER JOIN _tracktitle t ON (i.provider_title_id=t.title_id)
WHERE
    i.content_type_id IN ('movie', 'movie bundle', 'tv season')
# LIMIT 10
"""
def batch():
    
    # For each date we have history for
    w = Warehouse()
    w.set_logging()
    w.connect()
    w.setup(sql=SQL, partition_on_field='date')
    schema = w.schema
    cursor,conn=w.cursor,w.conn
    FIRST_DATE = datetime.date(2015,7,4) # first day we have entries in main_iteminstance
    LAST_DATE = datetime.date(2020, 5, 11) # first day we started historical prices
    DATE_RANGE = (LAST_DATE - FIRST_DATE).days
    DATES_TO_PARSE = sorted([(FIRST_DATE + datetime.timedelta(days=n)) for n in range(DATE_RANGE+0)], reverse=True)
    
    # get the most-recent DB-data / map InstanceID+Territory+Offer (ignore date)
    #  cursor.execute(SQL)
    #  res = cursor.fetchall()
    #  open('res.json','w').write(json.dumps(res))
    logger.debug('Loading query from earlier...')
    res = json.loads(open('res.json').read())
    logger.debug('Query loaded')
    data = {'%s+%s+%s' % (i[1], i[8], i[10] or '') : list(i) for i in res}
    for num,date in enumerate(DATES_TO_PARSE):

        date = str(date)
        logger.debug('%s/%s -- %s' % (num, len(DATES_TO_PARSE), date))

        # Get the historical data for that day
        logger.debug('1. getting historical data for %s' % (date))
        cursor.execute('SELECT min(id), max(id) FROM main_history where date=%s', (date,))
        min_id, max_id = cursor.fetchone()

        # Note: we use the 'old_value' since we're going backwards from our current prices
        cursor.execute('''SELECT instance_id, territory_id, field, old_value, 
                           CASE when xpath like '%%uhdbuy%%' then 'UHDBUY' when xpath like '%%uhdrent%%' then 'UHDRENT' 
                                when xpath like '%%hdbuy%%' then 'HDBUY' when xpath like '%%hdrent%%' then 'HDRENT'
                                when xpath like '%%sdbuy%%' then 'SDBUY' when xpath like '%%sdrent%%' then 'SDRENT'
                            END offer
                            FROM main_history
                            WHERE id between %s and %s AND field in ('Price', 'IsFutureRelease', 'Currency')''', (min_id, max_id))
        historical_data = cursor.fetchall()

        # Update the data with the history data for that day and all days after it
        logger.debug('2. updating historical data with that data')
        for update in historical_data:
            key = '%s+%s+%s' % (update[0], update[1], update[4])
            pseudo_key = '%s+%s+' % (update[0], update[1])
            field, value = update[2], update[3]
            # a) Adding the data if that offer wasn't there
            if (key not in data) and (pseudo_key in data):
                data[key] = data[pseudo_key]
            # b) Normal update of data
            if key in data:
                if field == 'Price': 
                    data[key][11] = value # if the price is NULL, we skip it -- meaning it's not available
                elif field == 'IsFutureRelease':
                    data[key][13] = value
                elif field == 'Currency':
                    data[key][12] = value
                    if pseudo_key in data: data[pseudo_key][12] = value

        # for updating all the InstanceData -- remove items that are before date_inserted and update date
        for key in sorted(data):
            data[key][0] = date
            date_inserted = str(data[key][14])
            if date_inserted > date:
                data[key][11] = None

        
        # if the data has already been stored and saved to BQ
        # we still need to parse the data (because history needs to be layered for all days going back
        # but we don't save the file or save it to BQ
        cursor.execute('SELECT 1 FROM bq_helper WHERE value=%s', (date,))
        if cursor.fetchone(): continue

        # save to csv
        logger.debug('3. saving to local csv file')
        csv_filepath = 'data/historical_%s.csv' % date
        with open(csv_filepath, 'w') as f:
            writer = csv.writer(f, delimiter=',', quoting=csv.QUOTE_MINIMAL)
            for row_key in data.keys():
                row = [item or '' for item in data[row_key][:-1]] # set null = ''
                if row[11] is None: continue
                row[13] = 1 if str(value).lower() in ('1', 'true') else 0
                writer.writerow(row)

        #  # save to bq and enter in an entry in the DB
        logger.debug('4. saving local csv to bq')
        cmd = subprocess.call('''bq load --source_format=CSV --time_partitioning_field=date data_warehouse.historical_prices %s %s''' % (csv_filepath, schema), shell=True)
        if cmd == 0:
            logger.debug('Saved SQL results to BQ')
            os.remove(csv_filepath)
            cursor.execute('INSERT into bq_helper (`table`, value) values (%s, %s)', ('historical_prices', date))
            conn.commit()
            logger.debug('Done!')
        else:
            logger.debug('Failed BQ save')



if __name__ == '__main__':
    batch()

