import pandas as pd
import csv, json
from geojson import Feature, FeatureCollection, Point
import psycopg2
import psycopg2.extras as extras

def import_waqi_daily_avg_csv(filename, latitude, longitude):
    # pd.read_csv(filename)
    # utc_date =
    # no2 = 
    # pm10 = 
    # pm25 = 

    features = []
    with open(filename, newline='') as csvfile:
        reader = csv.reader(csvfile, delimiter=',')
        for date, pm25, pm10, o3, no2, so2 in reader:
            latitude, longitude = map(float, (latitude, longitude))
            features.append(
                Feature(
                    geometry = Point((longitude, latitude)),
                    properties = {
                        'utc_date': date,
                        'pm25': pm25,
                        'pm10': pm10,
                        'o3': o3,
                        'no2': no2,
                        'so2': so2,
                    }
                )
            )

    collection = FeatureCollection(features)
    with open("GeoObs.json", "w") as f:
        f.write('%s' % collection)


import_waqi_daily_avg_csv('birmingham-a4540 roadside-air-quality.csv', 52.476145, -1.874978)
# import_waqi_daily_avg_csv('birmingham-ladywood-air-quality.csv', 52.481346, -1.918235)

def convert_df_to_db_format(df, conn, cursor, table_name, renamed_cols):
    df = df.rename(columns = renamed_cols)
    # removing columns that don't exist in db
    cursor.execute("SELECT * FROM %s LIMIT 0" % (table_name,))
    db_cols = [desc[0] for desc in cursor.description]
    df = df[df.columns.intersection(db_cols)]
    
    # convert df to list of tuples for bulk insert to db
    tuples = [tuple(x) for x in df.to_numpy()]
    cols = ', '.join(f'"{c}"' for c in df.columns.tolist())
    query  = "INSERT INTO %s(%s) VALUES %%s ON CONFLICT DO NOTHING" % (table_name, cols)

    try:
        extras.execute_values(cursor, query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        conn.rollback()
        cursor.close()
        # TODO fix error return format -> use Flask response
        return("Error: %s" % error)

    cursor.close()
    return "Sensor readings inserted successfully."
