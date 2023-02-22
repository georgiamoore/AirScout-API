from app import get_db_connection

def get_5_readings():
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("""
    SELECT row_to_json(fc) 
    FROM ( SELECT 'FeatureCollection' As type, array_to_json(array_agg(f)) As features
    FROM (SELECT 'Feature' As type, 
        ST_AsGeoJSON(lg.geom)::json As geometry, 
        (
            select row_to_json(t) 
            from (select id, utc_date, no2, voc, pm1, pm10, pm25) t
        )
        As properties
    FROM public.plume_sensor  As lg  WHERE latitude IS NOT NULL LIMIT 5 ) As f )  As fc;
    """)

    records = cur.fetchall()
    cur.close()
    conn.close()
    return records