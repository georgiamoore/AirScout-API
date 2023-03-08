from pyaurn import importAURN
from geojson import Feature, Point, FeatureCollection

def get_historic():
    # TODO use this to prevent hardcoding lat/long
    # (causes issues with retrieving pollutant list)
    # meta = importMeta()
    # valid_stations = meta[(meta['site_name'].str.contains('Birmingham')) & (meta['end_date']=='ongoing')]

    s1 = convert_to_feature_list("BIRR", range(2023, 2024), ['O3', 'NO', 'NO2','NOXasNO2', 'PM10', 'PM2.5'], 52.476145,  -1.874978)
    # s2 = convert_to_feature_list("BMLD", range(2023, 2024), ['O3', 'NO', 'NO2','NOXasNO2', 'SO2', 'PM10', 'PM2.5'], 52.481346,  -1.918235)
    # s3 = convert_to_feature_list("BOLD", range(2023, 2024), ['NO', 'NO2','NOXasNO2'], 52.502436,  -2.003497)
    # TODO - returning all 3 sensors is too slow to be loaded by the frontend map
    # return FeatureCollection(np.concatenate((s1,s2,s3)).tolist()) 
    return FeatureCollection(s1)


def convert_to_feature_list(site, years, pollutant_list, latitude, longitude):
    df = importAURN(site, years, pollutant=pollutant_list)
    df = df.fillna('')
    df = df.rename(columns = {'PM10':'pm10'}) #TODO rename other cols
    df['date'] = df['date'].astype(str)
    location = Point((longitude, latitude))
    features = []
    for row in df.itertuples(index=False):
        # print(df.columns.values) 
        # print(row["code"])
        features.append(Feature(geometry=location, properties={
            ""+col+"": row[i] for i, col in enumerate(df.columns.values)
        }))

    return features
