from pyaurn import importAURN, importMeta, timeAverage
import pandas as pd
def get_historic():
    s1 = importAURN("BIRR", range(2017, 2022), pollutant=['O3', 'NO', 'NO2','NOXasNO2', 'PM10', 'NV10', 'V10', 'PM2.5', 'NV2.5', 'V2.5','AT10', 'AP10', 'AT2.5', 'AP2.5'])
    s6 = importAURN("BMLD", range(2017, 2022), pollutant=['O3', 'NO', 'NO2','NOXasNO2', 'SO2', 'PM10', 'NV10', 'V10', 'PM2.5', 'NV2.5', 'V2.5','AT10', 'AP10', 'AT2.5', 'AP2.5'])
    s9 = importAURN("BOLD", range(2017, 2022), pollutant=['NO', 'NO2','NOXasNO2'])
    

    data = pd.concat([s1, s6, s9])

    print(data)
   
    data2 = s1.copy()
    data2= data2.set_index('date').groupby('site')
    data_monthly = timeAverage(data2,avg_time="month",statistic="mean")

    print(data_monthly)
    data.reset_index(inplace=True)
    return data.to_json(orient="records")
