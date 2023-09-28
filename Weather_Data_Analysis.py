import pandas as pd
import requests
import json
import datetime
from datetime import date, timedelta
import matplotlib.pyplot as plt
import pymysql

#API_KEY = "17e4adfd66b777ef604433c946c71866"

# Function to fetch weather data for a city
def get_weather(lat,lon):

    
    link = f"https://api.openweathermap.org/data/3.0/onecall?lat={lat}&lon={lon}&appid=17e4adfd66b777ef604433c946c71866"
    
    response = requests.get(link)
    if response.status_code == 200:
        data = json.loads(response.text)
        return data
    else:
        return None

# Function to save weather data to the MySQL database
def insertion(city, temperature, humidity, wind_speed, description,dates):
    connection = pymysql.connect(
        host="localhost",
        user="root",
        password="Adrion023",
        database="weather_data"
    )

    cursor = connection.cursor()
    insert_query = "INSERT INTO weather (city, temperature, humidity, wind_speed, description,dates) VALUES (%s, %s, %s, %s, %s,%s)"
    cursor.execute(insert_query, (city, temperature, humidity, wind_speed, description,dates))
    connection.commit()
    connection.close()
    


def main():
    df = pd.read_csv("Indiancities.csv")
    cities = df['CITY']
    df.set_index('CITY', inplace=True)
    #end_date = date.today()
    #start_date = end_date - timedelta(days=5)
  
    for city in cities:
        lt = df.loc[[city],['lat']]
        lng= df.loc[[city],['lng']]
        lat = float(lt.loc[city]) 
        lon = float(lng.loc[city])
        data = get_weather(lat,lon)
        
        if data:
            for i in range(8):
                x=data['daily'][i]
                datetime_str = x['dt']
                dates = datetime.datetime.utcfromtimestamp(int(datetime_str)).strftime('%Y-%m-%d ')
                temperature = x["temp"]['day']
                humidity = x["humidity"]
                wind_speed = x["wind_speed"]
                description = x["weather"][0]["description"]            
                insertion(city, temperature, humidity, wind_speed, description,dates)
                i+=1
                print(f"Data saved for {city}")
               
            else:
                continue
        
        else:
            print(f"Failed to fetch data for {city}")
    connection = pymysql.connect(
        host="localhost",
        user="root",
        password="Adrion023",
        database="weather_data"
        )
    hquery = "SELECT city, MAX(temperature) AS Hghest_Temperature FROM weather WHERE dates GROUP BY city ORDER BY Hghest_Temperature DESC"
    htemp = pd.read_sql(hquery, connection)
    print(htemp)
    htemp.plot()
    plt.show()
    
    lquery = "SELECT city, MIN(temperature) AS Lowest_Temperature FROM weather GROUP BY city ORDER BY Lowest_Temperature ASC;"
    ltemp= pd.read_sql(lquery,connection)
    print(ltemp)
    ltemp.plot()
    plt.show()
    
    wquery="SELECT city, MAX(wind_speed) AS Highest_Wind_Speed FROM weather WHERE dates GROUP BY city ORDER BY highest_wind_speed DESC;"
    windiest = pd.read_sql(wquery, connection)
    print(windiest)
    windiest.plot.bar()
    plt.show()
    
    rquery="SELECT city FROM weather WHERE dates = CURDATE() AND description LIKE '%rain%';;"
    rainfall = pd.read_sql(rquery, connection)
    print("Cities where rainfall is expected:")
    print(rainfall)  
  
if __name__ == "__main__":
    main()
