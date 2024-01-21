# https://pypi.org/project/faker_airtravel/
# pip install faker_airtravel
import csv
from faker import Faker
from faker_airtravel import AirTravelProvider

fake = Faker()
fake.add_provider(AirTravelProvider)

csv_file = open('faker_airports.csv', 'w')
csv_file.write('airport|iata|icao|city|state|country\n')

num_records = 100000
iata_list = []

for i in range(0, num_records):
    airline = fake.flight()
    print(airline)

    origin = airline['origin']
    destination = airline['destination']

    iata = origin['iata']
    
    if iata not in iata_list:
        iata_list.append(iata)

        airport = origin['airport']
        icao = origin['icao']        
        city = origin['city']
        state = origin['state']        
        country = origin['country'].title()
        
        csv_file.write(airport + '|' + iata + '|' + icao + '|' + city + '|' + state + '|' + country + '\n')

    iata = destination['iata']
    
    if iata not in iata_list:
        iata_list.append(iata)

        airport = destination['airport']
        icao = destination['icao']
        city = destination['city']
        state = destination['state']
        country = destination['country'].title()
        
        csv_file.write(airport + '|' + iata + '|' + icao + '|' + city + '|' + state + '|' + country + '\n')        
