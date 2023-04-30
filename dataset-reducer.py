import csv
import sys

NUMBER_OF_COUNTRIES = 100
DAYS_PER_COUNTRY = 150

with open('files/input.csv', newline='') as csvfile:
    data = csv.reader(csvfile, delimiter=',', quotechar='|')

    prevCountry = ""
    header = next(data) # Save the first line (header)
    output = ','.join(header) + '\n'
    
    row = next(data)
    for i in range(NUMBER_OF_COUNTRIES):
        while(row[6] == prevCountry): # Skip all the lines until reaching a new country
            try:
                row = next(data)
            except:
                print("Number of countries is too large")
                sys.exit()
        prevCountry = row[6]
        for j in range(DAYS_PER_COUNTRY):
            if(prevCountry != row[6]):
                print("Number of days too large for a country -> ", prevCountry)
                sys.exit()
            output += ','.join(row)
            output += "\n"
            try:
                row = next(data)
            except:
                print("End of file reached")
                sys.exit()
    
    f = open('files/reduced-dataset.csv', 'w')
    f.write(output)
    f.close
    print("Done.")