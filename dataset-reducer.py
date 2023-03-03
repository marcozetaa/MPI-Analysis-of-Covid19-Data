import csv
import sys

NUMBER_OF_COUNTRIES = 10
DAYS_PER_COUNTRY = 20

with open('files/input.csv', newline='') as csvfile:
    data = csv.reader(csvfile, delimiter=',', quotechar='|')

    output = ""
    prevCountry = ""
    next(data) # Skip the first line (header)
    
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
                print("Number of days too large for a country")
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