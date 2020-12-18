# Import libraries
import os
import urllib.request
import datetime

# get currrent year, moonth
today = datetime.datetime.now()

# Initiate the program with a welcome message
print(f' --------------------------------------------\n Starting Program on: {today.year}-{today.month}-{today.day}\n --------------------------------------------\n')

# Supply the target/base URLs that will be scrapped for this project
base_url = ['https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_',
            'https://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_',
            'https://nyc-tlc.s3.amazonaws.com/trip+data/fhv_tripdata_',
            'https://nyc-tlc.s3.amazonaws.com/trip+data/fhvhv_tripdata_']

# Provide file names that will be used to house data
filenames = ["tf_yellow_tripdata.csv", "tf_green_tripdata.csv", "tf_fhv_tripdata.csv",  "tf_fhvhv_tripdata.csv"]

# Create a function to:
def appendData(year, month):
    
    for i in range(4):
        url = base_url[i] + str(year) + "-" + '{0:02d}'.format(month) + ".csv" # concatenate base_yrl + year + month + .csv, to create web links: url        
            
        
        # Build try clause to: check if file path exists if not write one if so (else) open the appropriate file and append data
        try:
            datasource = urllib.request.urlopen(url) # access urls and open a stream to download data from them
            if not os.path.isfile(filenames[i]):
                fp = open(filenames[i], "w")
                new_file = True # indicate if open new file
            else:  # else it exists so append without writing the header (to prevent overwriting data within source file)
                fp = open(filenames[i], "a")
                new_file = False # indicate that open existing file
            count = 0
            
            while True:
                line = datasource.readline()
                if new_file == False and count == 0: # if downloaded file exists skip header as you already have it from previous file 
                    count += 1
                    continue

                if not line: # check if .csv stream is ended or not
                    break

                line = line.decode("utf-8").rstrip() # convert binary value to string and remove the last return character('\r\n')

                if not line: # skip empty line
                    continue

                line = line + '\n'
                fp.write(line)
                count += 1

                if count % 50000 == 0:
                    print("Line " + str(count) + " is processed")

                if count > 1: # just save 1000 records for each csv file for quick testing
                    break

            fp.close()
            
            print(url + " is Okay\n")    
        except Exception as e:
            print(f' --- The following: -{str(e)}- has occured for the following filles:')
            # print(f'Year: {str(year)} Month = {str(month)}')
            print(url + " is Invalid")    
            print(f'========================================================================')


# Write files
# Extract all of the historocal .csv data and merge except for this month
print(f' --------------------------------------------\n Whole Data Overview\n --------------------------------------------\n')
stop_flag = False
for year in range(2009, today.year + 1):
    if stop_flag == True:
        break
        
    for month in range(1, 13):
        if year == today.year and month == today.month:
            stop_flag = True
            break
            
        appendData(year, month)
        
#delta
