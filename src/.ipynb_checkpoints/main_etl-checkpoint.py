# Import libraries
from enum import Enum
import os
import datetime
import urllib.request

# Sample/Full Extract Transform Load (ETL) toggle (True = pulls sample data and number sample lines (prototype), Falso = pull all data)
SAMPLING = False
SAMPLE_LINES = 5


# Build Mapper
#Create source variables to append to file paths
class Source(Enum):
    YELLOW = "YELLOW"
    GREEN = "GREEN"
    FHV = "FHV"
    FHVHV = "FHVHV"

#Establish base urls to scrape Tap/Source TLC Trip Record Data
BASE_URLS = {
    Source.YELLOW: "https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_",
    Source.GREEN: "https://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_",
    Source.FHV: "https://nyc-tlc.s3.amazonaws.com/trip+data/fhv_tripdata_",
    Source.FHVHV: "https://nyc-tlc.s3.amazonaws.com/trip+data/fhvhv_tripdata_",
}

#Establish file names to archive extracted data in Target location
FILENAMES = {
    Source.YELLOW: "../data/sample_yellow_tripdata.csv",
    Source.GREEN: "../data/sample_green_tripdata.csv",
    Source.FHV: "../data/sample_fhv_tripdata.csv",
    Source.FHVHV: "../data/sample_fhvhv_tripdata.csv",
}

#Establish error logfiles (Success/Failure) to capture success (report of data collected) and failures (report of broken or error urls)
SUCCESS_LOG = "../data/log_success_file.txt"
FAILURE_LOG = "../data/log_failure_file.txt"

#Establish each field name for every file type to validate against during the extraction (Big E) of ETL for each "Tap"/Source being pulled
#target field: index in original file
FIELD_MAPPING = {
    Source.YELLOW: {"vendor_name": 0,
                    "Trip_Pickup_DateTime": 1,
                    "Trip_Dropoff_DateTime": 2,
                    "Passenger_Count": 3,
                    "Trip_Distance": 4,
                    "Start_Lon": 5,
                    "Start_Lat": 6,
                    "Rate_Code": 7,
                    "store_and_forward": 8,
                    "End_Lon": 9,
                    "End_Lat": 10,
                    "Payment_Type": 11,
                    "Fare_Amt": 12,
                    "surcharge": 13,
                    "mta_tax": 14,
                    "Tip_Amt": 15,
                    "Tolls_Amt": 16,
                    "Total_Amt": 17
                   },
    Source.GREEN: {"VendorID": 0,
                   "lpep_pickup_datetime": 1,
                   "Lpep_dropoff_datetime": 2,
                   "Store_and_fwd_flag": 3,
                   "RateCodeID": 4,
                   "Pickup_longitude": 5,
                   "Pickup_latitude": 6,
                   "Dropoff_longitude": 7,
                   "Dropoff_latitude": 8,
                   "Passenger_count": 9,
                   "Trip_distance": 10,
                   "Fare_amount": 11,
                   "Extra": 12,
                   "MTA_tax": 13,
                   "Tip_amount": 14,
                   "Tolls_amount": 15,
                   "Ehail_fee": 16,
                   "Total_amount": 17,
                   "Payment_type": 18,
                   "Trip_type": 19
                  },

    Source.FHV: {"Dispatching_base_num": 0,
                 "Pickup_date": 1,
                 "locationID":2
                },
    Source.FHVHV: {"hvfhs_license_num": 0,
                   "dispatching_base_num": 1,
                   "pickup_datetime": 2,
                   "dropoff_datetime": 3,
                   "PULocationID": 4,
                   "DOLocationID": 5,
                   "SR_Flag": 6
                  }
}

#Define order of required fields in the transformed file
#for each source type, define field order
TRANSFORMED_FIELD_ORDER = {
    Source.YELLOW: ["vendor_name",
                    "Trip_Pickup_DateTime",
                    "Trip_Dropoff_DateTime",
                    "Passenger_Count",
                    "Trip_Distance",
                    "Start_Lon",
                    "Start_Lat",
                    "Rate_Code",
                    "store_and_forward",
                    "End_Lon",
                    "End_Lat",
                    "Payment_Type",
                    "Fare_Amt",
                    "surcharge",
                    "mta_tax",
                    "Tip_Amt",
                    "Tolls_Amt",
                    "Total_Amt",],
    
    Source.GREEN: ["VendorID",
                   "lpep_pickup_datetime",
                   "Lpep_dropoff_datetime",
                   "Store_and_fwd_flag",
                   "RateCodeID",
                   "Pickup_longitude",
                   "Pickup_latitude",
                   "Dropoff_longitude",
                   "Dropoff_latitude",
                   "Passenger_count",
                   "Trip_distance",
                   "Fare_amount",
                   "Extra",
                   "MTA_tax",
                   "Tip_amount",
                   "Tolls_amount",
                   "Ehail_fee",
                   "Total_amount",
                   "Payment_type",
                   "Trip_type",],
    
    Source.FHV: ["Dispatching_base_num",
                 "Pickup_date",
                 "locationID"],
    
    Source.FHVHV: ["hvfhs_license_num",
                    "dispatching_base_num",
                    "pickup_datetime",
                    "dropoff_datetime",
                    "PULocationID",
                    "DOLocationID",
                    "SR_Flag"]
                   }


# This function returns a concatenated string: base_url+year+"-"+formatted_month+".csv"
def _get_project_url(base_url, month, year):
    formatted_month = "{0:02d}".format(month)
    return f"{base_url}{year}-{formatted_month}.csv"


def get_transformed_row(source_type, row):
    try:
        transformation_fields = TRANSFORMED_FIELD_ORDER[source_type]
        mapper = FIELD_MAPPING[source_type]

        return [row[mapper[field]] for field in transformation_fields]

    except Exception as e:
        if row != [""]:
            print("!"*1000)
            print(e)
            print(row)
        return None

def _process_url(source_type, url, filename):
    print(f"Starting URL processing for {url} and {filename}")
    datasource = urllib.request.urlopen(url)

    file_exists = os.path.isfile(filename)
    write_mode = "a" if file_exists else "w"
    with open(filename, write_mode) as f, open(SUCCESS_LOG, "a") as success_f:
        lines_from_datasource = [datasource.readline() for i in range(SAMPLE_LINES)]

        for i, line in enumerate(lines_from_datasource):
            if not line:
                continue

            if i == 0 and file_exists:
                # if downloaded file exists skip header
                # as you already have it from previous file
                continue

            row = line.decode("utf8").rstrip().split(",")
            transformed_row = get_transformed_row(source_type, row)
            if not transformed_row: continue
                
            f.write(",".join(transformed_row) + "\n")

            if SAMPLING and i > SAMPLE_LINES:
                break
        success_f.write(f"{url}\n")


def _print_neat_error(err, month, year, url):
    print(
        f"""
        {err}\nThe above error occured for the following:\n
        URL: {url}
        Month: {month}
        Year: {year}
        {_line_separator()}
    """
    )


def sync_files(month, year):
    for source_type, base_url in BASE_URLS.items():
        url = _get_project_url(base_url, month, year)
        filename = FILENAMES[source_type]
        try:
            print("Calling process_url for ", url, filename)
            _process_url(source_type, url, filename)
        except Exception as e:
            _print_neat_error(e, month, year, url)


def _line_separator():
    return "=" * 50


def _banner():
    return "%s\nWhole Data Overview\n%s" % ((_line_separator(),) * 2)


def _greeting(day, month, year):
    return f"Starting program for {month}-{year}-{day}\n" + _line_separator()


def run():
    today = datetime.datetime.now()
    current_day, current_month, current_year = today.day, today.month, today.year

    print(_greeting(current_day, current_month, current_year))
    print(_banner())

    for year in range(2009, current_year + 1):
        for month in range(1, 13):
            if (month, year) == (current_month, current_year):
                return
            sync_files(month, year)

run()