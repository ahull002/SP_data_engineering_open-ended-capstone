import os
import datetime
import urllib.request

import click


DATASETS = {
    "yellow": {
        "filename": "",
        "base_url": "",
    },
    "green": {
        "filename": "",
        "base_url": "",
    }
}

FILENAMES = [
    "tf_yellow_tripdata.csv",
    "tf_green_tripdata.csv",
    "tf_fhv_tripdata.csv",
    "tf_fhvhv_tripdata.csv",
]

BASE_URLS = [
    "https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_",
    "https://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_",
    "https://nyc-tlc.s3.amazonaws.com/trip+data/fhv_tripdata_",
    "https://nyc-tlc.s3.amazonaws.com/trip+data/fhvhv_tripdata_",
]

SUCCESS_LOG = "success_files"


def _get_project_url(base_url, month, year):
    formatted_month = "{0:02d}".format(month)
    return f"{base_url}{year}-{formatted_month}.csv"


def _process_url(url, filename, sample):
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

            f.write(line.decode("utf8").rstrip())
           

            if sample and i > int(sample):
                break


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


def sync_files(month, year, sample=None):
    urls = [_get_project_url(base_url, month, year) for base_url in BASE_URLS]
    for url, file in zip(urls, FILENAMES):
        try:
            _process_url(url, file, sample)
        except Exception as e:
            _print_neat_error(e, month, year, url)


def _line_separator():
    return "=" * 50


def _banner():
    return "%s\nWhole Data Overview\n%s" % ((_line_separator(),) * 2)


def _greeting(day, month, year):
    return f"Starting program for {month}-{year}-{day}\n" + _line_separator()


@click.command()
@click.option("--sample", default=None, required=False)
@click.option("--datasets", description="Put name of datasets like yellow,green", required=False, default=None)
def run(sample, datasets):
    today = datetime.datetime.now()
    current_day, current_month, current_year = today.day, today.month, today.year

    print(_greeting(current_day, current_month, current_year))
    print(_banner())

    for year in range(2009, current_year + 1):
        for month in range(1, 13):
            if (month, year) == (current_month, current_year):
                return
            sync_files(month, year, sample)


if __name__ == "__main__":
    run()
