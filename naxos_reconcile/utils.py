import csv
import json
import datetime
import itertools
import os
from typing import Generator
from bookops_worldcat import WorldcatAccessToken


MARC_URI = "http://www.loc.gov/MARC21/slim"
MARC_NS = "{http://www.loc.gov/MARC21/slim}"


def get_file_length(file: str) -> int:
    """
    Reads a csv file and returns the number of rows in the file

    Args:
        file: path to location of csv file

    Returns:
        length of csv file as int
    """
    with open(file, "r", encoding="utf-8") as infile:
        row_count = sum(1 for row in infile)
    return row_count


def date_directory() -> str:
    """create directory for output files using today's date"""
    date = datetime.date.today()
    return f"./data/files/{str(date)}"


def out_file(file: str) -> str:
    """create file for output in today's file director"""
    if not os.path.exists(date_directory()):
        os.makedirs(date_directory())
    return f"{date_directory()}/{file}"


def save_csv(outfile: str, row: list) -> None:
    """save row of output to a csv"""
    with open(outfile, "a", encoding="utf-8") as csvfile:
        out = csv.writer(
            csvfile,
            delimiter=",",
            lineterminator="\n",
        )
        out.writerow(row)


def get_token(filepath: str) -> WorldcatAccessToken:
    """get token for worldcat searches"""
    with open(filepath, "r") as file:
        data = json.load(file)
        return WorldcatAccessToken(
            key=data["key"],
            secret=data["secret"],
            scopes=data["scopes"],
        )


def open_csv_file(file: str, last_row: int) -> Generator:
    """open a csv file to and yield the next row"""
    with open(file, "r", encoding="utf-8") as csv_file:
        reader = csv.reader(csv_file, delimiter=",")
        for row in itertools.islice(reader, last_row, None):
            yield row
