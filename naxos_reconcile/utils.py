import csv
import datetime
import os


def get_file_length(file: str) -> int:
    """
    Reads a csv file and returns the number of rows in the file

    Args:
        file: path to location of csv file

    Returns:
        length of csv file as integer (does not count header row)
    """
    rows = []
    with open(file, "r") as infile:
        for row in infile:
            rows.append(row)
    return len(rows) - 1


def date_directory() -> str:
    """create directory for output files using today's date"""
    date = datetime.date.today()
    return f"./data/files/{str(date)}"


def out_file(file: str) -> str:
    """create file for output"""
    if not os.path.exists(date_directory()):
        os.makedirs(date_directory())
    return f"{date_directory()}/{file}"


def save_csv(outfile: str, row: list) -> None:
    """save output to a csv"""
    with open(outfile, "a", encoding="utf-8") as csvfile:
        out = csv.writer(
            csvfile,
            delimiter=",",
            lineterminator="\n",
        )
        out.writerow(row)
