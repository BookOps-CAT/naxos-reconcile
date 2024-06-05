import csv
import json
import datetime
import itertools
import os
from bookops_worldcat import WorldcatAccessToken


MARC_URI = "http://www.loc.gov/MARC21/slim"
MARC_NS = "{http://www.loc.gov/MARC21/slim}"


def get_file_length(file: str) -> int:
    """
    Reads a csv file and returns the number of rows in the file

    Args:
        file: path to location of csv file

    Returns:
        length of csv file as integer
    """
    rows = []
    with open(file, "r") as infile:
        for row in infile:
            rows.append(row)
    return len(rows)


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


def get_token(filepath: str) -> WorldcatAccessToken:
    """get token for worldshare searches"""
    with open(filepath, "r") as file:
        data = json.load(file)
        return WorldcatAccessToken(
            key=data["key"],
            secret=data["secret"],
            scopes=data["scopes"],
        )


# def sample_csv(infile: str, outfile: str, header: Optional[int]) -> str:
#     """
#     Read a .csv into a pd.DataFrame and return a sample of the data.
#     Sample will contain 5% of the rows of the infile.
#     Sample will be output to a .csv file and returned as a DataFrame.

#     Args:
#         infile: name of .csv file to read
#         outfile: name of .csv file to output sample to
#         header: if the .csv file contains a header, the header row as an int

#     Returns:
#         the name of the file that the sample was output to
#     """
#     sample_out = out_file(outfile)
#     if header:
#         in_df = pd.read_csv(
#             infile,
#             header=header,
#             dtype=str,
#         )
#     else:
#         in_df = pd.read_csv(infile, dtype=str)
#     sample_df = in_df.sample(frac=0.05)
#     sample_df.to_csv(sample_out, index=False)
#     return sample_out


# def sample_marc(infile: str, outfile: str) -> str:
#     """
#     Read a marc file and create another marc file with 5% of the original file

#     Args:
#         infile: name of .mrc file to read
#         outfile: name of .mrc file to output sample to
#         header: if the .csv file contains a header, the header row as an int

#     Returns:
#         the name of the file that the sample was output to
#     """
#     sample_out = out_file(outfile)
#     records = parse_xml_to_array(open(infile, "rb"))
#     writer = MARCWriter(open(sample_out, "wb"))
#     file_length = len(records)
#     for record in itertools.islice(records, 0, file_length, 20):
#         writer.write(record)
#     writer.close()
#     return sample_out


def open_csv_file(file: str, skip_rows: int):
    """"""
    with open(file, "r") as csv_file:
        reader = csv.reader(csv_file, delimiter=",")
        for row in itertools.islice(reader, skip_rows, None):
            yield row
