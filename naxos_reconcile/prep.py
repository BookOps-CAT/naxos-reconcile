import os
import csv
import random
import xml.etree.ElementTree as ET

import pandas as pd
from pymarc import MARCWriter, parse_xml_to_array

from naxos_reconcile.utils import out_file, save_csv, get_file_length, date_directory

MARC_URI = "http://www.loc.gov/MARC21/slim"
MARC_NS = "{http://www.loc.gov/MARC21/slim}"


def combine_naxos_xml(dir: str) -> str:
    """
    Reads MARC/XML files from Naxos and combines them into a single .xml file.

    Args:
        file: path to MARC/XML files to process

    Returns:
        name of combined .xml file as str
    """
    combined_xml = out_file("naxos_marc_combined.xml")

    ET.register_namespace("marc", MARC_URI)
    ET.register_namespace("xsi", "http://www.w3.org/2001/XMLSchema-instance")
    root = ET.Element(f"{MARC_NS}collection")
    root.set(
        "{http://www.w3.org/2001/XMLSchema-instance}schemaLocation",
        f"{MARC_URI} http://www.loc.gov/standards/marcxml/schema/MARC21slim.xsd",
    )
    combined_tree = ET.ElementTree(root)

    file_list = os.listdir(dir)
    for file in file_list:
        if ".xml" in file:
            tree = ET.parse(f"{dir}/{file}")
            file_root = tree.getroot()
            for record in file_root.findall(f"./{MARC_NS}record"):
                root.append(record)
    combined_tree.write(
        combined_xml,
        encoding="utf-8",
        xml_declaration=True,
    )
    return combined_xml


def edit_naxos_xml(file: str) -> str:
    """
    Reads combined MARC/XML file and edits file. Removes 505 and 511
    fields to shorten records and edits url in 856$u. Writes edited
    records to new .xml file.

    Args:
        file: path to combined MARCXML file to edit

    Returns:
        name of edited .xml file as str
    """
    edited_xml = out_file("naxos_marc_edited.xml")

    ET.register_namespace("marc", MARC_URI)
    tree = ET.parse(file)
    root = tree.getroot()

    for record in root.findall(f"./{MARC_NS}record"):
        marc_505 = record.findall(f"./{MARC_NS}datafield[@tag='505']")
        for field_505 in marc_505:
            record.remove(field_505)
        marc_511 = record.findall(f"./{MARC_NS}datafield[@tag='511']")
        for field_511 in marc_511:
            record.remove(field_511)
        marc_urls = [
            i
            for i in record.findall(
                f"./{MARC_NS}datafield[@tag='856']/{MARC_NS}subfield[@code='u']"
            )
        ]
        for url in marc_urls:
            if url.text is not None and isinstance(url.text, str):
                new_url = url.text.replace("univportal", "nypl")
                url.text = new_url
    tree.write(
        edited_xml,
        encoding="utf-8",
        xml_declaration=True,
    )
    return edited_xml


def naxos_xml_to_marc(file: str) -> str:
    """
    Reads combined, edited Naxos MARC/XML file and writes it to MARC21.

    Args:
        file: path to MARCXML file to process

    Returns:
        name of .mrc file as str
    """
    naxos_marc_processed = out_file("naxos_marc_edited.mrc")
    records = parse_xml_to_array(open(file, "rb"))
    writer = MARCWriter(open(naxos_marc_processed, "wb"))
    for record in records:
        writer.write(record)
    writer.close()
    return naxos_marc_processed


def prep_naxos_csv(file: str) -> str:
    """
    Reads naxos edited .xml and outputs data to .csv. Output contains:
    - URL
    - CID
    - Title
    - Publisher (from 260$b)
    - Series

    Args:
        file: path to MARCXML file to process

    Returns:
        name of .csv file as str
    """
    naxos_csv = out_file("prepped_naxos_input.csv")
    tree = ET.parse(file)
    root = tree.getroot()
    for record in root.findall(f"./{MARC_NS}record"):
        control_no = [
            i.text for i in record.findall(f"./{MARC_NS}controlfield[@tag='001']")
        ]
        title = [
            i.text
            for i in record.findall(
                f"./{MARC_NS}datafield[@tag='245']/{MARC_NS}subfield[@code='a']"
            )
        ]
        publisher = [
            i.text
            for i in record.findall(
                f"./{MARC_NS}datafield[@tag='260']/{MARC_NS}subfield[@code='b']"
            )
        ]
        series = [
            i.text
            for i in record.findall(
                f"./{MARC_NS}datafield[@tag='490']/{MARC_NS}subfield[@code='a']"
            )
        ]
        urls = [
            i.text
            for i in record.findall(
                f"./{MARC_NS}datafield[@tag='856']/{MARC_NS}subfield[@code='u']"
            )
            if i.text is not None and "?cid=" in i.text
        ]
        if len(control_no) == 1 and len(urls) >= 1:
            for url in urls:
                save_csv(
                    naxos_csv,
                    [
                        url,
                        str(url.split("?cid=")[1].strip()),
                        title[0],
                        publisher[0],
                        series[0],
                    ],
                )
    return naxos_csv


def prep_sierra_csv(infile: str) -> str:
    """
    Reads a csv file and splits rows with multiple urls into separate rows.
    URLs must be separated by a semicolon and the other fields must be
    separated by a comma. The processed data is written to a new file.

    Args:
        infile: path to Sierra export file to process

    Returns:
        name of .csv as str
    """
    processed_sierra_file = out_file("prepped_sierra_input.csv")
    with open(infile, "r", encoding="utf-8") as csvfile:
        reader = csv.reader(csvfile, delimiter=",")
        for row in reader:
            oclc_no = row[0]
            bib_id = row[1]
            if "?cid=" and ";" in row[2]:
                urls = [i for i in row[2].split(";") if "?cid=" in i]
                for url in urls:
                    save_csv(
                        processed_sierra_file,
                        [
                            url,
                            str(url.split("?cid=")[1].strip()),
                            oclc_no.strip().strip("(OCoLC)"),
                            bib_id,
                        ],
                    )
            elif "?cid=" in row[2] and ";" not in row[2]:
                url = row[2]
                save_csv(
                    processed_sierra_file,
                    [
                        url,
                        str(url.split("?cid=")[1].strip()),
                        oclc_no.strip().strip("(OCoLC)"),
                        bib_id,
                    ],
                )
    return processed_sierra_file


def prep_csv_sample(infile: str) -> str:
    """
    Read a .csv and returns a sample of the data as a new .csv.
    Outputs 5% of rows using a random selection.

    Args:
        infile: full path to .csv file to read

    Returns:
        the name of the file that the sample was output to
    """
    sample_out = out_file(f"sample{infile.split('records')[1].split('.csv')[0]}")
    file_length = get_file_length(f"{date_directory()}/{infile}")
    sample_count = round(file_length / 20)
    with open(f"{date_directory()}/{infile}", "r", encoding="utf-8") as csvfile:
        reader = csv.reader(csvfile, delimiter=",")
        next(reader)

        sample = random.choices(list(reader), k=sample_count)
        for row in sample:
            out_row = [i for i in row]
            save_csv(sample_out, out_row)
    return sample_out


def compare_files(sierra_file: str, naxos_file: str) -> None:
    """
    Compare prepped Naxos and Sierra files. Joins .csv files on CID
    and creates three output files:
    - records_to_check.csv: overlap between Sierra and Naxos data
    - records_to_import.csv: records in Naxos xml that are not in Sierra
    - records_to_delete.csv: records in Sierra that are not in Naxos xml

    URLs from records_to_check.csv and records_to_import.csv should be
    reviewed to identify dead links.

    Args:
        sierra_file: path to prepped sierra .csv file
        naxos_file: path to prepped naxos .csv file
    """
    # create output files
    check_csv = out_file("records_to_check.csv")
    delete_csv = out_file("records_to_delete.csv")
    import_csv = out_file("records_to_import.csv")

    # read input files into dataframes
    sierra_df = pd.read_csv(
        sierra_file,
        header=None,
        names=[
            "URL_SIERRA",
            "CID_SIERRA",
            "OCLC_NUMBER",
            "BIB_ID",
        ],
        dtype=str,
    )
    naxos_df = pd.read_csv(
        naxos_file,
        header=None,
        names=["URL_NAXOS", "CID_NAXOS", "TITLE", "PUBLISHER", "SERIES"],
        dtype=str,
    )

    # drop duplicate rows, just in case
    sierra_df.drop_duplicates(inplace=True)
    naxos_df.drop_duplicates(inplace=True)

    # merge dataframes with an inner join and write matched rows to file
    check_df = sierra_df.merge(naxos_df, left_on="CID_SIERRA", right_on="CID_NAXOS")
    check_df.to_csv(
        check_csv,
        index=False,
        columns=[
            "URL_NAXOS",
            "CID_SIERRA",
            "OCLC_NUMBER",
            "BIB_ID",
        ],
        header=False,
    )
    print(f"urls to check in {check_csv}")

    # merge dataframes with an outer join to identify unique rows
    unique_df = sierra_df.merge(
        naxos_df,
        how="outer",
        indicator=True,
        left_on="CID_SIERRA",
        right_on="CID_NAXOS",
    )

    # write rows only in sierra input to file
    sierra_unique = unique_df[unique_df["_merge"] == "left_only"]
    sierra_unique.to_csv(
        delete_csv,
        index=False,
        columns=["URL_SIERRA", "CID_SIERRA", "OCLC_NUMBER", "BIB_ID"],
        header=False,
    )
    print(f"records to delete in {delete_csv}")

    # write rows only in naxos input to file
    naxos_unique = unique_df[unique_df["_merge"] == "right_only"]
    naxos_unique.to_csv(
        import_csv,
        index=False,
        columns=[
            "URL_NAXOS",
            "CID_NAXOS",
            "TITLE",
            "PUBLISHER",
            "SERIES",
        ],
        header=False,
    )
    print(f"records to import in {import_csv}")
