import os
import csv
import xml.etree.ElementTree as ET

from pymarc import MARCWriter, parse_xml_to_array

from naxos_reconcile.utils import out_file, save_csv

MARC_NS = "{http://www.loc.gov/MARC21/slim}"


def combine_naxos_xml(dir: str) -> str:
    """
    Reads Naxos MARC/XML files from Naxos and combines them into a single file.
    Removes 505 fields from records to make records shorter for pymarc.

    Args:

        file: file path for MARCXML files to process
    Returns:

        name of processed .xml file as str

    """
    combined_xml = out_file("combined_naxos.xml")

    ET.register_namespace("marc", MARC_NS)
    ET.register_namespace("xsi", "http://www.w3.org/2001/XMLSchema-instance")
    root = ET.Element(f"{MARC_NS}collection")
    root.set(
        "{http://www.w3.org/2001/XMLSchema-instance}schemaLocation",
        f"{MARC_NS} http://www.loc.gov/standards/marcxml/schema/MARC21slim.xsd",
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
    Reads combined MARC/XML file and edits fields.
    Removes 505 to shorten records and edits url in 856$u.
    Writes records to new file and any records missing 856 fields
    are written to an error file.
    """
    edited_xml = out_file("edited_naxos.xml")

    ET.register_namespace("marc", MARC_NS)
    tree = ET.parse(file)
    root = tree.getroot()

    for record in root.findall(f"./{MARC_NS}record"):
        marc_505 = record.findall(f"./{MARC_NS}datafield[@tag='505']")
        for field_505 in marc_505:
            record.remove(field_505)
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
    Reads Naxos MARC/XML file and converts it to MARC21.

    Args:

        file: file path for MARCXML file to process
    Returns:

        name of processed .mrc file as str
    """
    naxos_marc_processed = out_file("converted_naxos.mrc")
    records = parse_xml_to_array(open(file, "rb"))
    writer = MARCWriter(open(naxos_marc_processed, "wb"))
    for record in records:
        writer.write(record)
    writer.close()
    return naxos_marc_processed


def prep_naxos_csv(file: str) -> str:
    naxos_csv = out_file("prepped_naxos_data.csv")

    tree = ET.parse(file)
    root = tree.getroot()
    for record in root.findall(f"./{MARC_NS}record"):
        control_no = [
            i.text for i in record.findall(f"./{MARC_NS}controlfield[@tag='001']")
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
                        control_no[0],
                        str(url.split("?cid=")[1].strip()),
                    ],
                )
    return naxos_csv


def prep_sierra_csv(infile: str) -> str:
    """
    Reads a csv file and splits rows with multiple urls into separate rows.
    URLs must be separated by a ";" and the other fields must be separated
    by another delimiter. The processed data is written to a new file.

    Args:
        infile: the path to the file to process

    Returns:
        name of outfile as str
    """
    processed_sierra_file = out_file("prepped_sierra_data.csv")
    with open(infile, "r", encoding="utf-8") as csvfile:
        reader = csv.reader(csvfile, delimiter=",")
        next(reader)
        for row in reader:
            oclc_no = row[0]
            bib_id = row[1]
            if "?cid=" and ";" in row[2]:
                urls = [i for i in row[2].split(";") if "?cid=" in i]
                for url in urls:
                    save_csv(
                        processed_sierra_file,
                        [oclc_no, bib_id, url, str(url.split("?cid=")[1].strip())],
                    )
            elif "?cid=" in row[2] and ";" not in row[2]:
                url = row[2]
                save_csv(
                    processed_sierra_file,
                    [oclc_no.strip(), bib_id, url, str(url.split("?cid=")[1].strip())],
                )
    return processed_sierra_file
