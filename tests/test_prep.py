import csv
import xml.etree.ElementTree as ET
from pymarc import MARCReader
import pytest

from naxos_reconcile.prep import (
    combine_naxos_xml,
    edit_naxos_xml,
    naxos_xml_to_marc,
    prep_naxos_csv,
    prep_sierra_csv,
    prep_csv_sample,
)

from naxos_reconcile.utils import out_file

MARC_NS = "{http://www.loc.gov/MARC21/slim}"


@pytest.mark.parametrize("count", [3, 4, 5])
def test_combine_naxos_xml(
    test_marc_xml, count, test_date_directory, mock_date_directory
):
    for i in range(count):
        out = out_file(f"test_{i}.xml")
        test_marc_xml.write(
            out,
            encoding="utf-8",
            xml_declaration=True,
        )
    test_xml = combine_naxos_xml(test_date_directory)
    combined_tree = ET.parse(test_xml)
    combined_root = combined_tree.getroot()
    records = combined_root.findall(f"./{MARC_NS}record")
    assert len(records) == count


def test_edit_naxos_xml(test_marc_xml, test_date_directory, mock_date_directory):
    for i in range(3):
        out = out_file(f"test_{i}.xml")
        test_marc_xml.write(
            out,
            encoding="utf-8",
            xml_declaration=True,
        )
    file = combine_naxos_xml(test_date_directory)

    test_xml = edit_naxos_xml(file)
    test_tree = ET.parse(test_xml)
    test_root = test_tree.getroot()
    marc_urls = [
        i.text
        for i in test_root.findall(
            f".//{MARC_NS}datafield[@tag='856']/{MARC_NS}subfield[@code='u']"
        )
    ]
    marc_505s = [
        i.text for i in test_root.findall(f".//{MARC_NS}datafield[@tag='505']")
    ]
    marc_511s = [
        i.text for i in test_root.findall(f".//{MARC_NS}datafield[@tag='511']")
    ]
    assert "edited_naxos.xml" in test_xml
    assert test_root.tag == f"{MARC_NS}collection"
    assert len(marc_urls) == 6
    assert "http://nypl.naxosmusiclibrary.com/catalogue/item.asp?cid=foo" in marc_urls
    assert len(marc_505s) == 0
    assert len(marc_511s) == 0


def test_naxos_xml_to_marc(test_marc_xml, test_date_directory, mock_date_directory):
    for i in range(3):
        out = out_file(f"test_{i}.xml")
        test_marc_xml.write(
            out,
            encoding="utf-8",
            xml_declaration=True,
        )
    file = combine_naxos_xml(test_date_directory)

    test_mrc = naxos_xml_to_marc(file)
    reader = MARCReader(open(test_mrc, "rb"))
    ids = []
    for record in reader:
        ids.append(record["001"])
    assert len(ids) == 3


def test_prep_naxos_csv(test_marc_xml, test_date_directory, mock_date_directory):
    for i in range(3):
        out = out_file(f"test_{i}.xml")
        test_marc_xml.write(
            out,
            encoding="utf-8",
            xml_declaration=True,
        )
    file = combine_naxos_xml(test_date_directory)

    test_csv = prep_naxos_csv(file)
    control_nos = []
    with open(test_csv, "r") as csv_file:
        reader = csv.reader(csv_file, delimiter=",")
        for row in reader:
            control_nos.append(row[1])
    assert len(control_nos) == 6


def test_prep_sierra_csv(test_date_directory, mock_date_directory):
    file = "tests/test_csv.csv"
    test_csv = prep_sierra_csv(file)
    control_nos = []
    urls = []
    with open(test_csv, "r") as csv_file:
        reader = csv.reader(csv_file, delimiter=",")
        for row in reader:
            urls.append(row[2])
            control_nos.append(row[1])
    assert len(urls) == 9
    assert sorted(control_nos) == [
        "b218396235",
        "b218396247",
        "b218396247",
        "b218396259",
        "b218396259",
        "b218396260",
        "b218396272",
        "b218396284",
        "b218396296",
    ]


def test_prep_csv_sample(test_date_directory, mock_date_directory):
    file = "tests/test_csv.csv"
    writer = csv.writer(open(f"{test_date_directory}/test_combined.csv", "w"))
    with open(file, "r") as csvfile:
        n = 0
        reader = csv.reader(csvfile, delimiter=",")
        while n < 20:
            for row in reader:
                writer.writerow(row)
    control_nos = []
    sample = prep_csv_sample(file)
    with open(sample, "r") as csvfile:
        reader = csv.reader(csvfile, delimiter=",")
        for row in reader:
            control_nos.append(row[1])
    assert len(control_nos) == 8
