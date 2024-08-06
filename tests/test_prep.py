import csv
import os
import xml.etree.ElementTree as ET
from pymarc import MARCReader
import pandas as pd
import pytest

from naxos_reconcile.prep import (
    combine_naxos_xml,
    edit_naxos_xml,
    naxos_xml_to_marc,
    prep_naxos_csv,
    prep_sierra_csv,
    prep_csv_sample,
    compare_files,
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
    assert "naxos_marc_edited.xml" in test_xml
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
            urls.append(row[0])
            control_nos.append(row[2])
    assert len(urls) == 9
    assert sorted(control_nos) == [
        "58540368",
        "58540422",
        "58540422",
        "58540442",
        "58540442",
        "58540558",
        "58540630",
        "58540680",
        "58540716",
    ]


def test_prep_csv_sample(test_date_directory, mock_date_directory):
    n = 0
    out = out_file("records_to_test.csv")
    with open(out, "a") as csvfile:
        writer = csv.writer(
            csvfile,
            delimiter=",",
            lineterminator="\n",
        )
        while n < 40:
            writer.writerow(
                [
                    "http://nypl.naxosmusiclibrary.com/catalogue/item.asp?cid=00028948328901",
                    "00028948328901",
                    "1039717294",
                    "b216209031",
                ]
            )
            n += 1
    control_nos = []
    sample = prep_csv_sample(out)
    with open(sample, "r") as csvfile:
        reader = csv.reader(csvfile, delimiter=",")
        for row in reader:
            control_nos.append(row[1])
    assert len(control_nos) == 2


def test_compare_files(test_date_directory, mock_date_directory):
    sierra_df = pd.DataFrame(
        [
            [
                "123",
                "456",
                "https://nypl.naxosmusiclibrary.com/catalogue/item.asp?cid=foo",
                "foo",
            ],
            [
                "987",
                "654",
                "https://nypl.naxosmusiclibrary.com/catalogue/item.asp?cid=bar",
                "bar",
            ],
        ]
    )
    sierra_df.to_csv(out_file("sierra.csv"), index=False)
    naxos_df = pd.DataFrame(
        [
            [
                "https://nypl.naxosmusiclibrary.com/catalogue/item.asp?cid=foo",
                "foo" "foo",
            ],
            [
                "https://nypl.naxosmusiclibrary.com/catalogue/item.asp?cid=baz",
                "baz",
                "baz",
            ],
        ]
    )
    naxos_df.to_csv(out_file("naxos.csv"), index=False)
    check_file, import_file = compare_files(
        sierra_file=out_file("naxos.csv"), naxos_file=out_file("naxos.csv")
    )
    assert sorted(
        [
            "records_to_check.csv",
            "records_to_import.csv",
            "records_to_delete.csv",
            "sierra.csv",
            "naxos.csv",
        ]
    ) == sorted(os.listdir(test_date_directory))
    assert check_file == f"{test_date_directory}/records_to_check.csv"
    assert import_file == f"{test_date_directory}/records_to_import.csv"
