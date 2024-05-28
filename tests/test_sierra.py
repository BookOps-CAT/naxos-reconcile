import csv
import pytest

from naxos_reconcile.sierra import NaxosRecord


def test_NaxosRecord_200(mock_200_response):
    csv_record = NaxosRecord(
        control_no="12345",
        bib_id="b98765",
        url="https://nypl.naxosmusiclibrary.com/catalogue/item.asp?cid=foo",
    )
    assert csv_record.control_no == "12345"
    assert csv_record.bib_id == "b98765"
    assert (
        csv_record.url
        == "https://nypl.naxosmusiclibrary.com/catalogue/item.asp?cid=foo"
    )
    assert csv_record.cid == "foo"
    assert csv_record.status == 200


def test_NaxosRecord_404(mock_404_response):
    csv_record = NaxosRecord(
        control_no="98765",
        bib_id="b12345",
        url="https://nypl.naxosmusiclibrary.com/catalogue/item.asp?cid=bar",
    )
    assert csv_record.control_no == "98765"
    assert csv_record.bib_id == "b12345"
    assert (
        csv_record.url
        == "https://nypl.naxosmusiclibrary.com/catalogue/item.asp?cid=bar"
    )
    assert csv_record.cid == "bar"
    assert csv_record.status == 404


def test_data_from_csv(mock_200_response):
    records = []
    with open("tests/test_csv.csv", "r") as csvfile:
        reader = csv.reader(
            csvfile,
        )
        for record in reader:
            records.append(record)
    csv_record = NaxosRecord.data_from_csv(records[1])
    assert csv_record.control_no == "58540368"
    assert csv_record.bib_id == "b218396235"
    assert (
        csv_record.url
        == "https://nypl.naxosmusiclibrary.com/catalogue/item.asp?cid=11026-2"
    )
    assert csv_record.cid == "11026-2"
    assert csv_record.status == 200


def test_process_naxos_xml(test_date_directory, mock_date_directory):
    file = "tests/test.xml"
    test_mrc = NaxosRecord.process_naxos_xml(file)
    test_csv = NaxosRecord.extract_naxos_data(test_mrc)
    assert test_mrc == f"{test_date_directory}/naxos-prepped.mrc"
    assert test_csv == f"{test_date_directory}/naxos-prepped-data.csv"


def test_process_sierra_export(test_date_directory, mock_date_directory):
    file = "tests/test_csv.csv"
    testing = NaxosRecord.process_sierra_export(file)
    assert testing == f"{test_date_directory}/sierra-prepped-data.csv"
