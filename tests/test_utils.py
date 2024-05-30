import datetime
import csv
import pytest

from naxos_reconcile.utils import get_file_length, date_directory, out_file, save_csv


def test_get_file_length():
    file_length = get_file_length("tests/test_csv.csv")
    assert file_length == 7


def test_date_directory():
    today = datetime.date.today()
    assert date_directory() == f"./data/files/{today}"


@pytest.mark.parametrize("file", ["foo", "bar"])
def test_create_out_file(file, test_date_directory, mock_date_directory):
    filepath = out_file(file)
    assert filepath == f"{test_date_directory}/{file}"


def test_save_csv(tmpdir):
    outrow = ["CONTROL_NO", "BIB_ID", "URL", "CID"]
    save_csv(tmpdir.join("test.csv"), outrow)
    with open(tmpdir.join("test.csv"), "r") as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            assert row[0] == "CONTROL_NO"
            assert row[1] == "BIB_ID"
            assert row[2] == "URL"
            assert row[3] == "CID"
