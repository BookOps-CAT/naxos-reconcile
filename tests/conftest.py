import datetime
import os
import pytest
import requests
from pymarc import Record, Field, Subfield
from naxos_reconcile import utils

# @pytest.fixture
# def mock_naxos_response


class MockNaxosResponseSuccess:
    """Simulates successful response from Naxos website for URL"""

    def __init__(self):
        self.status_code = 200


class MockNaxosResponseError:
    """Simulates unsuccessful response from Naxos website for URL"""

    def __init__(self):
        self.status_code = 404


@pytest.fixture
def mock_200_response(monkeypatch):
    def mock_naxos_response(*args, **kwargs):
        return MockNaxosResponseSuccess()

    monkeypatch.setattr(requests, "get", mock_naxos_response)


@pytest.fixture
def mock_404_response(monkeypatch):
    def mock_naxos_response(*args, **kwargs):
        return MockNaxosResponseError()

    monkeypatch.setattr(requests, "get", mock_naxos_response)


@pytest.fixture
def test_date_directory(tmpdir):
    today = datetime.date.today()
    return tmpdir.join(f"{today}")


@pytest.fixture
def mock_date_directory(monkeypatch, test_date_directory):
    def _patch(*args, **kwargs):
        return test_date_directory

    monkeypatch.setattr(utils, "date_directory", _patch)


@pytest.fixture
def mock_marc():
    record = Record()
    record.add_field(
        Field(tag="001", data="123456789"),
        Field(
            tag="035",
            indicators=[" ", " "],
            subfields=[Subfield(code="a", value="(OCoLC)ocm1234567")],
        ),
        Field(
            tag="505",
            indicators=[" ", " "],
            subfields=[Subfield(code="a", value="Very long formatted contents note")],
        ),
        Field(
            tag="856",
            indicators=["4", "0"],
            subfields=[
                Subfield(
                    code="u",
                    value="https://nypl.naxosmusiclibrary.com/catalogue/item.asp?cid=foo",
                )
            ],
        ),
    )
    return record
