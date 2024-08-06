import datetime
import xml.etree.ElementTree as ET
import pytest
import requests
from pymarc import Record, Field, Subfield, record_to_xml_node
from naxos_reconcile import utils

MARC_NS = "{http://www.loc.gov/MARC21/slim}"


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
    return tmpdir.join(f"data/files/{today}")


@pytest.fixture
def mock_date_directory(monkeypatch, test_date_directory):
    def _patch(*args, **kwargs):
        return test_date_directory

    monkeypatch.setattr(utils, "date_directory", _patch)


@pytest.fixture
def test_marc_xml() -> ET.ElementTree:
    ET.register_namespace("marc", MARC_NS)
    ET.register_namespace("xsi", "http://www.w3.org/2001/XMLSchema-instance")
    root = ET.Element(f"{MARC_NS}collection")
    root.set(
        "{http://www.w3.org/2001/XMLSchema-instance}schemaLocation",
        "{MARC_NS} http://www.loc.gov/standards/marcxml/schema/MARC21slim.xsd",
    )
    tree = ET.ElementTree(root)

    marc21 = Record()
    marc21.add_field(
        Field(tag="001", data="123456789"),
        Field(
            tag="024",
            indicators=["1", "1"],
            subfields=[Subfield(code="a", value="0123456789")],
        ),
        Field(
            tag="245",
            indicators=["0", "0"],
            subfields=[
                Subfield(code="a", value="Foo"),
            ],
        ),
        Field(
            tag="260",
            indicators=[" ", " "],
            subfields=[
                Subfield(code="a", value="Hong Kong :"),
                Subfield(code="b", value="Naxos Digital Services US Inc."),
            ],
        ),
        Field(
            tag="490",
            indicators=["1", " "],
            subfields=[Subfield(code="a", value="Naxos Music Library")],
        ),
        Field(
            tag="505",
            indicators=["0", "0"],
            subfields=[Subfield(code="a", value="Very long formatted contents field")],
        ),
        Field(
            tag="511",
            indicators=["0", " "],
            subfields=[Subfield(code="a", value="Very long list of performers")],
        ),
        Field(
            tag="856",
            indicators=["4", "0"],
            subfields=[
                Subfield(
                    code="u",
                    value="http://univportal.naxosmusiclibrary.com/catalogue/item.asp?cid=bar",
                )
            ],
        ),
        Field(
            tag="856",
            indicators=["4", "0"],
            subfields=[
                Subfield(
                    code="u",
                    value="http://univportal.naxosmusiclibrary.com/catalogue/item.asp?cid=foo",
                )
            ],
        ),
    )
    root.append(record_to_xml_node(marc21, namespace=True))
    return tree
