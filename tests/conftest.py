import datetime
import json
import xml.etree.ElementTree as ET
from typing import Dict

from bookops_worldcat import WorldcatAccessToken
import pytest
from pymarc import Record, Field, Subfield, record_to_xml_node
import requests
import seleniumbase

# from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.support.wait import WebDriverWait

from naxos_reconcile import utils

MARC_NS = "{http://www.loc.gov/MARC21/slim}"


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


class MockTokenResponseSuccess:
    """Simulates auth server response to successful token request"""

    def __init__(self):
        self.status_code = 200

    def json(self):
        return {
            "access_token": "tk_Yebz4BpEp9dAsghA7KpWx6dYD1OZKWBlHjqW",
            "token_type": "bearer",
            "expires_in": "1199",
            "principalID": "",
            "principalIDNS": "",
            "scopes": "scope1",
            "contextInstitutionId": "00001",
            "expires_at": "2020-01-01 01:00:00Z",
        }


@pytest.fixture
def mock_creds() -> Dict[str, str]:
    return {
        "key": "my_WSkey",
        "secret": "my_WSsecret",
        "scopes": "scope1 scope2",
    }


@pytest.fixture
def mock_creds_file(mocker, mock_creds):
    m = mocker.mock_open(read_data=json.dumps(mock_creds))
    mocker.patch("builtins.open", m)
    return m


@pytest.fixture
def mock_token_response(monkeypatch, mock_creds):
    def mock_token_response_success(*args, **kwargs):
        return MockTokenResponseSuccess()

    monkeypatch.setattr(requests, "post", mock_token_response_success)
    return WorldcatAccessToken(**mock_creds)


@pytest.fixture
def test_date_directory(tmpdir):
    today = datetime.date.today()
    return tmpdir.join(f"data/files/{today}")


@pytest.fixture
def mock_date_directory(monkeypatch, test_date_directory):
    def _patch(*args, **kwargs):
        return test_date_directory

    monkeypatch.setattr(utils, "date_directory", _patch)


class MockElement:
    """Mock for selenium.webdriver.remote.webelement.WebElement"""

    def __init__(self):
        pass

    def is_displayed(self, *args, **kwargs):
        pass

    def is_enabled(self, *args, **kwargs):
        pass

    def click(self, *args, **kwargs):
        pass


class MockDriver:
    """Mock for selenium.webdriver.common.by.By"""

    def __init__(self):
        pass

    def find_element(self, *args, **kwargs):
        pass

    def uc_open(self, *args, **kwargs):
        pass


@pytest.fixture
def mock_driver(monkeypatch):
    def mock_driver_obj(*args, **kwargs):
        return MockDriver()

    monkeypatch.setattr(seleniumbase, "Driver", mock_driver_obj)
    monkeypatch.setattr(seleniumbase, "SB", mock_driver_obj)


@pytest.fixture
def stub_driver(mock_driver):
    return seleniumbase.Driver()


@pytest.fixture
def mock_wait(mock_driver):
    driver = seleniumbase.Driver()
    return WebDriverWait(driver, 2)


@pytest.fixture
def mock_available_element(monkeypatch):
    def element(*args, **kwargs):
        return MockElement()

    def success(*args, **kwargs):
        return True

    monkeypatch.setattr(MockDriver, "find_element", element)
    monkeypatch.setattr(MockElement, "is_displayed", success)
    monkeypatch.setattr(MockElement, "is_enabled", success)


@pytest.fixture
def mock_live_url(monkeypatch, mock_wait):
    def live_url(*args, **kwargs):
        def not_located(*args, **kwargs):
            return False

        monkeypatch.setattr(WebDriverWait, "until", not_located)
        if "song-play" in args:
            return MockElement()
        else:
            return MockElement()

    monkeypatch.setattr(MockDriver, "find_element", live_url)


@pytest.fixture
def mock_dead_url(monkeypatch):
    def dead_url(*args, **kwargs):
        if "notfindCon-text" in args:
            return MockElement()
        else:
            pass

    monkeypatch.setattr(MockDriver, "find_element", dead_url)


@pytest.fixture
def mock_open_file(mocker):
    m = mocker.mock_open()
    mocker.patch.object("builtins.open", m)
    return m
