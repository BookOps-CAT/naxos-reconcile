from contextlib import nullcontext as does_not_raise
import pytest
from naxos_reconcile.check import (
    parse_worldcat_results,
    search_oclc_only,
    # check_urls_only,
    # search_oclc_check_urls,
    click_cookie,
    click_logout,
    click_homepage,
    check_cookie,
    check_logout,
    get_selenium_status,
)


@pytest.mark.parametrize(
    "record_count, oclc_number, agency, output",
    [
        (
            1,
            "1234",
            "FOO",
            {
                "number_of_records": 1,
                "oclc_number": "1234",
                "record_source": "FOO",
            },
        ),
        (
            3,
            "5678",
            "BAR",
            {"number_of_records": 3, "oclc_number": "5678", "record_source": "BAR"},
        ),
    ],
)
def test_parse_worldcat_results_oclc_match(record_count, oclc_number, agency, output):
    data = {
        "numberOfRecords": record_count,
        "briefRecords": [
            {
                "oclcNumber": oclc_number,
                "catalogingInfo": {
                    "catalogingAgency": agency,
                    "levelOfCataloging": " ",
                    "catalogingLanguage": "eng",
                },
            }
        ],
    }
    results = parse_worldcat_results(data, "123456789")
    assert results == output


def test_parse_worldcat_results_zero_records():
    data = {"numberOfRecords": 0}
    results = parse_worldcat_results(data, "123456789")
    assert results == {
        "number_of_records": 0,
        "oclc_number": None,
        "record_source": None,
    }


def test_parse_worldcat_results_multiple_records():
    data = {
        "numberOfRecords": 2,
        "briefRecords": [
            {
                "oclcNumber": "123456789",
                "catalogingInfo": {
                    "catalogingAgency": "FOO",
                    "levelOfCataloging": "M",
                    "catalogingLanguage": "eng",
                },
            },
            {
                "oclcNumber": "987654321",
                "catalogingInfo": {
                    "catalogingAgency": "NAXOS",
                    "levelOfCataloging": " ",
                    "catalogingLanguage": "eng",
                },
            },
        ],
    }
    results = parse_worldcat_results(data, "111111111")
    assert results == {
        "number_of_records": 2,
        "oclc_number": "987654321",
        "record_source": "NAXOS",
    }


def test_click_homepage(mock_wait, mock_available_element):
    with does_not_raise():
        click_homepage(wait=mock_wait)


def test_click_cookie(mock_wait, mock_available_element):
    with does_not_raise():
        click_cookie(wait=mock_wait)


def test_click_logout(mock_wait, mock_available_element):
    with does_not_raise():
        click_logout(wait=mock_wait)


def test_check_cookie_button_clicked(mock_wait, mock_available_element):
    with does_not_raise():
        check_cookie(wait=mock_wait)


def test_check_cookie_button_not_present(mock_button, mock_wait):
    mock_button.check_cookie(wait=mock_wait)
    mock_button.click.assert_not_called()


def test_check_logout_button_clicked(mock_wait, mock_available_element):
    with does_not_raise():
        check_logout(wait=mock_wait)


def test_check_logout_button_not_present(mock_button, mock_wait):
    mock_button.check_logout(wait=mock_wait)
    mock_button.click.assert_not_called()


def test_get_selenium_status_live(stub_driver, mock_wait, mock_live_url):
    assert (
        get_selenium_status(driver=stub_driver, wait=mock_wait, url="foo.com") == "Live"
    )


def test_get_selenium_status_dead(stub_driver, mock_wait, mock_dead_url):
    assert (
        get_selenium_status(driver=stub_driver, wait=mock_wait, url="foo.com") == "Dead"
    )


# def test_search_oclc_only(mock_open_file):
#     file = search_oclc_only("test_csv.csv", 0)
#     assert file == "test_csv_search_results.csv"
