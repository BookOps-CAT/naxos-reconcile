from naxos_reconcile.check import (
    parse_worldcat_results,
    search_oclc_only,
    check_urls_only,
    search_oclc_check_urls,
    click_cookie,
    click_logout,
    click_homepage,
    check_cookie,
    check_logout,
)


def test_parse_worldcat_results():
    data = {
        "numberOfRecords": 1,
        "briefRecords": [
            {
                "oclcNumber": "123456789",
                "catalogingInfo": {
                    "catalogingAgency": "NAXOS",
                    "levelOfCataloging": " ",
                    "catalogingLanguage": "eng",
                },
            }
        ],
    }
    results = parse_worldcat_results(data, "123456789")
    assert results == {
        "number_of_records": 1,
        "oclc_number": "123456789",
        "record_source": "NAXOS",
    }
