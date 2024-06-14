import os
import time
from typing import Optional
from bookops_worldcat import MetadataSession
from seleniumbase import Driver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    NoSuchElementException,
    ElementNotInteractableException,
    TimeoutException,
)
from naxos_reconcile.utils import (
    get_token,
    out_file,
    save_csv,
    open_csv_file,
    get_file_length,
    date_directory,
)

ERRORS = [NoSuchElementException, ElementNotInteractableException, TimeoutException]


def parse_worldcat_results(data: dict, oclc_num: Optional[str] = None) -> dict:
    """
    Parse data returned by WorldCat Metadata API brief_bibs_search query.
    Identifies best OCLC number based on:
        - number of records returned,
        - whether the OCLC number matches the 001 from the Sierra record,
        - whether the cataloging agency is Naxos, and
        - whether the OCLC number is for a full record
    If results do not match any of the above criteria or if there are multiple
    records matching the criteria, a list of OCLC numbers will be returned.

    Args:
        data: json data returned by query to Metadata API
        oclc_num: OCLC number from Sierra 001 field, if applicable
    Returns:
        dict containing:
        - total number of records returned by API query
        - oclc_number (as str or list),
        - cataloging source (as str or list)
        - whether the OCLC number matches the 001 from Sierra, if applicable
    """
    out_dict = {"number_of_records": data["numberOfRecords"]}
    match data:
        case {"numberOfRecords": 1, "briefRecords": [{"oclcNumber": oclc_num}]}:
            (
                out_dict["oclc_number"],
                out_dict["record_source"],
                out_dict["oclc_match"],
            ) = (
                data["briefRecords"][0]["oclcNumber"],
                data["briefRecords"][0]["catalogingInfo"]["catalogingAgency"],
                True,
            )
            return out_dict
        case {"numberOfRecords": 1} if oclc_num:
            (
                out_dict["oclc_number"],
                out_dict["record_source"],
                out_dict["oclc_match"],
            ) = (
                data["briefRecords"][0]["oclcNumber"],
                data["briefRecords"][0]["catalogingInfo"]["catalogingAgency"],
                False,
            )
            return out_dict
        case {"numberOfRecords": 1} if not oclc_num:
            out_dict["oclc_number"], out_dict["record_source"] = (
                data["briefRecords"][0]["oclcNumber"],
                data["briefRecords"][0]["catalogingInfo"]["catalogingAgency"],
            )
            return out_dict
        case {"numberOfRecords": 0} if oclc_num:
            (
                out_dict["oclc_number"],
                out_dict["record_source"],
                out_dict["oclc_match"],
            ) = (None, None, False)
            return out_dict
        case {"numberOfRecords": 0} if not oclc_num:
            out_dict["oclc_number"], out_dict["record_source"] = None, None
            return out_dict
        case {
            "numberOfRecords": number,
            "briefRecords": [{"oclcNumber": oclc_num}],
        } if number > 1:
            (
                out_dict["oclc_number"],
                out_dict["record_source"],
                out_dict["oclc_match"],
            ) = (
                [
                    i["oclcNumber"]
                    for i in data["briefRecords"]
                    if i["oclcNumber"] == oclc_num
                ][0],
                [
                    i["catalogingInfo"]["catalogingAgency"]
                    for i in data["briefRecords"]
                    if i["oclcNumber"] == oclc_num
                ][0],
                True,
            )
            return out_dict
        case _:
            pass
    bibs = [
        i
        for i in data["briefRecords"]
        if i["catalogingInfo"]["catalogingLanguage"] == "eng"
    ]
    if oclc_num:
        out_dict["oclc_match"] = False
    naxos_bibs = [i for i in bibs if i["catalogingInfo"]["catalogingAgency"] == "NAXOS"]
    full_bibs = [
        i
        for i in bibs
        if i not in naxos_bibs
        and (
            i["catalogingInfo"]["levelOfCataloging"] == " "
            or i["catalogingInfo"]["levelOfCataloging"] == "1"
        )
    ]
    other_bibs = [i for i in bibs if i not in full_bibs and i not in naxos_bibs]
    if len(naxos_bibs) == 1:
        out_dict["oclc_number"], out_dict["record_source"] = (
            naxos_bibs[0]["oclcNumber"],
            naxos_bibs[0]["catalogingInfo"]["catalogingAgency"],
        )
    elif len(full_bibs) == 1:
        out_dict["oclc_number"], out_dict["record_source"] = (
            full_bibs[0]["oclcNumber"],
            full_bibs[0]["catalogingInfo"]["catalogingAgency"],
        )
    elif len(other_bibs) == 1:
        out_dict["oclc_number"], out_dict["record_source"] = (
            other_bibs[0]["oclcNumber"],
            other_bibs[0]["catalogingInfo"]["catalogingAgency"],
        )
    elif len(naxos_bibs) > 1:
        out_dict["oclc_number"], out_dict["record_source"] = [
            i["oclcNumber"] for i in naxos_bibs
        ], [i["catalogingInfo"]["catalogingAgency"] for i in naxos_bibs]
    elif len(full_bibs) > 1:
        out_dict["oclc_number"], out_dict["record_source"] = [
            i["oclcNumber"] for i in full_bibs
        ], [i["catalogingInfo"]["catalogingAgency"] for i in full_bibs]
    else:
        out_dict["oclc_number"], out_dict["record_source"] = [
            i["oclcNumber"] for i in other_bibs
        ], [i["catalogingInfo"]["catalogingAgency"] for i in other_bibs]
    return out_dict


def search_oclc_check_urls(infile: str, last_row: int) -> str:
    """
    Search worldcat for brief bibs and check if URLs are live
    Outputs a .csv file with:
    - number of records returned by query to Metadata API
    - OCLC number(s),
    - source(s) of catalog record
    - status of URL (Live, Dead, Unavailable, Blocked, or Unknown)

    Args:
        infile:
            filename for file to be used in queries
        last_row:
            the last row from infile to be checked.
            CLI command will default to 0 if not provided
    Returns:
        name of .csv outfile as str
    """
    # outfile = out_file(f"{infile.split('.csv')[0]}_results.csv")
    # n = last_row + 1
    # data = open_csv_file(f"{date_directory()}/{infile}", last_row)
    # file_length = get_file_length(f"{date_directory()}/{infile}")
    outfile = "data/files/2024-06-13/sample_to_check_results.csv"
    n = last_row + 1
    data = open_csv_file("data/files/2024-06-13/sample_to_check.csv", last_row)
    file_length = get_file_length("data/files/2024-06-13/sample_to_check.csv")
    token = get_token(os.path.join(os.environ["USERPROFILE"], ".oclc/nyp_wc_test.json"))
    driver = Driver(uc=True, headless=True)
    with MetadataSession(authorization=token, totalRetries=3) as session:
        for row in data:
            start = time.perf_counter()
            naxos_search = session.brief_bibs_search(
                q=f"mn='{row[1]}'",
                itemSubType="music-digital",
            )
            naxos_json = naxos_search.json()  # type: ignore[union-attr]
            parsed_data = parse_worldcat_results(data=naxos_json, oclc_num=row[2])
            status = selenium_get_url_status(driver=driver, url=row[0])
            row.extend(
                [
                    parsed_data["number_of_records"],
                    parsed_data["oclc_number"],
                    parsed_data["record_source"],
                    status,
                ]
            )
            save_csv(outfile, row)
            stop = time.perf_counter()
            print(f"Record {n} of {file_length}. Search took {stop-start:0.4f} seconds")
            print(
                f"URL is {status}. "
                f"{parsed_data['number_of_records']} record(s) in OCLC."
            )
            n += 1
    return outfile


def oclc_title_search(infile: str, last_row: int) -> str:
    """
    Search worldcat for brief bibs using title and series data from
    records_to_import.csv or sample_to_import.csv files. This can be used if
    the first Metadata API queries did not identify records for the resource.
    Outputs a .csv file with:
    - number of records returned by query to Metadata API
    - OCLC number(s),
    - source(s) of catalog record
    - status of URL (Live, Dead, Unavailable, Blocked, or Unknown)

    Args:
        infile:
            filename for file to be used in queries
        last_row:
            the last row from infile to be checked.
            CLI command will default to 0 if not provided
    Returns:
        name of .csv outfile as str
    """
    outfile = out_file(f"{infile.split('_results.csv')[0]}title_search_results.csv")
    n = last_row + 1
    data = open_csv_file(f"{date_directory()}/{infile}", last_row)
    file_length = get_file_length(f"{date_directory()}/{infile}")
    token = get_token(os.path.join(os.environ["USERPROFILE"], ".oclc/nyp_wc_test.json"))
    with MetadataSession(authorization=token, totalRetries=3) as session:
        for row in data:
            if row[5] == 0 and row[6] is None:
                naxos_search = session.brief_bibs_search(
                    q=f"se='{row[4]}' AND ti='{row[2]}*'",
                    itemSubType="music-digital",
                )
                naxos_title_json = naxos_search.json()  # type: ignore[union-attr]
                parsed_data = parse_worldcat_results(
                    data=naxos_title_json,
                )
            del row[5:9]
            row.extend(
                [
                    parsed_data["number_of_records"],
                    parsed_data["oclc_number"],
                    parsed_data["record_source"],
                ]
            )
            save_csv(outfile, row)
            print(f"Record {n} of {file_length}.")
            print(f"{parsed_data['number_of_records']} record(s) in OCLC.")
            n += 1
    return outfile


def click_button(web_driver: Driver, button_type: str) -> None:
    """
    Given the type of button to click, wait until it is available
    on a page and then click it.
    """
    wait = WebDriverWait(web_driver, 5, ignored_exceptions=ERRORS)
    if button_type == "cookie":
        button = wait.until(EC.element_to_be_clickable((By.ID, "cmpwelcomebtnyes")))
    elif button_type == "homepage":
        button = wait.until(
            EC.element_to_be_clickable((By.CLASS_NAME, "notfindCon-btn"))
        )
    else:
        button = wait.until(
            EC.element_to_be_clickable((By.XPATH, "//div[@class='head-use']/a"))
        )
    button.click()


def check_cookie(driver: Driver) -> None:
    """
    Check if a cookie button is present on a page and click it.
    If the button is not present on the page, do not wait for it.
    """
    wait = WebDriverWait(driver, 5, ignored_exceptions=ERRORS)
    try:
        cookie = wait.until(
            EC.none_of(EC.presence_of_element_located((By.ID, "cmpwelcomebtnyes")))
        )
        if cookie is True:
            pass
    except TimeoutException:
        click_button(web_driver=driver, button_type="cookie")


def selenium_get_url_status(driver: Driver, url: str) -> str:
    """
    Given a URL, try to connect to the page and identify if the resource is
    available based on content of html. The web driver will wait for the page
    to load and for specific html tags to be located. The status of the web
    resource is identified based on the presence of those tags. This function
    uses SeleniumBase UC mode to appear human to Naxos.

    Possible statuses:
    - Live: the resource is available if "@class='song-play'" is on the page,
    - Dead: the link is dead if "@class='notfindCon-text'" is on the page,
    - Blocked: Naxos has blocked the crawler if "@id='cardNo'" is on the page,
    - Unavailable: resource is unavailble in US if a "<p>" tag contains
        'title is not available in your country'
    - Unknown: if none of the above tags appear

    The driver will click on buttons to accept cookies and log out from the
    platform as necessary.

    Args:
        driver: the selenium Driver object to use
        url: the url to check
    """
    driver.uc_open_with_reconnect(url)
    wait = WebDriverWait(driver, 5, ignored_exceptions=ERRORS)
    check_cookie(driver=driver)
    condition = wait.until(
        EC.any_of(
            EC.presence_of_element_located((By.XPATH, "//div[@class='song-play']/a")),
            EC.presence_of_element_located(
                (By.XPATH, "//div[@class='notfindCon-text']")
            ),
            EC.presence_of_element_located((By.XPATH, "//input[@id='cardNo']")),
            EC.presence_of_element_located(
                (
                    By.XPATH,
                    "//div[@class='playlists-right']/p",
                )
            ),
        )
    )
    if condition.get_attribute("role") == "button":
        status = "Live"
    elif condition.get_attribute("class") == "notfindCon-text":
        click_button(web_driver=driver, button_type="homepage")
        check_cookie(driver=driver)
        status = "Dead"
    elif condition.get_attribute("id") == "cardNo":
        return "Blocked"
    elif "title is not available in your country" in condition.text:
        status = "Unavailable"
    else:
        status = "Unknown"
    click_button(web_driver=driver, button_type="logout")
    return status
