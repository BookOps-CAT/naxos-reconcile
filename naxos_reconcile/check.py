import os
from time import perf_counter
from bookops_worldcat import MetadataSession
from seleniumbase import Driver, SB
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


def parse_worldcat_results(data: dict, oclc_num: str) -> dict:
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
        data:
            json data returned by query to Metadata API
        oclc_num:
            OCLC number from Sierra export/column 3 of .csv.
            sample_to_import.csv and records_to_import.csv files
            contain the record title in column 3 rather than the OCLC number
    Returns:
        dict containing:
        - total number of records returned by API query
        - oclc_number (as str or list),
        - cataloging source (as str or list)
        - whether the OCLC number matches the 001 from Sierra
    """
    match data:
        case {"numberOfRecords": 1}:
            return {
                "number_of_records": data["numberOfRecords"],
                "oclc_number": data["briefRecords"][0]["oclcNumber"],
                "record_source": data["briefRecords"][0]["catalogingInfo"][
                    "catalogingAgency"
                ],
            }
        case {"numberOfRecords": 0}:
            return {
                "number_of_records": data["numberOfRecords"],
                "oclc_number": None,
                "record_source": None,
            }
        case {
            "numberOfRecords": record_count,
        } if record_count > 1 and oclc_num in [
            i["oclcNumber"] for i in data["briefRecords"]
        ]:
            return {
                "number_of_records": data["numberOfRecords"],
                "oclc_number": [
                    i["oclcNumber"]
                    for i in data["briefRecords"]
                    if i["oclcNumber"] == oclc_num
                ][0],
                "record_source": [
                    i["catalogingInfo"]["catalogingAgency"]
                    for i in data["briefRecords"]
                    if i["oclcNumber"] == oclc_num
                ][0],
            }
        case _:
            pass
    bibs = [
        i
        for i in data["briefRecords"]
        if i["catalogingInfo"]["catalogingLanguage"] == "eng"
    ]
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
        return {
            "number_of_records": data["numberOfRecords"],
            "oclc_number": naxos_bibs[0]["oclcNumber"],
            "record_source": naxos_bibs[0]["catalogingInfo"]["catalogingAgency"],
            "oclc_match": False,
        }
    elif len(full_bibs) == 1:
        return {
            "number_of_records": data["numberOfRecords"],
            "oclc_number": full_bibs[0]["oclcNumber"],
            "record_source": full_bibs[0]["catalogingInfo"]["catalogingAgency"],
            "oclc_match": False,
        }
    elif len(other_bibs) == 1:
        return {
            "number_of_records": data["numberOfRecords"],
            "oclc_number": other_bibs[0]["oclcNumber"],
            "record_source": other_bibs[0]["catalogingInfo"]["catalogingAgency"],
            "oclc_match": False,
        }
    elif len(naxos_bibs) > 1:
        return {
            "number_of_records": data["numberOfRecords"],
            "oclc_number": [i["oclcNumber"] for i in naxos_bibs],
            "record_source": [
                i["catalogingInfo"]["catalogingAgency"] for i in naxos_bibs
            ],
            "oclc_match": False,
        }
    elif len(full_bibs) > 1:
        return {
            "number_of_records": data["numberOfRecords"],
            "oclc_number": [i["oclcNumber"] for i in full_bibs],
            "record_source": [
                i["catalogingInfo"]["catalogingAgency"] for i in full_bibs
            ],
            "oclc_match": False,
        }
    else:
        return {
            "number_of_records": data["numberOfRecords"],
            "oclc_number": [i["oclcNumber"] for i in other_bibs],
            "record_source": [
                i["catalogingInfo"]["catalogingAgency"] for i in other_bibs
            ],
            "oclc_match": False,
        }


def search_oclc_only(infile: str, last_row: int) -> str:
    """
    Search worldcat for brief bibs
    Outputs a .csv file with:
    - number of records returned by query to Metadata API
    - OCLC number(s),
    - source(s) of catalog record

    Args:
        infile:
            filename for file to be used in queries
        last_row:
            the last row from infile to be checked.
            CLI command will default to 0 if not provided
    Returns:
        name of .csv outfile as str
    """
    outfile = out_file(f"{infile.split('.csv')[0]}_search_results.csv")
    n = last_row + 1
    data = open_csv_file(f"{date_directory()}/{infile}", last_row)
    file_length = get_file_length(f"{date_directory()}/{infile}")
    token = get_token(os.path.join(os.environ["USERPROFILE"], ".oclc/nyp_wc_test.json"))
    with MetadataSession(authorization=token, totalRetries=3) as session:
        for row in data:
            start = perf_counter()
            response = session.brief_bibs_search(
                q=f"mn='{row[1]}' OR am='{row[0].split('nypl.')[1]}'",
                itemSubType="music-digital",
            )
            response_json = response.json()  # type: ignore[union-attr]
            parsed_data = parse_worldcat_results(data=response_json, oclc_num=row[2])
            row.extend(
                [
                    parsed_data["number_of_records"],
                    parsed_data["oclc_number"],
                    parsed_data["record_source"],
                ]
            )
            save_csv(outfile, row)
            stop = perf_counter()
            print(f"Record {n} of {file_length}, search took {stop-start:0.4f} seconds")
            n += 1
    return outfile


def check_urls_only(infile: str, last_row: int) -> str:
    """
    Check if URLs are live. Outputs a .csv file with status of URL.
    Possible URL statuses:
    - Live
    - Dead
    - Unavailable
    - Blocked
    - Unknown

    Args:
        infile:
            filename for file to be used in queries
        last_row:
            the last row from infile to be checked.
            CLI command will default to 0 if not provided
    Returns:
        name of .csv outfile as str
    """
    outfile = out_file(f"{infile.split('.csv')[0]}_url_results.csv")
    n = last_row + 1
    data = open_csv_file(f"{date_directory()}/{infile}", last_row)
    file_length = get_file_length(f"{date_directory()}/{infile}")
    with SB(uc=True, headless=True) as sb:
        driver_wait = WebDriverWait(sb.driver, 2, ignored_exceptions=ERRORS)
        for row in data:
            start = perf_counter()
            status = get_selenium_status(driver=sb, wait=driver_wait, url=row[0])
            check_logout(wait=driver_wait)
            row.append(status)
            save_csv(outfile, row)
            stop = perf_counter()
            print(f"{n} of {file_length}, URL is {row[-1]}, {stop-start:0.4f} seconds")
            n += 1
    return outfile


def click_cookie(wait: WebDriverWait) -> None:
    """wait until cookie button is available and click it"""
    button = wait.until(EC.element_to_be_clickable((By.ID, "cmpwelcomebtnyes")))
    button.click()


def click_logout(wait: WebDriverWait) -> None:
    """wait until logout button is available and click it"""
    button = wait.until(
        EC.element_to_be_clickable((By.XPATH, "//div[@class='head-use']/a"))
    )
    button.click()


def click_homepage(wait: WebDriverWait) -> None:
    """wait until homepage button is available and click it"""
    button = wait.until(EC.element_to_be_clickable((By.CLASS_NAME, "notfindCon-btn")))
    button.click()


def check_cookie(wait: WebDriverWait) -> None:
    """
    Check if a cookie button is present on a page. If the button is
    not present, do not wait for it. If it is present, click it.
    """
    try:
        cookie = wait.until(
            EC.all_of(EC.element_to_be_clickable((By.ID, "cmpwelcomebtnyes")))
        )
        if cookie is True:
            click_cookie(wait=wait)
    except TimeoutException:
        pass


def check_logout(wait: WebDriverWait) -> None:
    """
    Check if a cookie button is present on a page. If the button is
    not present, do not wait for it. If it is present, click it.
    """
    try:
        logout = wait.until(
            EC.all_of(
                EC.element_to_be_clickable((By.XPATH, "//div[@class='head-use']/a"))
            )
        )
        if logout is True:
            click_logout(wait=wait)
    except TimeoutException:
        pass


def get_selenium_status(wait: WebDriverWait, driver: SB, url: str) -> str:
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
    driver.uc_open(url=url)
    try:
        wait.until(
            EC.presence_of_element_located((By.XPATH, "//div[@class='song-play']/a")),
        )
        return "Live"
    except TimeoutException:
        pass
    try:
        wait.until(
            EC.presence_of_element_located(
                (By.XPATH, "//div[@class='playlists-right']/p")
            ),
        )
        return "Unavailable"
    except TimeoutException:
        pass
    try:
        wait.until(
            EC.presence_of_element_located(
                (By.XPATH, "//div[@class='notfindCon-text']")
            ),
        )
        click_homepage(wait=wait)
        check_cookie(wait=wait)
        return "Dead"
    except TimeoutException:
        pass
    try:
        wait.until(
            EC.presence_of_element_located((By.XPATH, "//input[@id='cardNo']")),
        )
        return "Blocked"
    except TimeoutException:
        return "Unknown"


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
    outfile = out_file(f"{infile.split('.csv')[0]}_full_results.csv")
    n = last_row + 1
    data = open_csv_file(f"{date_directory()}/{infile}", last_row)
    file_length = get_file_length(f"{date_directory()}/{infile}")
    token = get_token(os.path.join(os.environ["USERPROFILE"], ".oclc/nyp_wc_test.json"))
    driver = Driver(uc=True, headless=True)
    driver_wait = WebDriverWait(driver, 2, ignored_exceptions=ERRORS)
    with MetadataSession(authorization=token, totalRetries=3) as session:
        for row in data:
            start = perf_counter()
            status = get_selenium_status(driver=driver, wait=driver_wait, url=row[0])
            check_logout(wait=driver_wait)
            response = session.brief_bibs_search(
                q=f"mn='{row[1]}' OR am='{row[0].split('nypl.')[1]}'",
                itemSubType="music-digital",
            )
            response_json = response.json()  # type: ignore[union-attr]
            parsed_data = parse_worldcat_results(data=response_json, oclc_num=row[2])
            row.extend(
                [
                    parsed_data["number_of_records"],
                    parsed_data["oclc_number"],
                    parsed_data["record_source"],
                    status,
                ]
            )
            save_csv(outfile, row)
            stop = perf_counter()
            print(
                f"{n} of {file_length}. URL is {status}. "
                f"Search time: {stop-start:0.4f} seconds"
            )
            n += 1
    return outfile
