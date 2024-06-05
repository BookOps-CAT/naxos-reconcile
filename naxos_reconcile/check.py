import os
import requests
from bookops_worldcat import MetadataSession

from naxos_reconcile.utils import (
    get_token,
    out_file,
    save_csv,
    open_csv_file,
    get_file_length,
)


def worldcat_missing_records(infile: str, first_row: int) -> None:
    """
    Search worldcat for brief bibs for records not in sierra
    """
    outfile = out_file("naxos_worldcat_records_to_import.csv")
    token = get_token(os.path.join(os.environ["USERPROFILE"], ".oclc/nyp_wc_test.json"))
    data = open_csv_file(infile, first_row)
    file_length = get_file_length(infile)
    n = 1
    for row in data:
        print(f"Record {n} of {file_length}: {row[2]}")
        with MetadataSession(authorization=token, totalRetries=3) as session:
            naxos_search = session.brief_bibs_search(
                q=f"mn='{row[2]}'",
                itemSubType="music-digital",
            )
            naxos_json = naxos_search.json()
            if naxos_json["numberOfRecords"] == 1:
                oclc_number = naxos_json["briefRecords"][0]["oclcNumber"]
                record_source = naxos_json["briefRecords"][0]["catalogingInfo"][
                    "catalogingAgency"
                ]
                all_oclc_numbers = oclc_number
            elif naxos_json["numberOfRecords"] == 0:
                oclc_number = None
                record_source = None
                all_oclc_numbers = None
            else:
                all_oclc_numbers = [i["oclcNumber"] for i in naxos_json["briefRecords"]]
                bibs = naxos_json["briefRecords"]
                naxos_bibs = [
                    i
                    for i in bibs
                    if i["catalogingInfo"]["catalogingAgency"] == "NAXOS"
                ]
                other_bibs = [
                    i
                    for i in bibs
                    if i["catalogingInfo"]["catalogingAgency"] != "NAXOS"
                ]
                if row[0] in all_oclc_numbers:
                    oclc_number = [
                        i["oclcNumber"] for i in bibs if i["oclcNumber"] == row[0]
                    ][0]
                    record_source = [
                        i["catalogingInfo"]["catalogingAgency"]
                        for i in bibs
                        if i["oclcNumber"] == row[0]
                    ][0]
                elif len(naxos_bibs) == 1:
                    oclc_number = naxos_bibs[0]["oclcNumber"]
                    record_source = naxos_bibs[0]["catalogingInfo"]["catalogingAgency"]
                elif len(naxos_bibs) == 0 and len(other_bibs) == 1:
                    oclc_number = other_bibs[0]["oclcNumber"]
                    record_source = other_bibs[0]["catalogingInfo"]["catalogingAgency"]
                elif len(naxos_bibs) > 1:
                    oclc_number = [i["oclcNumber"] for i in naxos_bibs]
                    record_source = [
                        i["catalogingInfo"]["catalogingAgency"] for i in naxos_bibs
                    ]
                else:
                    oclc_number = [i["oclcNumber"] for i in other_bibs]
                    record_source = [
                        i["catalogingInfo"]["catalogingAgency"] for i in other_bibs
                    ]
        # url_status = get_url_status(row[0])
        if naxos_json["numberOfRecords"] >= 1:
            record_avail = True
        else:
            record_avail = False
        print(
            f"Record {n} of {file_length}: {row[2]}. Record available: {record_avail}"
        )
        save_csv(
            outfile,
            [
                row[0],
                row[1],
                row[2],
                naxos_json["numberOfRecords"],
                oclc_number,
                record_source,
                all_oclc_numbers,
                # url_status,
            ],
        )
        n += 1


def worldcat_brief_bibs(infile: str, first_row: int) -> None:
    """
    Search worldcat for brief bibs
    """
    outfile = out_file("naxos_worldcat_brief_bibs.csv")
    token = get_token(os.path.join(os.environ["USERPROFILE"], ".oclc/nyp_wc_test.json"))
    data = open_csv_file(infile, first_row)
    file_length = get_file_length(infile)
    n = 1
    for row in data:
        print(f"Record {n} of {file_length}: {row[2]}")
        with MetadataSession(authorization=token, totalRetries=3) as session:
            naxos_search = session.brief_bibs_search(
                q=f"mn='{row[2]}'",
                itemSubType="music-digital",
            )
            naxos_json = naxos_search.json()
            if naxos_json["numberOfRecords"] == 1:
                oclc_number = naxos_json["briefRecords"][0]["oclcNumber"]
                record_source = naxos_json["briefRecords"][0]["catalogingInfo"][
                    "catalogingAgency"
                ]
                all_oclc_numbers = oclc_number
            elif naxos_json["numberOfRecords"] == 0:
                oclc_number = None
                record_source = None
                all_oclc_numbers = None
            else:
                all_oclc_numbers = [i["oclcNumber"] for i in naxos_json["briefRecords"]]
                bibs = naxos_json["briefRecords"]
                naxos_bibs = [
                    i
                    for i in bibs
                    if i["catalogingInfo"]["catalogingAgency"] == "NAXOS"
                ]
                other_bibs = [
                    i
                    for i in bibs
                    if i["catalogingInfo"]["catalogingAgency"] != "NAXOS"
                ]
                if row[0] in all_oclc_numbers:
                    oclc_number = [
                        i["oclcNumber"] for i in bibs if i["oclcNumber"] == row[0]
                    ][0]
                    record_source = [
                        i["catalogingInfo"]["catalogingAgency"]
                        for i in bibs
                        if i["oclcNumber"] == row[0]
                    ][0]
                elif len(naxos_bibs) == 1:
                    oclc_number = naxos_bibs[0]["oclcNumber"]
                    record_source = naxos_bibs[0]["catalogingInfo"]["catalogingAgency"]
                elif len(naxos_bibs) == 0 and len(other_bibs) == 1:
                    oclc_number = other_bibs[0]["oclcNumber"]
                    record_source = other_bibs[0]["catalogingInfo"]["catalogingAgency"]
                elif len(naxos_bibs) > 1:
                    oclc_number = [i["oclcNumber"] for i in naxos_bibs]
                    record_source = [
                        i["catalogingInfo"]["catalogingAgency"] for i in naxos_bibs
                    ]
                else:
                    oclc_number = [i["oclcNumber"] for i in other_bibs]
                    record_source = [
                        i["catalogingInfo"]["catalogingAgency"] for i in other_bibs
                    ]
        # url_status = get_url_status(row[3])
        if oclc_number == row[0]:
            best_match = True
            other_match = False
        elif not all_oclc_numbers and not oclc_number:
            best_match = False
            other_match = False
        elif oclc_number != row[0] and row[0] in all_oclc_numbers:
            best_match = False
            other_match = True
        else:
            best_match = False
            other_match = False

        save_csv(
            outfile,
            [
                row[0],
                row[1],
                row[2],
                row[3],
                row[4],
                naxos_json["numberOfRecords"],
                oclc_number,
                record_source,
                all_oclc_numbers,
                best_match,
                other_match,
                # url_status,
            ],
        )
        n += 1

    # print(
    #     f"Dead links: {dead_links.shape[0]} of {df.shape[0]}, {(int(dead_links.shape[0]) / int(df.shape[0])) * 100}%"  # noqa: E501
    # )


def get_url_status(url: str) -> int:
    """check the URL and return the status code as int"""
    response = requests.get(url)
    return response.ok
