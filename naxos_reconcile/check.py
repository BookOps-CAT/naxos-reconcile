import os
from typing import Optional, List, Dict
import requests
from bookops_worldcat import MetadataSession

from naxos_reconcile.utils import (
    get_token,
    out_file,
    save_csv,
    open_csv_file,
    get_file_length,
)


def parse_worldcat_results(data: dict, csv_data: Optional[List] = None) -> Dict:
    """
    Parse data returned by WorldCat Metadata API brief_bibs_search query
    """
    out_dict = {}
    if data["numberOfRecords"] == 1:
        out_dict["oclc_number"] = data["briefRecords"][0]["oclcNumber"]
        out_dict["record_source"] = data["briefRecords"][0]["catalogingInfo"][
            "catalogingAgency"
        ]
        out_dict["all_oclc_numbers"] = out_dict["oclc_number"]
    elif data["numberOfRecords"] == 0:
        out_dict["oclc_number"] = None
        out_dict["record_source"] = None
        out_dict["all_oclc_numbers"] = None
    elif data["numberOfRecords"] > 1:
        out_dict["all_oclc_numbers"] = [i["oclcNumber"] for i in data["briefRecords"]]
        bibs = data["briefRecords"]
        naxos_bibs = [
            i for i in bibs if i["catalogingInfo"]["catalogingAgency"] == "NAXOS"
        ]
        other_bibs = [
            i for i in bibs if i["catalogingInfo"]["catalogingAgency"] != "NAXOS"
        ]
        if csv_data:
            if csv_data[0] in out_dict["all_oclc_numbers"]:
                out_dict["oclc_number"] = [
                    i["oclcNumber"] for i in bibs if i["oclcNumber"] == csv_data[0]
                ][0]
                out_dict["record_source"] = [
                    i["catalogingInfo"]["catalogingAgency"]
                    for i in bibs
                    if i["oclcNumber"] == csv_data[0]
                ][0]
        elif len(naxos_bibs) == 1:
            out_dict["oclc_number"] = naxos_bibs[0]["oclcNumber"]
            out_dict["record_source"] = naxos_bibs[0]["catalogingInfo"][
                "catalogingAgency"
            ]
        elif len(naxos_bibs) == 0 and len(other_bibs) == 1:
            out_dict["oclc_number"] = other_bibs[0]["oclcNumber"]
            out_dict["record_source"] = other_bibs[0]["catalogingInfo"][
                "catalogingAgency"
            ]
        elif len(naxos_bibs) > 1:
            out_dict["oclc_number"] = [i["oclcNumber"] for i in naxos_bibs]
            out_dict["record_source"] = [
                i["catalogingInfo"]["catalogingAgency"] for i in naxos_bibs
            ]
        else:
            out_dict["oclc_number"] = [i["oclcNumber"] for i in other_bibs]
            out_dict["record_source"] = [
                i["catalogingInfo"]["catalogingAgency"] for i in other_bibs
            ]
    else:
        out_dict["oclc_number"] = [i["oclcNumber"] for i in other_bibs]
        out_dict["record_source"] = [
            i["catalogingInfo"]["catalogingAgency"] for i in other_bibs
        ]
        out_dict["all_oclc_numbers"] = out_dict["oclc_number"]
    return out_dict


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
            parsed_data = parse_worldcat_results(naxos_json)
        url_status = get_url_status(row[0])
        save_csv(
            outfile,
            [
                row[0],
                row[1],
                row[2],
                naxos_json["numberOfRecords"],
                parsed_data["oclc_number"],
                parsed_data["record_source"],
                parsed_data["all_oclc_numbers"],
                url_status,
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
            url_status = get_url_status(row[3])
            naxos_json = naxos_search.json()
            if naxos_json:
                parsed_data = parse_worldcat_results(naxos_json, row)
                save_csv(
                    outfile,
                    [
                        row[0],
                        row[1],
                        row[2],
                        row[3],
                        row[4],
                        naxos_json["numberOfRecords"],
                        parsed_data["oclc_number"],
                        parsed_data["record_source"],
                        parsed_data["all_oclc_numbers"],
                        url_status,
                    ],
                )
        n += 1


def get_url_status(url: str) -> int:
    """check the URL and return the status code as int"""
    response = requests.get(url)
    return response.ok
