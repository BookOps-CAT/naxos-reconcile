import os
import pandas as pd
import requests
from bookops_worldcat import MetadataSession

from naxos_reconcile.utils import (
    get_token,
    out_file,
    save_csv,
    open_csv_file,
    get_file_length,
)


def get_worldcat_data(infile: str, first_row: int):
    """
    Read the prepped Naxos csv and query WorldCat for each row of data
    Creates another csv with data from the results of the API query

    """
    outfile = out_file("naxos_with_worldcat.csv")
    token = get_token(os.path.join(os.environ["USERPROFILE"], ".oclc/nyp_wc_test.json"))
    data = open_csv_file(infile, first_row)
    file_length = get_file_length(infile)
    n = 1
    for row in data:
        print(f"Line {n} of {file_length}")
        with MetadataSession(authorization=token, totalRetries=3) as session:
            naxos_search = session.brief_bibs_search(
                q=f"mn='{row[2]}'",
                itemSubType="music-digital",
            )
            naxos_json = naxos_search.json()
            number_of_records = naxos_json["numberOfRecords"]
            match (naxos_json["numberOfRecords"]):
                case 1:
                    oclc_number = naxos_json["briefRecords"][0]["oclcNumber"]
                    record_source = naxos_json["briefRecords"][0]["catalogingInfo"][
                        "catalogingAgency"
                    ]
                case 0:
                    oclc_number = None
                    record_source = None
                case _:
                    oclc_number = [i["oclcNumber"] for i in naxos_json["briefRecords"]]
                    record_source = [
                        i["catalogingInfo"]["catalogingAgency"]
                        for i in naxos_json["briefRecords"]
                    ]
        save_csv(
            outfile,
            [
                row[0],
                row[1],
                row[2],
                number_of_records,
                oclc_number,
                record_source,
            ],
        )
        n += 1


def review_worldcat_results(infile: str):
    df = pd.read_csv(
        infile,
        header=None,
        names=[
            "URL",
            "CONTROL_NO",
            "CID",
            "NUMBER_OF_RECORDS",
            "OCLC_NUMBER",
            "RECORD_SOURCE",
        ],
    )
    with_records = df[df["NUMBER_OF_RECORDS"] >= 1]
    print(f"{with_records.shape[0]} out of {df.shape[0]} had a record in oclc")
    naxos_records = with_records[with_records["RECORD_SOURCE"] == "NAXOS"]
    print(f"{naxos_records.shape[0]} out of {df.shape[0]} are from Naxos")
    print(f"{(int(with_records.shape[0]) / int(df.shape[0])) * 100}% had records")
    print(
        f"{(int(naxos_records.shape[0]) / int(df.shape[0])) * 100}% were Naxos records"
    )


def get_url_status(url: str) -> int:
    """check the URL and return the status code as int"""
    response = requests.get(url)
    return response.status_code
