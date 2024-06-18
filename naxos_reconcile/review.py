import os
import pandas as pd


def review_all_results(date: str) -> None:
    """
    Reads directory for date provided (in format YYYY-MM-DD)
    and reviews each file containing "_results.csv" in the file name
    using the `review_file` function.
    """
    all_files = os.listdir(date)
    results_files = [i for i in all_files if "_results.csv" in i]
    n = 1
    for file in results_files:
        if "import" in file:
            file_type = "import"
        else:
            file_type = "check"
        print(f"Results for file #{n}, {file.split('/')[-1]}")
        review_file(infile=f"./data/files/{date}/{file}", file_type=file_type)


def review_file(infile: str, file_type: str) -> None:
    """
    Reads  csv outputs from WorldCat API queries and URL checks and
    reviews results. Summary of results is printed to stdout.

    Args:
        infile:
            path to file to review
        file_type:
            whether the file to be reviewed is a "records_to_import.csv" or
            "records_to_check.csv".
    """
    if file_type == "import":
        columns = [
            "URL",
            "CID",
            "TITLE",
            "PUBLISHER",
            "SERIES",
            "NUMBER_OF_RECORDS",
            "OCLC_NUM",
            "RECORD_SOURCE",
            # "URL_STATUS",
        ]
        df = pd.read_csv(
            infile,
            header=None,
            names=columns,
            converters={
                "NUMBER_OF_RECORDS": lambda x: int(x),
            },
            index_col=False,
            on_bad_lines="warn",
        )
    else:
        columns = [
            "URL",
            "CID",
            "SIERRA_OCLC_NUM",
            "BIB_ID",
            "NUMBER_OF_RECORDS",
            "OCLC_NUM",
            "RECORD_SOURCE",
            "OCLC_MATCH",
            # "URL_STATUS",
        ]
        df = pd.read_csv(
            infile,
            header=None,
            names=columns,
            converters={
                "NUMBER_OF_RECORDS": lambda x: int(x),
                "OCLC_MATCH": lambda x: eval(x),
            },
            index_col=False,
            on_bad_lines="warn",
        )
    df.drop_duplicates(inplace=True)
    total = df.shape[0]
    with_record = df[df["NUMBER_OF_RECORDS"] >= 1].shape[0]

    print(
        f"Records with at least one match in WorldCat: {with_record}/{total}, "
        f"{round((with_record / total), 2) * 100}%"
    )
    if "OCLC_MATCH" in df:
        sierra_match = df[df["OCLC_MATCH"] == True].shape[0]
        print(
            f"Records with match on OCLC number from Sierra: {sierra_match}/{total}, "
            f"{round((sierra_match / total), 2) * 100}%"
        )
    dead_links = int(df[df["URL_STATUS"] == "Dead"].shape[0])
    live_links = int(df[df["URL_STATUS"] == "Live"].shape[0])
    unavailable_links = int(df[df["URL_STATUS"] == "Unavailable"].shape[0])
    blocked_links = int(df[df["URL_STATUS"] == "Blocked"].shape[0])
    unknown_status_links = int(df[df["URL_STATUS"] == "Unknown"].shape[0])
    if live_links > 0:
        print(
            f"Records with live urls: {live_links}/{total}, "
            f"{round((live_links / total), 2) * 100}%"
        )
    if dead_links > 0:
        print(
            f"Records with dead links: {dead_links}/{total}, "
            f"{round((dead_links / total), 2) * 100}%"
        )
    if unavailable_links > 0:
        print(
            f"Records unavailable in US: {unavailable_links}/{total}, "
            f"{round((unavailable_links / total), 2) * 100}%"
        )
    if blocked_links > 0:
        print(
            f"URL checks blocked for: {blocked_links}/{total}, "
            f"{round((blocked_links / total), 2) * 100}%"
        )
    if unknown_status_links > 0:
        print(
            f"Unknown URL status for: {unknown_status_links}/{total}, "
            f"{round((unknown_status_links / total), 2) * 100}%"
        )
