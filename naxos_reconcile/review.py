import pandas as pd


def dedupe_file(infile: str) -> None:
    columns = [
        "URL",
        "CID",
        "OCLC_SIERRA",
        "BIB_ID",
        "NUMBER_OF_RECORDS",
        "OCLC_NUM",
        "RECORD_SOURCE",
        "URL_STATUS",
    ]
    df = pd.read_csv(
        infile,
        header=None,
        names=columns,
        index_col=False,
        on_bad_lines="warn",
    )
    df.drop_duplicates(inplace=True)
    print(df.shape[0])
    df.to_csv(
        f"{infile.split('.csv')[0]}_deduped.csv",
        index=False,
        header=False,
    )


def review_file(infile: str) -> None:
    """
    Reads csv file output from WorldCat API queries and URL checks
    and summarizes results.

    Args:
        infile: name of file to review
    """

    if "import" in infile:
        columns = [
            "URL",
            "CID",
            "TITLE",
            "PUBLISHER",
            "SERIES",
            "NUMBER_OF_RECORDS",
            "OCLC_NUM",
            "RECORD_SOURCE",
            "URL_STATUS",
        ]
    else:
        columns = [
            "URL",
            "CID",
            "SIERRA_OCLC_NUM",
            "BIB_ID",
            "NUMBER_OF_RECORDS",
            "OCLC_NUM",
            "RECORD_SOURCE",
            "URL_STATUS",
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
    df.drop_duplicates(inplace=True)
    total = df.shape[0]

    print(f"\nResults for {infile}")
    with_record = df[df["NUMBER_OF_RECORDS"] >= 1]
    print("WorldCat queries:\n")
    print(
        "Records with at least one match in WorldCat: "
        f"{int(with_record.shape[0])}/{total}, "
        f"{round((int(with_record.shape[0]) / total), 4) * 100}%"
    )
    one_oclc_record = df[df["NUMBER_OF_RECORDS"] == 1]
    print(
        "Records with exactly one match in WorldCat: "
        f"{int(one_oclc_record.shape[0])}/{total}, "
        f"{round((int(one_oclc_record.shape[0]) / total), 4) * 100}%"
    )
    no_oclc_record = df[df["NUMBER_OF_RECORDS"] == 0]
    no_oclc_record.to_csv(
        f"{infile.split('.csv')[0]}_no_oclc_record.csv",
        index=False,
        header=False,
    )
    print(
        f"Records with no matches in WorldCat written to "
        f"{infile.split('.csv')[0]}_no_oclc_record.csv"
    )
    problem_links = df[
        (df["URL_STATUS"] == "Unavailable")
        | (df["URL_STATUS"] == "Dead")
        | (df["URL_STATUS"] == "Blocked")
        | (df["URL_STATUS"] == "Unknown")
    ]
    dead_links = df[df["URL_STATUS"] == "Dead"]
    live_links = df[df["URL_STATUS"] == "Live"]
    unavailable_links = df[df["URL_STATUS"] == "Unavailable"]
    blocked_links = df[df["URL_STATUS"] == "Blocked"]
    unknown_status_links = df[df["URL_STATUS"] == "Unknown"]
    print("\nURL checks:\n")
    if live_links.shape[0] > 0:
        print(
            f"Total live urls: {live_links.shape[0]}/{total}, "
            f"{round((live_links.shape[0] / total), 4) * 100}%"
        )
    if problem_links.shape[0] > 0:
        print(
            f"Total invalid urls: {problem_links.shape[0]}/{total}, "
            f"{round((problem_links.shape[0] / total), 4) * 100}%"
        )
    if dead_links.shape[0] > 0:
        print(
            f"Dead urls: {dead_links.shape[0]}/{total}, "
            f"{round((dead_links.shape[0] / total), 4) * 100}%"
        )
    if unavailable_links.shape[0] > 0:
        print(
            f"Unavailable in US: {unavailable_links.shape[0]}/{total}, "
            f"{round((unavailable_links.shape[0] / total), 4) * 100}%"
        )
    if blocked_links.shape[0] > 0:
        print(
            f"URL checks blocked: {blocked_links.shape[0]}/{total}, "
            f"{round((blocked_links.shape[0] / total), 4) * 100}%"
        )
    if unknown_status_links.shape[0] > 0:
        print(
            f"Unknown URL status: {unknown_status_links.shape[0]}/{total}, "
            f"{round((unknown_status_links.shape[0] / total), 4) * 100}%"
        )
    problem_links.to_csv(
        f"{infile.split('.csv')[0]}_problem_urls.csv",
        index=False,
        header=False,
    )
    print(f"Records without live URLs in {infile.split('.csv')[0]}_problem_urls.csv")
