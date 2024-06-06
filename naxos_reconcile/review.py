import pandas as pd


def review_results(infile: str):
    if "records_to_import" in infile:
        columns = [
            "URL",
            "CID",
            "NUMBER_OF_RECORDS",
            "OCLC_NUM",
            "RECORD_SOURCE",
            "ALL_OCLC_NUM",
            "URL_STATUS",
        ]
    elif "records_to_check" in infile:
        columns = [
            "URL",
            "CID",
            "NUMBER_OF_RECORDS",
            "OCLC_NUM",
            "RECORD_SOURCE",
            "ALL_OCLC_NUM",
            "URL_STATUS",
            "SIERRA_OCLC_NUM",
            "BIB_ID",
        ]
    df = pd.read_csv(
        infile, header=None, names=columns, converters={"URL_STATUS": lambda x: eval(x)}
    )
    if df["SIERRA_OCLC_NUM"] and df["OCLC_NUM"] == df["SIERRA_OCLC_NUM"]:
        df["OCLC_MATCH"] = True
    elif df["SIERRA_OCLC_NUM"] and df["OCLC_NUM"] != df["SIERRA_OCLC_NUM"]:
        df["OCLC_MATCH"] = False
    else:
        pass

    total = int(df.shape[0])

    with_record = int(df[df["NUMBER_OF_RECORDS"] >= 1].shape[0])
    dead_links = int(df[df["URL_STATUS"] is False].shape[0])
    print(
        f"Records with at least one match in WorldCat: {with_record} of {total}, {round((with_record / total), 3) * 100}%"  # noqa: E501
    )
    print(
        f"Records with dead links: {dead_links} of {total}, {round((dead_links / total), 2) * 100}%"  # noqa: E501
    )
    if "records_to_check" in infile:
        sierra_match = int(df[df["OCLC_MATCH"] is True].shape[0])
        print(
            f"Records with match on OCLC number from Sierra: {sierra_match} of {total}, {round((sierra_match / total), 3) * 100}%"  # noqa: E501
        )
