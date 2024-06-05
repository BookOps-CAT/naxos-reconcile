import pandas as pd


def review_import_results(infile: str):
    df = pd.read_csv(
        infile,
        header=None,
        names=[
            "URL",
            "CID",
            "CONTROL_NO",
            "WC_NUMBER_OF_RECORDS",
            "WC_OCLC_NUMBER",
            "WC_RECORD_SOURCE",
            "WC_ALL_OCLC_NUM",
            # "URL_STATUS",
        ],
    )
    with_records = df[df["WC_NUMBER_OF_RECORDS"] >= 1]
    dead_links = df[df["URL_STATUS"] == False]
    print(
        f"At least one match: {with_records.shape[0]} of {df.shape[0]}, {(int(with_records.shape[0]) / int(df.shape[0])) * 100}%"  # noqa: E501
    )
    # print(
    #     f"Dead links in Naxos record: {dead_links.shape[0]} of {df.shape[0]}, {(int(dead_links.shape[0]) / int(df.shape[0])) * 100}%"  # noqa: E501
    # )


def review_results(infile: str):
    df = pd.read_csv(
        infile,
        header=None,
        names=[
            "OCLC_NUMBER",
            "BIB_ID",
            "CID",
            "URL",
            "CONTROL_NO",
            "WC_NUMBER_OF_RECORDS",
            "WC_OCLC_NUMBER",
            "WC_RECORD_SOURCE",
            "WC_ALL_OCLC_NUM",
            "WC_BEST_MATCH",
            "WC_OTHER_MATCH",
            # "URL_STATUS",
        ],
    )
    with_records = df[df["WC_NUMBER_OF_RECORDS"] >= 1]
    sierra_exact_match = df[df["WC_BEST_MATCH"] == True]
    sierra_other_match = df[df["WC_OTHER_MATCH"] == True]
    # dead_links = df[df["URL_STATUS"] == False]
    print(
        f"At least one match: {with_records.shape[0]} of {df.shape[0]}, {(int(with_records.shape[0]) / int(df.shape[0])) * 100}%"  # noqa: E501
    )
    print(
        f"Exact Sierra match: {sierra_exact_match.shape[0]} of {df.shape[0]}, {(int(sierra_exact_match.shape[0]) / int(df.shape[0])) * 100}%"  # noqa: E501
    )
    print(
        f"Match on Sierra record but not best record from WC: {sierra_other_match.shape[0]} of {df.shape[0]}, {(int(sierra_other_match.shape[0]) / int(df.shape[0])) * 100}%"  # noqa: E501
    )
