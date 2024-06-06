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
            "URL_STATUS",
        ],
    )
    total = df.shape[0]
    with_records = int(df[df["WC_NUMBER_OF_RECORDS"] >= 1].shape[0])
    dead_links = df[df["URL_STATUS"] == False]  # noqa: E712
    print(
        f"At least one match: {with_records} of {total}, {round((with_records / total), 3) * 100}%"  # noqa: E501
    )
    print(
        f"Dead links in Naxos record: {dead_links.shape[0]} of {total}, {round((int(dead_links.shape[0]) / total), 2) * 100}%"  # noqa: E501
    )


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
            "URL_STATUS",
        ],
    )
    with_records = int(df[df["WC_NUMBER_OF_RECORDS"] >= 1].shape[0])
    sierra_exact_match = int(df[df["WC_BEST_MATCH"] == True].shape[0])  # noqa: E712
    # sierra_other_match = int(df[df["WC_OTHER_MATCH"] == True].shape[0])  # noqa: E712
    dead_links = df[df["URL_STATUS"] == False]  # noqa: E712
    total = int(df.shape[0])
    print(
        f"At least one match: {with_records} of {total}, {round((with_records / total), 3) * 100}%"  # noqa: E501
    )
    print(
        f"Exact Sierra match: {sierra_exact_match} of {total}, {round((sierra_exact_match / total), 3) * 100}%"  # noqa: E501
    )
    # print(
    #     f"Match on Sierra record but not best record from WC: {sierra_other_match} of {total}, {round((sierra_other_match / total), 3) * 100}%"  # noqa: E501
    # )
    print(
        f"Dead links in Naxos record: {dead_links.shape[0]} of {total}, {round((int(dead_links.shape[0]) / total), 2) * 100}%"  # noqa: E501
    )
