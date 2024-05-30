import requests
import pandas as pd

from naxos_reconcile.utils import out_file


def get_url_status(url: str) -> int:
    """check the URL and return the status code as int"""
    response = requests.get(url)
    return response.status_code


def compare_files(sierra_file: str, naxos_file: str):
    """compare prepped files"""
    # create output files
    check_csv = out_file("combined_urls_to_check.csv")
    delete_csv = out_file("records_to_delete.csv")
    import_csv = out_file("records_to_import.csv")

    # read input files into dataframes
    sierra_df = pd.read_csv(
        sierra_file,
        header=None,
        names=["OCLC_NUMBER", "BIB_ID", "URL_SIERRA", "CID_SIERRA"],
        dtype=str,
    )
    naxos_df = pd.read_csv(
        naxos_file,
        header=None,
        names=["URL_NAXOS", "CONTROL_NO", "CID_NAXOS"],
        dtype=str,
    )

    # drop duplicate rows, just in case
    sierra_df.drop_duplicates(inplace=True)
    naxos_df.drop_duplicates(inplace=True)

    # merge dataframes with an inner join and keep only exact matches
    check_df = sierra_df.merge(naxos_df, left_on="CID_SIERRA", right_on="CID_NAXOS")
    check_df.to_csv(
        check_csv,
        index=False,
    )
    print(f"urls to check in {check_csv}")

    # merge dataframes with an outer join to identify unique rows
    unique_df = sierra_df.merge(
        naxos_df,
        how="outer",
        indicator=True,
        left_on="CID_SIERRA",
        right_on="CID_NAXOS",
    )

    # rows only in sierra input file should be deleted from sierra
    sierra_unique = unique_df[unique_df["_merge"] == "left_only"]
    sierra_unique.to_csv(
        delete_csv,
        index=False,
        columns=["OCLC_NUMBER", "BIB_ID", "URL_SIERRA", "CID_SIERRA"],
    )
    print(f"records to delete in {delete_csv}")

    # rows only in naxos input file should be imported into sierra
    naxos_unique = unique_df[unique_df["_merge"] == "right_only"]
    naxos_unique.to_csv(
        import_csv, index=False, columns=["URL_NAXOS", "CONTROL_NO", "CID_NAXOS"]
    )
    print(f"records to import in {import_csv}")
