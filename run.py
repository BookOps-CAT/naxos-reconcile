import csv

import click
import pandas as pd

from naxos_reconcile.sierra import NaxosRecord
from naxos_reconcile.utils import (
    get_file_length,
    out_file,
    save_csv,
    date_directory,
)


@click.group()
def cli():
    """
    Get Naxos Data
    """
    pass


@click.option(
    "-t",
    "--file-type",
    type=click.Choice(["sierra", "naxos"], case_sensitive=False),
    help="Type of file to process (Sierra .csv or Naxos MARC)",
)
@click.option("-f", "--file", "file", help="File to process")
@cli.command("prep", short_help="Process Sierra .csv or Naxos MARC file")
def process_sierra_file(file_type, file):
    if file_type == "sierra":
        print("Processing Sierra csv file")
        NaxosRecord.process_sierra_export(file)
    elif file_type == "naxos":
        print("Processing Naxos MARC file")
        naxos_marc_processed = NaxosRecord.process_naxos_marc(file)
        print("Creating Naxos CSV file")
        NaxosRecord.extract_naxos_data(naxos_marc_processed)
    else:
        pass


@click.option("-f", "--file", "file", help="File to process")
@cli.command("process-xml", short_help="Process Naxos XML")
def process_naxos_xml(file):
    print("Processing Naxos XML file")
    NaxosRecord.process_naxos_xml(file)


@cli.command("sample", short_help="Get sample of prepped Naxos csv file")
def sample_csv_file():
    naxos_df = pd.read_csv(
        f"{date_directory()}/naxos-prepped-data.csv",
        header=None,
        names=["CONTROL_NO", "BIB_ID", "URL", "CID"],
        dtype=str,
    )
    sample = naxos_df.sample(frac=0.2)
    print(sample)
    # sample.to_csv(f"{date_directory()}/naxos-sample.csv", index=False, header=False)


@cli.command("compare", short_help="Compare prepped Sierra and Naxos files")
def compare_infiles():
    sierra_df = pd.read_csv(
        f"{date_directory()}/sierra-prepped-data.csv",
        header=None,
        names=["CONTROL_NO", "BIB_ID", "URL", "CID"],
        dtype=str,
    )
    naxos_df = pd.read_csv(
        f"{date_directory()}/naxos-prepped-data.csv",
        header=None,
        names=["CONTROL_NO", "BIB_ID", "URL", "CID"],
        dtype=str,
    )

    # merge dataframes with an inner join and keep only exact matches
    check_df = sierra_df.merge(naxos_df)
    check_df.to_csv(
        f"{date_directory()}/combined-urls-to-check.csv", index=False, header=False
    )
    print(f"urls to check in {date_directory()}/combined-urls-to-check.csv")
    # merge dataframes with a outer join to identify unique rows
    unique_df = sierra_df.merge(naxos_df, how="outer", indicator=True)

    sierra_unique = unique_df[unique_df["_merge"] == "left_only"]
    sierra_unique.to_csv(
        f"{date_directory()}/records-to-delete.csv", index=False, header=False
    )
    print(f"records to delete in {date_directory()}/records-to-delete.csv")
    naxos_unique = unique_df[unique_df["_merge"] == "right_only"]
    naxos_unique.to_csv(
        f"{date_directory()}/records-to-import.csv", index=False, header=False
    )
    print(f"records to import in {date_directory()}/records-to-import.csv")


@click.option("-f", "file", help=".csv file to process")
@cli.command("check-urls", short_help="Check status of urls")
def check_urls(infile):
    print("Checking URLs")
    checked = out_file("combined-urls-status.csv")
    length = get_file_length(infile)
    n = 1
    with open(infile, "r", encoding="utf-8") as csvfile:
        reader = csv.reader(csvfile, delimiter=",")
        for row in reader:
            record = NaxosRecord.data_from_csv(row)
            save_csv(
                checked,
                [
                    record.control_no,
                    record.bib_id,
                    record.url,
                    record.cid,
                    record.status,
                ],
            )
            print(f"({n} of {length}): {record.bib_id, record.url, record.status}")
            n += 1


@click.option("-s", "--sierra", "sierra_file", help="Sierra .csv file to process")
@click.option("-n", "--naxos", "naxos_file", help="Naxos .mrc file to process")
@cli.command(
    "reconcile", short_help="Process Sierra and Naxos files and then compare them"
)
def reconcile_files(sierra_file, naxos_file):
    # prep data from Sierra export
    print("Processing Sierra csv file")
    sierra_out = NaxosRecord.process_sierra_export(sierra_file)
    print("Finished processing Sierra csv file")
    print(f"Sierra output is in {sierra_out}")

    # prep data from Naxos MARC records
    print("Processing Naxos MARC file")
    naxos_marc_out = NaxosRecord.process_naxos_marc(naxos_file)
    print("Finished processing Naxos MARC file")
    print(f"Processed MARC file is in {naxos_marc_out}")
    print("Creating Naxos CSV file")
    naxos_csv_out = NaxosRecord.extract_naxos_data(naxos_marc_out)
    print("Finished processing Naxos csv file")
    print(f"Naxos output is in {naxos_csv_out}")

    # compare data from Naxos MARC records and Sierra export
    print("Comparing files")
    sierra_df = pd.read_csv(
        sierra_out,
        header=None,
        names=["CONTROL_NO", "BIB_ID", "URL", "CID"],
        dtype=str,
    )
    naxos_df = pd.read_csv(
        naxos_csv_out,
        header=None,
        names=["CONTROL_NO", "BIB_ID", "URL", "CID"],
        dtype=str,
    )
    url_file = out_file("combined-urls-to-check.csv")
    delete_file = out_file("records-to-delete.csv")
    import_file = out_file("records-to-import.csv")
    # Inner join dataframes to identify matches/URLs to check
    check_df = sierra_df.merge(naxos_df)
    check_df.to_csv(url_file, index=False, header=False)
    print(f"urls to check in {url_file}")

    # Outer join dataframes to identify unique items
    unique_df = sierra_df.merge(naxos_df, how="outer", indicator=True)
    print(unique_df)
    # # Records that are only in Sierra, not in Naxos MARC
    # sierra_unique = unique_df[unique_df["_merge"] == "left_only"]
    # sierra_unique.to_csv(delete_file, index=False, header=False)
    # print(f"records to delete in {delete_file}")

    # # Records that are only in Naxos MARC, not in Sierra
    # naxos_unique = unique_df[unique_df["_merge"] == "right_only"]
    # naxos_unique.to_csv(import_file, index=False, header=False)
    # print(f"records to import in {import_file}")

    # # Check URLs to identify dead links
    # print("Checking URLs")
    # checked_urls = out_file("combined-urls-status.csv")
    # length = get_file_length(url_file)
    # n = 1
    # with open(url_file, "r", encoding="utf-8") as csvfile:
    #     reader = csv.reader(csvfile, delimiter=",")
    #     for row in reader:
    #         record = NaxosRecord.data_from_csv(row)
    #         save_csv(
    #             checked_urls,
    #             [
    #                 record.control_no,
    #                 record.bib_id,
    #                 record.url,
    #                 record.cid,
    #                 record.status,
    #             ],
    #         )
    #         print(f"({n} of {length}): {record.bib_id, record.url, record.status}")
    #         n += 1


if __name__ == "__main__":
    cli()
