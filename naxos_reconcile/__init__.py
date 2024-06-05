import click
from rich import print
from naxos_reconcile.prep import (
    combine_naxos_xml,
    naxos_xml_to_marc,
    prep_naxos_csv,
    prep_sierra_csv,
    edit_naxos_xml,
    prep_csv_sample,
)
from naxos_reconcile.reconcile import compare_files
from naxos_reconcile.review import (
    review_results,
    # review_import_results,
)
from naxos_reconcile.check import worldcat_brief_bibs, worldcat_missing_records
from naxos_reconcile.utils import date_directory


@click.group()
def cli():
    """
    Get Naxos Data
    """
    pass


@click.option("-s", "--sierra", "sierra_file", help="Sierra .csv file to process")
@click.option("-n", "--naxos", "naxos_filepath", help="Path to Naxos files to process")
@cli.command("prep", short_help="Prep Sierra and/or Naxos file(s)")
def prep_files(sierra_file, naxos_filepath):
    if sierra_file:
        print("Processing Sierra .csv file")
        sierra_prepped_csv = prep_sierra_csv(sierra_file)
        print("Finished processing Sierra file(s)")
        print(f"Prepped csv file is in {sierra_prepped_csv}")
    if naxos_filepath:
        print("Processing Naxos XML files")
        naxos_combined = combine_naxos_xml(naxos_filepath)
        print(f"Combined Naxos .xml file is in {naxos_combined}")
        naxos_edited_xml = edit_naxos_xml(naxos_combined)
        print(f"Edited Naxos .xml file is in {naxos_edited_xml}")

        print("Converting Naxos XML file to MARC21")
        naxos_edited_marc = naxos_xml_to_marc(naxos_edited_xml)
        print(f"Naxos MARC21 file is in {naxos_edited_marc}")

        print("Converting Naxos XML to csv")
        naxos_prepped_csv = prep_naxos_csv(naxos_edited_xml)
        print("Finished processing Naxos file(s)")
        print(f"Prepped csv is in  file is in {naxos_prepped_csv}")
    else:
        print("No files provided.")


@click.option("-s", "--sierra", "sierra", help="Sierra .csv file to process")
@click.option("-n", "--naxos", "naxos", help="Path to Naxos files to process")
@cli.command("compare", short_help="Compare prepped Sierra and Naxos files")
def compare_infiles(sierra, naxos):
    print("Comparing files")
    compare_files(
        sierra_file=sierra,
        naxos_file=naxos,
    )


@click.option("-s", "--sierra", "sierra_file", help="Sierra .csv file to process")
@click.option("-n", "--naxos", "naxos_filepath", help="Path to Naxos files to process")
@cli.command(
    "reconcile", short_help="Process Sierra and Naxos files and then compare them"
)
def reconcile_files(sierra_file, naxos_filepath):
    print("Processing Sierra .csv file")
    sierra_prepped_csv = prep_sierra_csv(sierra_file)
    print("Finished processing Sierra file(s)")
    print(f"Prepped csv file is in {sierra_prepped_csv}")

    print("Processing Naxos XML files")
    naxos_combined = combine_naxos_xml(naxos_filepath)
    print(f"Combined Naxos .xml file is in {naxos_combined}")
    naxos_edited_xml = edit_naxos_xml(naxos_combined)
    print(f"Edited Naxos .xml file is in {naxos_edited_xml}")

    print("Converting Naxos XML file to MARC21")
    naxos_edited_marc = naxos_xml_to_marc(naxos_edited_xml)
    print(f"Naxos MARC21 file is in {naxos_edited_marc}")

    print("Converting Naxos XML to csv")
    naxos_prepped_csv = prep_naxos_csv(naxos_edited_xml)
    print("Finished processing Naxos file(s)")
    print(f"Prepped csv is in  file is in {naxos_prepped_csv}")

    print("Comparing files")
    compare_files(sierra_file=sierra_prepped_csv, naxos_file=naxos_prepped_csv)

    print("Creating sample file")
    prep_csv_sample(f"{date_directory()}/records_to_check_urls.csv")
    print(f"Sample data is in {date_directory()}/sample_records_to_check.csv")


@click.option(
    "--sample/--nosample",
    default=True,
    help="whether to run command on sample of data",
)
@click.option("-r", "--row_number", default=0, help="row number to start with")
@cli.command("review-naxos-data")
def search_worldcat(sample, row_number):
    if sample:
        print("Checking WorldCat for Naxos Records")
        csv_input = f"{date_directory()}/sample_records_to_check.csv"
        worldcat_brief_bibs(csv_input, 0)
    else:
        print("Checking WorldCat for Naxos Records")
        worldcat_brief_bibs(f"{date_directory()}/records_to_check.csv", row_number)

    print("Results of brief_bibs_search:")
    review_results(f"{date_directory()}/naxos_worldcat_brief_bibs.csv")


@cli.command("get-import-records")
def search_import_records():
    print("Checking WorldCat for Naxos Records")
    csv_input = f"{date_directory()}/records_to_import.csv"
    worldcat_missing_records(csv_input, 0)

    # print("Results of brief_bibs_search:")
    # review_import_results(f"{date_directory()}/naxos_worldcat_records_to_import.csv")


def main():
    cli()
