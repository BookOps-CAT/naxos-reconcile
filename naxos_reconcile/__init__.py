import click
from rich import print
from naxos_reconcile.prep import (
    combine_naxos_xml,
    naxos_xml_to_marc,
    prep_naxos_csv,
    prep_sierra_csv,
    edit_naxos_xml,
    prep_csv_sample,
    compare_files,
)
from naxos_reconcile.review import review_file, dedupe_file
from naxos_reconcile.check import (
    search_oclc_check_urls,
    search_oclc_only,
    check_urls_only,
)


@click.group()
def cli():
    """
    Get Naxos Data
    """
    pass


@click.option("-s", "--sierra", "sierra_file", help="Sierra .csv file to process")
@click.option("-n", "--naxos", "naxos_filepath", help="Path to Naxos files to process")
@cli.command("prep", short_help="Prep Sierra and/or Naxos file(s)")
def prep_files(sierra_file: str, naxos_filepath: str) -> None:
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


@click.option("-s", "--sierra", "sierra_file", help="Sierra .csv file to process")
@click.option("-n", "--naxos", "naxos_filepath", help="Path to Naxos files to process")
@cli.command("compare", short_help="Compare prepped Sierra and Naxos files")
def compare_infiles(sierra_file: str, naxos_filepath: str) -> None:
    print("Comparing files")
    compare_files(
        sierra_file=sierra_file,
        naxos_file=naxos_filepath,
    )


@click.option("-s", "--sierra", "sierra_file", help="Sierra .csv file to process")
@click.option("-n", "--naxos", "naxos_filepath", help="Path to Naxos files to process")
@cli.command(
    "reconcile", short_help="Process Sierra and Naxos files and then compare them"
)
def reconcile_files(sierra_file: str, naxos_filepath: str) -> None:
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
    records_to_check, records_to_import = compare_files(
        sierra_file=sierra_prepped_csv, naxos_file=naxos_prepped_csv
    )

    print("Creating sample files")
    prep_csv_sample(records_to_check)
    prep_csv_sample(records_to_import)
    print("Finished prepping files")


@click.option(
    "-f",
    "--file",
    "file",
    help="File to review",
)
@click.option(
    "-r",
    "--row",
    "row",
    default=0,
    help="Last row checked",
)
@cli.command("search", short_help="Search for records in WorldCat")
def search_worldcat(file: str, row: int) -> None:
    print(f"Searching WorldCat and checking URLs for {file}")
    results_file = search_oclc_check_urls(infile=file, last_row=row)
    print(f"Results in {results_file}")


@click.option(
    "-f",
    "--file",
    "file",
    help="File to review",
)
@click.option(
    "-r",
    "--row",
    "row",
    default=0,
    help="Last row checked",
)
@cli.command("url-check", short_help="Check URLs only")
def check_urls(file: str, row: int) -> None:
    print(f"Checking URLs for {file}")
    results_file = check_urls_only(infile=file, last_row=row)
    print(f"Results in {results_file}")


@click.option(
    "-f",
    "--file",
    "file",
    help="File to review",
)
@click.option(
    "-r",
    "--row",
    "row",
    default=0,
    help="Last row checked",
)
@cli.command("search-only", short_help="Search for records in WorldCat only")
def search_worldcat_only(file: str, row: int) -> None:
    print(f"Searching WorldCat for records for {file}")
    results_file = search_oclc_only(infile=file, last_row=row)
    print(f"Results in {results_file}")


@click.option("-f", "--file", "file", help="Path to file to review", multiple=True)
@cli.command("review", short_help="Review output of WorldCat searches and URL checks")
def review_data(file: str) -> None:
    for f in file:
        review_file(f)


def main():
    cli()
