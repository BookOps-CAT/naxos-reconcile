import click
from rich import print
from naxos_reconcile.prep import (
    combine_naxos_xml,
    naxos_xml_to_marc,
    prep_naxos_csv,
    prep_sierra_csv,
    edit_naxos_xml,
)
from naxos_reconcile.reconcile import compare_files
from naxos_reconcile.review import (
    worldcat_check_data,
    sample_naxos_data,
    review_worldcat_results,
)
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
        sierra_csv = prep_sierra_csv(sierra_file)
        print("Finished processing Sierra file(s)")
        print(f"Prepped csv file is in {sierra_csv}")
    if naxos_filepath:
        print("Processing Naxos XML files")
        combined_xml = combine_naxos_xml(naxos_filepath)
        print(f"Combined Naxos .xml file is in {combined_xml}")
        edited_xml = edit_naxos_xml(combined_xml)
        print(f"Edited Naxos .xml file is in {edited_xml}")

        print("Converting Naxos XML file to MARC21")
        marc_file = naxos_xml_to_marc(edited_xml)
        print(f"Naxos MARC21 file is in {marc_file}")

        print("Converting Naxos XML to csv")
        naxos_csv = prep_naxos_csv(edited_xml)
        print("Finished processing Naxos file(s)")
        print(f"Prepped csv is in  file is in {naxos_csv}")
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


# @click.option("-f", "file", help=".csv file to process")
# @cli.command("check-urls", short_help="Check status of urls")
# def check_urls(infile):
#     print("Checking URLs")
#     checked = out_file("combined-urls-status.csv")
#     length = get_file_length(infile)
#     n = 1
#     with open(infile, "r", encoding="utf-8") as csvfile:
#         reader = csv.reader(csvfile, delimiter=",")
#         for row in reader:
#             record = NaxosRecord.data_from_csv(row)
#             save_csv(
#                 checked,
#                 [
#                     record.control_no,
#                     record.bib_id,
#                     record.url,
#                     record.cid,
#                     record.status,
#                 ],
#             )
#             print(f"({n} of {length}): {record.bib_id, record.url, record.status}")
#             n += 1


@click.option("-s", "--sierra", "sierra_file", help="Sierra .csv file to process")
@click.option("-n", "--naxos", "naxos_filepath", help="Path to Naxos files to process")
@cli.command(
    "reconcile", short_help="Process Sierra and Naxos files and then compare them"
)
def reconcile_files(sierra_file, naxos_filepath):
    print("Processing Sierra .csv file")
    sierra_csv = prep_sierra_csv(sierra_file)
    print("Finished processing Sierra file(s)")
    print(f"Prepped csv file is in {sierra_csv}")

    print("Processing Naxos XML files")
    combined_xml = combine_naxos_xml(naxos_filepath)
    print(f"Combined Naxos .xml file is in {combined_xml}")
    edited_xml = edit_naxos_xml(combined_xml)
    print(f"Edited Naxos .xml file is in {edited_xml}")

    print("Converting Naxos XML file to MARC21")
    marc_file = naxos_xml_to_marc(edited_xml)
    print(f"Naxos MARC21 file is in {marc_file}")

    print("Converting Naxos XML to csv")
    naxos_csv = prep_naxos_csv(edited_xml)
    print("Finished processing Naxos file(s)")
    print(f"Prepped csv is in  file is in {naxos_csv}")

    print("Comparing files")
    compare_files(sierra_file=sierra_csv, naxos_file=naxos_csv)


@click.option(
    "--sample/--nosample",
    default=False,
    help="whether to run command on sample of data",
)
@cli.command("search")
def search_worldcat(sample):
    if sample:
        infile = sample_naxos_data(f"{date_directory()}/prepped_naxos_data.csv")
    else:
        infile = f"{date_directory()}/prepped_naxos_data.csv"
    print("Checking WorldCat for Naxos Records")
    worldcat_results = worldcat_check_data(infile)
    print(f"Results in {worldcat_results}")
    review_worldcat_results(f"{date_directory()}/worldcat_check_naxos.csv")


def main():
    cli()
