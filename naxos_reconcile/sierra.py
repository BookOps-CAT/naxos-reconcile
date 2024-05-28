import csv
import xml.etree.ElementTree as ET
from typing import List

import requests
from pymarc import MARCReader, MARCWriter, parse_xml_to_array

from naxos_reconcile.utils import out_file, save_csv


class NaxosRecord:
    """
    A class to define a Naxos Music Library object

    """

    def __init__(self, control_no: str, bib_id: str, url: str):
        self.control_no = control_no
        self.bib_id = bib_id
        self.url = url
        self.cid = self._get_cid()
        self.status = self._get_url_status()

    def _get_cid(self) -> str:
        """get CID from URL"""
        return self.url.split("?cid=")[1].strip()

    def _get_url_status(self) -> int:
        """check the URL and return the status code as int"""
        response = requests.get(self.url)
        return response.status_code

    @classmethod
    def data_from_csv(cls, row: List[str]):
        """
        Creates a SierraRecord from a row of a csv file
        """
        control_no = row[0].strip()
        bib_id = row[1].strip()
        url = row[2].strip()
        return cls(control_no, bib_id, url)

    @staticmethod
    def process_naxos_xml(file: str) -> str:
        """
        Reads Naxos MARC/XML file from Naxos and removes 505 fields. Some Naxos
        records are too long for pymarc to process and removing these fields
        cuts down on the record length.

        Processed MARC/XML files are written to a .xml file which is then read
        and written to a .mrc file.

        Args:

            file: file path for MARCXML file to process
        Returns:

            name of processed .mrc file as str
        """
        naxos_xml_processed = out_file("naxos-prepped.xml")
        naxos_marc_processed = out_file("naxos-prepped.mrc")
        tree = ET.parse(file)
        ET.register_namespace("marc", "http://www.loc.gov/MARC21/slim")
        root = tree.getroot()
        record = root.iterfind("./{http://www.loc.gov/MARC21/slim}record")
        for field in record:
            marc_505 = field.iterfind(
                "./{http://www.loc.gov/MARC21/slim}datafield[@tag='505']"
            )
            for field_505 in marc_505:
                field.remove(field_505)
        tree.write(
            naxos_xml_processed,
            encoding="utf-8",
            xml_declaration=True,
        )
        records = parse_xml_to_array(open(naxos_xml_processed, "rb"))
        writer = MARCWriter(open(naxos_marc_processed, "ab"))
        for record in records:
            writer.write(record)
        writer.close()
        return naxos_marc_processed

    # @staticmethod
    # def process_naxos_marc(file: str) -> str:
    #     """
    #     Reads .mrc file from Naxos and removes 505 fields. Some Naxos records are
    #     too long for pymarc to process and removing these fields cuts down on the
    #     record length.

    #     Args:

    #         file: file path for MARC21 file to process

    #     Returns:
    #         name of processed MARC21 file as a str
    #     """
    #     naxos_marc_processed = out_file("naxos-prepped-marc.mrc")
    #     reader = MARCReader(open(file, "rb"), utf8_handling="replace")
    #     for record in reader:
    #         if record is None:
    #             print(
    #                 f"Current chunk: {reader.current_chunk}, was ignored because "
    #                 f"the following exception was raised: {reader.current_exception}"
    #             )
    #         else:
    #             record.remove_fields("505")
    #             writer = MARCWriter(open(naxos_marc_processed, "ab"))
    #             writer.write(record)
    #             writer.close()
    #     return naxos_marc_processed

    @staticmethod
    def extract_naxos_data(file: str) -> str:
        """
        Reads processed Naxos MARC file and extracts bib ID, control # and url
        Args:

            file: file path for .mrc file to process

        Returns:
            name of .csv output file as a str
        """
        processed_naxos_csv = out_file("naxos-prepped-data.csv")
        reader = MARCReader(open(file, "rb"))
        for record in reader:
            urls = record.get_fields("856")
            bib_id = record["907"]["a"]
            control_no = record["035"]["a"]
            for url in urls:
                if "?cid=" in url["u"]:
                    save_csv(
                        processed_naxos_csv,
                        [
                            control_no.split("(OCoLC)")[1],
                            bib_id[1:],
                            url["u"],
                            str(url["u"].split("?cid=")[1]),
                        ],
                    )
        return processed_naxos_csv

    @staticmethod
    def process_sierra_export(infile: str) -> str:
        """
        Reads a csv file and splits rows with multiple urls into separate rows.
        URLs must be separated by a ";" and the other fields must be separated
        by another delimiter. The processed data is written to a new file.

        Args:
            infile: the path to the file to process

        Returns:
            name of outfile as str
        """
        processed_sierra_file = out_file("sierra-prepped-data.csv")
        with open(infile, "r", encoding="utf-8") as csvfile:
            reader = csv.reader(csvfile, delimiter=",")
            next(reader)
            for row in reader:
                control_no = row[0]
                bib_id = row[1]
                if ";" in row[2]:
                    urls = [i for i in row[2].split(";") if "?cid=" in i]
                    for url in urls:
                        save_csv(
                            processed_sierra_file,
                            [control_no, bib_id, url, str(url.split("?cid=")[1])],
                        )
                else:
                    if "?cid=" in row[2]:
                        url = row[2]
                        save_csv(
                            processed_sierra_file,
                            [
                                control_no.strip(),
                                bib_id,
                                url,
                                str(url.split("?cid=")[1]),
                            ],
                        )
        return processed_sierra_file
