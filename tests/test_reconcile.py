import os
import pandas as pd
from naxos_reconcile.reconcile import compare_files
from naxos_reconcile.utils import out_file


def test_compare_files(test_date_directory, mock_date_directory):
    sierra_df = pd.DataFrame(
        [
            [
                "123",
                "456",
                "https://nypl.naxosmusiclibrary.com/catalogue/item.asp?cid=foo",
                "foo",
            ],
            [
                "987",
                "654",
                "https://nypl.naxosmusiclibrary.com/catalogue/item.asp?cid=bar",
                "bar",
            ],
        ]
    )
    sierra_df.to_csv(out_file("sierra.csv"), index=False)
    naxos_df = pd.DataFrame(
        [
            [
                "https://nypl.naxosmusiclibrary.com/catalogue/item.asp?cid=foo",
                "foo" "foo",
            ],
            [
                "https://nypl.naxosmusiclibrary.com/catalogue/item.asp?cid=baz",
                "baz",
                "baz",
            ],
        ]
    )
    naxos_df.to_csv(out_file("naxos.csv"), index=False)
    compare_files(sierra_file=out_file("naxos.csv"), naxos_file=out_file("naxos.csv"))
    assert sorted(
        [
            "records_to_check.csv",
            "records_to_delete.csv",
            "records_to_import.csv",
            "sierra.csv",
            "naxos.csv",
        ]
    ) == sorted(os.listdir(test_date_directory))
