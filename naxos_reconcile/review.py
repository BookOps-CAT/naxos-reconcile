import os
import pandas as pd
from bookops_worldcat import MetadataSession
from bookops_worldcat.errors import WorldcatRequestError

from naxos_reconcile.utils import out_file, get_token


MARC_NS = "{http://www.loc.gov/MARC21/slim}"


def sample_naxos_data(file: str) -> str:
    """sample data from prepped naxos csv"""
    naxos_sample = out_file("sample_prepped_naxos.csv")
    naxos_df = pd.read_csv(
        file,
        header=None,
        names=["URL", "CONTROL_NO", "CID"],
        dtype=str,
    )
    sample = naxos_df.sample(frac=0.01)
    sample.to_csv(naxos_sample, index=False, columns=["URL", "CONTROL_NO", "CID"])
    return naxos_sample


def brief_bib_query(x: str, session: MetadataSession):
    try:
        _ = session.brief_bibs_search(
            q=f"mn='{x}'",
            catalogSource="NAXOS",
            itemSubType="music-digital",
        )
        return _.json()
    except WorldcatRequestError:
        return {"response": "error"}


def worldcat_check_data(infile: str) -> str:
    """read Naxos csv and query worldcat for record"""
    worldcat_out = out_file("naxos_worldcat_results.csv")
    token = get_token(os.path.join(os.environ["USERPROFILE"], ".oclc/nyp_wc_test.json"))
    df = pd.read_csv(infile, names=["URL", "CONTROL_NO", "CID"])
    with MetadataSession(authorization=token) as session:
        df = pd.read_csv(infile, names=["URL", "CONTROL_NO", "CID"])
        df["JSON"] = df["CID"].apply(lambda x: brief_bib_query(x, session=session))
        df["NUMBER_OF_RECORDS"] = df["JSON"].apply(lambda x: str(x["numberOfRecords"]))
        df["OCLC_NUMBERS"] = df["JSON"].apply(
            lambda x: [i["oclcNumber"] for i in x["briefRecords"] if i]
        )
        df.drop(["JSON"], axis=1, inplace=True)
        print(df)
        df.to_csv(worldcat_out, index=False)
    return worldcat_out


def review_worldcat_results(infile: str):
    df = pd.read_csv(
        infile, names=["URL", "CONTROL_NO", "CID", "numberOfRecords", "oclcNumber"]
    )
    with_records = df[df["numberOfRecords"] == "1"]
    print(f"{with_records.shape[0]} out of {df.shape[0]} records had results in oclc")
