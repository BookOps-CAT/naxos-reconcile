# Naxos Reconciliation

A command line tool to help review and reconcile Naxos records

## Commands

All commands can be run 

### `naxos prep`
Process `.xml` files from Naxos and/or `.csv` file from Sierra export

#### Options:
`-s` `--sierra`: path to `.csv` file exported from Sierra
`-n` `--naxos`: path to MARC/XML file(s) from Naxos

#### Process:
Sierra: 
1) File is read and rows with more than one URL are be split into multiple rows
2) Comma separated output file (`sierra_prepped.csv`) is written to `/data/files/{date}` directory. File contains:
    - OCLC Number
    - Bib ID
    - URL
    - CID (unique ID from Naxos URL)

Naxos:
1) File(s) in the given directory are be read and combined into a single XML file (`naxos_combined.xml`)
2) The combined `.xml` file is edited to remove all 505 and 511 fields (to create a record that can be read with `pymarc`) and replace strings in URLs ("https://univportal.naxosmusiclibrary.com" is replaced with "https://nypl.naxosmusiclibrary.com" in 856$u). Edited records are written to `naxos_edited.xml`.
3) The edited `.xml` file is converted to MARC21 and written to `naxos_edited.mrc`.
4) The edited `.xml` file is read and data is output to a `naxos_prepped.csv` file containing:
    - URL
    - Control Number (from 001 field)
    - CID (from URL)

### `naxos compare`
Compare prepped Naxos and Sierra files

#### Options
`-s` `--sierra`: Prepped Sierra data (.csv file) to use in comparison
`-n` `--naxos`: Prepped Naxos data (.csv file) to use in comparison 

#### Process:
1) Read prepped `.csv` files for Sierra and Naxos into DataFrames
2) Drop duplicate rows from DataFrames 
3) Merge dataframes with inner join using "CID" as key. Output results of join to `records_to_check.csv`
4) Merge dataframes with outer join using "CID" as key
5) Using data from outer join, create dataframe for all resources only present in Sierra data. Export results to `records_to_delete.csv`
6) Using data from outer join, create dataframe for all resources only present in Naxos data. Export results to `records_to_import.csv`

### `naxos reconcile`
Prep files from Sierra and Naxos and then compare them

#### Options
`-s` `--sierra`: Prepped Sierra data (.csv file) to use in comparison
`-n` `--naxos`: Prepped Naxos data (.csv file) to use in comparison 

#### Process:
1) Prepares files using process outlined above in `prep`
2) Compares files using process outlined above in `compare`
3) Creates a sample file (`sample_records_to_check.csv`) with 5% of the records from `records_to_check.csv`.


### `naxos search`
Query WorldCat Metadata API for records which should be imported into Sierra and overlap records. 

#### Options
`-o`, `--overlap`: overlap file to use in WorldCat queries. Default is `sample_records_to_check.csv`
`-i`, `--import_file`: import file to use in WorldCat queries. Default is `records_to_import.csv`
`--overlap-start`: Row to start overlap search on. Default is 0.
`--import-start`: Row to start import search on. Default is 0.

#### Process
1) Queries WorldCat Metadata API for brief bibs in overlap file using the CID and the /brief-bibs/search/ endpoint. Starts search from row specified with `--overlap-start`.
2) Queries WorldCat Metadata API for brief bibs in import file using the CID and the /brief-bibs/search/ endpoint. Starts search from row specified with `--import-start`.

### `naxos review`
Reviews output of overlap and import record searches and prints review. 

#### Options
`-o`, `--overlap`: overlap file search results to review. Default is `naxos_worldcat_brief_bibs.csv`
`-i`, `--import_file`: import file search results to review. Default is `naxos_worldcat_records_to_import.csv`

#### Process
1) Review results of API queries and url checks for overlap file. Prints a total count and percentage of records with:
    - At least one match on CID in OCLC
    - Match on CID to same OCLC record in Sierra
    - Dead link in record
2) Review results of API queries and url checks for import file. Prints a total count and percentage of records with:
    - At least one match on CID in OCLC
    - Dead link in record