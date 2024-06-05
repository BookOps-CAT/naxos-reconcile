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
2) The combined `.xml` file is edited to remove all 505 and 511 fields (to create a record with a valid record length) and replace strings in URLs ("https://univportal.naxosmusiclibrary.com" is replaced with "https://nypl.naxosmusiclibrary.com" in 856$u). Edited records are written to `naxos_edited.xml`.
3) The edited `.xml` file is converted to MARC21 and written to `naxos_edited.mrc`.
4) The edited `.xml` file is read and data is output to a `naxos_prepped.csv` file containing:
    - URL,
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
3) Merge dataframes with inner join using "CID" as key. Output results of join to `records_to_check_urls.csv`
4) Merge dataframes with outer join using "CID" as key.
5) Create dataframe for all resources only present in Sierra data. Export results to `records_to_delete.csv`
6) Create dataframe for all resources only present in Naxos data. Export results to `records_to_import.csv`

### `naxos reconcile`
Prep files from Sierra and Naxos and then compare them

#### Options
`-s` `--sierra`: Prepped Sierra data (.csv file) to use in comparison
`-n` `--naxos`: Prepped Naxos data (.csv file) to use in comparison 

#### Process:
1) Prepares files using process outlined above in `prep`
2) Compares files using process outlined above in `compare`
3) Creates a `sample_records_to_check.csv` with 5% of the records from `records_to_check_urls.csv`.


### `naxos review-naxos-data`
Query WorldCat Metadata API for Naxos records using data from overlap of Naxos MARC/XML and Sierra export

#### Options
`--sample/--nosample`: whether or not to create a sample of the prepped Naxos data and query just that sample. Default is True.
`-r`, `--row_number`: row number to start with. Default is 0 but the script can be restarted if it hits an error by including this option.

### `naxos get-import-records`
Query WorldCat Metadata API for records from Naxos MARC/XML that are missing in Sierra
