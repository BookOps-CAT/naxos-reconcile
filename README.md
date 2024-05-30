# Naxos Reconciliation

A command line tool to help review and reconcile Naxos records

## Commands

### `prep`
Process `.xml` files from Naxos and/or `.csv` file from Sierra export

#### Options:
`-s` `--sierra`: path to `.csv` file exported from Sierra
`-n` `--naxos`: path to MARC/XML file(s) from Naxos

#### Process:
Sierra: 
1) File is read and rows with more than one URL are be split into multiple rows
2) Comma separated output file is written to `/data/files/{date}` directory. File contains:
    - OCLC Number
    - Bib ID
    - URL
    - CID (unique ID from Naxos URL)
Naxos:
1) File(s) in the given directory are be read and combined into a single XML file
2) The combined `.xml` file is edited to remove all 505 fields (to create a record with a valid record length) and replace strings in URLs ("https://univportal.naxosmusiclibrary.com" is replaced with "https://nypl.naxosmusiclibrary.com" in 856$u). Edited records are written to a new `.xml` file.
3) The edited `.xml` file is converted to MARC21 and written to a `.mrc` file.
4) The edited `.xml` file is read and data is output to a `.csv` file containing:
    - URL,
    - Control Number (from 001 field)
    - CID (from URL)

### `compare`
Compare prepped Naxos and Sierra files

#### Options
`-s` `--sierra`: Prepped Sierra data (.csv file) to use in comparison
`-n` `--naxos`: Prepped Naxos data (.csv file) to use in comparison 

#### Process:
1) Read prepped `.csv` files for Sierra and Naxos into DataFrames
2) Drop duplicate rows from DataFrames 
3) Merge dataframes with inner join using "CID" as key. Output results of join to "combined_urls_to_check.csv"
4) Merge dataframes with outer join using "CID" as key.
5) Create dataframe for all resources only present in Sierra data. Export results to "records_to_delete.csv"
6) Create dataframe for all resources only present in Naxos data. Export results to "records_to_import.csv"

### `reconcile`
Prep files from Sierra and Naxos and then compare them

#### Options
`-s` `--sierra`: Prepped Sierra data (.csv file) to use in comparison
`-n` `--naxos`: Prepped Naxos data (.csv file) to use in comparison 

#### Process:
1) Prepares files using process outlined above in `prep`
2) Compares files using process outlined above in `compare`

### `check-urls`
Check URLs for each row in a spreadsheet; data should be prepped first

#### Options
`-f` `--file`: file to check


### `sample`
Create a sample of data from a spreadsheet

