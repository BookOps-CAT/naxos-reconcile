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
    - CID (from URL)
    - Title
    - Publisher
    - Series title


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
3) Creates sample files `sample_records_to_import.csv` with 5% of the records from `records_to_check.csv` and `records_to_import.csv`.


### `naxos search`
Use data from prepped `.csv` files to query WorldCat Metadata API and check if the URL from the record is live. 

#### Options
`-f`, `--file`: File to get WorldCat results for.
`-r`, `--row`: The last row that was checked/the row to start the search on. Default is 0. To restart a timed-out search, enter the last completed row.

#### Process
1) Queries WorldCat Metadata API for brief bibs using the CID and the /brief-bibs/search/ endpoint. Starts search after row number provided with `--row` arg.
2) Checks if URL is live. Possible URL statuses:
    - Dead: URL check resulted in 404 error from Naxos website
    - Unavailable: Naxos website lists that item is restricted within US due to copyright 
    - Live: item is still accessible and playable online
    - Blocked: Naxos blocked access when the URL was crawled the URl should be rechecked
    - Unknown: An unknown issue arose during URL check
3) The results of the query will be output to `.csv` file with:
    - rows from input `.csv`, 
    - number of records returned by Metadata API 
    - oclc number(s)
    - cataloging agency (for records from oclc number)
    - status of URL


### `naxos review`
Reviews output of overlap and import record searches and prints review. 

#### Options
`-o`, `--overlap`: overlap file search results to review. Default is `sample_records_to_check_worldcat_results.csv`
`-i`, `--import_file`: import file search results to review. Default is `records_to_import_worldcat_results.csv`

#### Process
1) Review results of API queries and url checks for overlap file. Prints a total count and percentage of records with:
    - At least one match on CID in OCLC
    - Match on CID to same OCLC record in Sierra
    - Dead link in record
2) Review results of API queries and url checks for import file. Prints a total count and percentage of records with:
    - At least one match on CID in OCLC
    - Dead link in record