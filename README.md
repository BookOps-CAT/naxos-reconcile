## Naxos Reconciliation

A command line tool to help review and reconcile Naxos records

### Commands

 - `prep`: process `.mrc` file from Naxos or `.csv` file from Sierra export
   - `.mrc` file from Naxos will be read and 505 fields will be removed; a new csv file will be created with bib ID, OCLC Number, and URL for each unique URL with a CID in each MARC record
   - `.csv` file from Sierra will be read and rows with more than one URL will be split into multiple rows
   - Options:
     - `-t` `--file-type`: Either `sierra` or `naxos`
     - `-f` `--file`: Path to file to process and prep for reconciliation
 - `check-urls`: check the URLs for each row in a spreadsheet; data should be prepped first
   - options:
     - `-f`: file to check
 - `compare`: compare prepped Naxos and Sierra files for today's date
 - `sample`: create a sample of data from a spreadsheet
 - `reconcile`: prep files from Sierra and Naxos, compare files and check URLs
   - options:
     - `-s` `--sierra`: Prepped Sierra export (.csv file) to use in comparison
     - `-n` `--naxos`: Prepped Naxos export (.csv file) to use in comparison 