UK departmental spending over GBP 25000
=======================================

This repository contains scripts to acquire, clean and process the 
spending information released by the UK central government. 


ETL stages
----------

The scripts have several stages that need to be run in order:

* ``build_index`` - will find all related metadata (tagged: 
  spend-transactions) on data.gov.uk
* ``retrieve`` will then try to fetch all the files
* ``extract`` will attempt to parse CSV/XLS/... and load it into a DB
* ``scan_columns`` will do some initial processing for later stages
* ``map_columns`` will outsource column name comprehension to the user
* ``condense`` will try to establish a common column schema
* ``format`` will try to munge numbers and dates
* ``suppliers`` will query opencorporates.org for supplier name resolution
* ``export`` will write a csv


Running the scripts
-------------------

To run some of the scripts, use ``nosetests`` (the scripts are tests). 
Adding -v will give you the names of the individual stages, -x will 
stop on the first error and --with-xunit will generate an XML log file.

These scripts are: build_index, retrieve, extract, condense, format

The other scripts can simply be run directly


Open Issues
-----------

?

Punted
------

* PDFs
* Zip files containing a bunch of CSVs
