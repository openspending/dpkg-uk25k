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
* ``combine`` column names are mapped and values are stored in one central table
* ``cleanup`` parses dates and amounts. Reconciles entity names with Nomenklatura.
* ``validate`` discards any transactions that don't have a parsed date and amount
* ``report`` creates the report HTML
* ``dump`` dumps all the database to spending.csv
* ``transfer`` transfers spending.csv to an OpenSpending website


Setup
-----

First clone this repo::

  git clone https://github.com/okfn/dpkg-uk25k.git

On Ubuntu you need to install a number of packages::

  sudo apt-get install python-dev postgresql libpq-dev libxml2-dev libxslt1-dev

You need to install the dependencies (best in a python virtual environment)::

  virtualenv pyenv-dpkg-uk25k
  pyenv-dpkg-uk25k/bin/pip install -r dpkg-uk25k/requirements.txt

Note: If you encounter ``Error: pg_config executable not found.`` when installing psycopg2 or compile errors for lxml ``xslt-config: not found``, it means that some of the packages were not successfully installed on the previous step.

The default configuration is in ``default.ini``. If you want to change the configuration, copy it ``config.ini`` and edit it there. To save specifying the report directory when you run report.py, add it to the config. To do the transfer stage, then an openspending.apikey needs to be specified.

Now create a postgres user for your unix user name::

  sudo -u postgres createuser -D -R -S $USER

Now check your postgres cluster is configured as UTF8 - it needs to be.

  psql -l

You should see UTF8 in the Encoding column. If it is something else then you should either create a new template with the correct encoding, or recreate your whole cluster. Note this will delete any other postgres databases! To do this::

  sudo -u postgres pg_dropcluster --stop 9.1 main
  sudo -u postgres pg_createcluster --start 9.1 main --locale=en_US.UTF-8

If you have any issues with making the database UTF8, see: http://stackoverflow.com/questions/8394789/postgresql-9-1-installation-and-database-encoding#answer-8405325

Before you can run the scripts you need to prepare a database::

  sudo -u postgres createdb uk25k

And allow access to the database by editing /etc/postgresql/9.1/main/pg_hba.conf and adding this line::

  local uk25k all trust

Now restart postgres::

  sudo service postgresql restart


Running the ETL scripts
-----------------------

Run the ETL scripts like this::

  . pyenv-dpkg-uk25k/bin/activate
  cd dpkg-uk25k
  python build_index.py
  python retrieve.py
  python extract.py
  python combine.py
  python cleanup.py
  python validate.py
  python report.py

Or do the whole lot together::

  python build_index.py && python retrieve.py && python extract.py && python combine.py && python cleanup.py && python validate.py && python report.py

When running the scripts multiple times, previously successful resources will not be processed again. Use --force to ensure they are. If you want to start completely from fresh, you can delete and recreate all tables like this::

  sudo -u postgres dropdb uk25k
  sudo -u postgres createdb uk25k

To limit the analysis to one publisher, specify the name as a parameter to build_index::

  python build_index.py wales-office

All the later steps can be confined to a particular publisher, dataset or resource using options. Use '--help' on each command for more details.

And finally, if you want to dump the resulting spend database to spending.csv and load it into OpenSpending then you can do::

  python dump.py
  python transfer.py


Optimising
----------

When the spending table gets big (10 millions rows seen 2/2014) then queries in cleanup.py and validate.py get very slow unless you create an index:

CREATE INDEX spending_resource_id_index ON spending (resource_id);


Punted
------

* PDFs
* Zip files containing a bunch of CSVs (potentially for a number of publishers)
