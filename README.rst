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
* ``validate``
* ``report`` creates the report HTML
* ``dump`` dumps all the database to spending.csv
* ``transfer`` transfers spending.csv to an OpenSpending website


Setup
-----

First clone this repo::

  git clone https://github.com/okfn/dpkg-uk25k.git

Now before you install postgresql, ensure that the locale mentions UTF8::

  locale

If it is just "en_US" then you need to change it::

  sudo update-locale LANG=en_US.UTF-8

Now reboot to see the locale as including UTF-8. Any issues, see: http://stackoverflow.com/questions/8394789/postgresql-9-1-installation-and-database-encoding#answer-8405325

On Ubuntu you need to install a number of packages::

  sudo apt-get install python-dev postgresql libpq-dev libxml2-dev libxslt1-dev

You need to install the dependencies (best in a python virtual environment)::

  virtualenv pyenv-dpkg-uk25k
  pyenv-dpkg-uk25k/bin/pip install -r dpkg-uk25k/requirements.txt

Note: If you encounter ``Error: pg_config executable not found.`` when installing psycopg2, on Ubuntu/Debian you can solve it with::

  sudo apt-get install libpq-dev python-dev

Note: If you encounter compile errors for lxml ``xslt-config: not found``, on Ubuntu you can solve it with::

  sudo apt-get install libxml2-dev libxslt1-dev

The default configuration is in ``default.ini``. If you want to change the configuration, copy it ``config.ini`` and edit it there. To do the transfer stage, then an openspending.apikey needs to be specified.

Now create a postgres user for your unix user name::

  sudo -u postgres createuser -D -R -S $USER

Now check your postgres cluster is configured as UTF8. 

  psql -l

You should see UTF8 in the Encoding column. If it is something else then you should either create a new template with the correct encoding, or recreate your whole cluster. Note this will delete any other postgres databases! To do this::

  sudo -u postgres pg_dropcluster --stop 9.1 main
  sudo -u postgres pg_createcluster --start 9.1 main --locale=en_US.UTF-8

Before you can run the scripts you need to prepare a database::

  sudo -u postgres createdb uk25k

And allow access to the database by editing /etc/postgresql/9.1/main/pg_hba.conf and adding this line::

  local uk25k all trust

Now restart postgres::

  sudo service postgresql restart


Running the scripts
-------------------

Run the scripts like this::

  . pyenv-dpkg-uk25k/bin/activate
  cd dpkg-uk25k
  python build_index.py
  python retrieve.py
  python extract.py
  python combine.py
  python cleanup.py
  python validate.py
  python report.py reports

Or do the whole lot together::

  python build_index.py && python retrieve.py && python extract.py && python combine.py && python cleanup.py && python validate.py && python report.py reports

Before running the scripts again, be sure to clear out old data from the issues table
or from all tables like this::

  sudo -u postgres dropdb uk25k
  sudo -u postgres createdb uk25k

To limit the analysis to one publisher, specify the name as a parameter to build_index::

  python build_index.py wales-office

And finally, if you want to dump the resulting spend database to spending.csv and load it into OpenSpending then you can do:

  python dump.py
  python transfer.py


Punted
------

* PDFs
* Zip files containing a bunch of CSVs (potentially for a number of publishers)
