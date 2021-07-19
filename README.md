# OpenSAFELY Anticoagulant Switching Research

This is the code and configuration for our paper, 'OpenSAFELY: impact of the NHS guidance of switching from warfarin to direct anticoagulants (DOACs) in early phase of COVID-19 pandemic'

* The preprint version of our paper is [here](https://www.medrxiv.org/content/10.1101/2020.12.03.20243535v1) which has been made available prior to peer review.
* The main analysis is in a notebook [here](https://github.com/opensafely/anticoagulant-switching-research/blob/master/notebooks/Warfarin_DOAC_rpt.ipynb), including charts and tables, with additional more detailed outputs [here](https://github.com/opensafely/anticoagulant-switching-research/tree/master/output). This was carried out in the very early stages of OpenSAFELY development during the pandemic and uses a deprecated method for accessing OpenSafely data involving SQL. See below for further details.
* An additional notebook assessing the associated prescribing costs is [here](https://github.com/opensafely/anticoagulant-switching-research/blob/master/notebooks/DOAC_costings.ipynb).
* Raw model outputs from the "factors associated with switching" analysis, including charts, crosstabs, etc, are [here](https://github.com/opensafely/anticoagulant-switching-research/tree/master/released_outputs)
* If you are interested in how we defined our code lists, look in the [codelists folder](./codelists/) and all codelists are available on [OpenCodelists](https://codelists.opensafely.org/) for re-use.
* If you are interested in how we defined our variables (for the "factors associated" analysis), take a look at the [study definition](analysis/study_definition.py); this is written in `python`, but non-programmers should be able to understand what is going on there
* Developers and epidemiologists interested in the code should review
[DEVELOPERS.md](./docs/DEVELOPERS.md).

## How to view notebooks

Notebooks live in the `notebooks/` folder (with an `ipynb`
extension). You can most easily view them [on
nbviewer](https://nbviewer.jupyter.org/github/ebmdatalab/<repo>/tree/master/notebooks/),
if looking at them in Github does not work.

To do development work, you'll need to set up a local jupyter server
and git repository - see notes below.

## Getting started re-running these notebooks

This project uses reproducible, cross-platform
analysis notebook, using Docker.  It also includes:

* configuration for `jupytext`, to support easier code review
* cross-platform startup scripts
* best practice folder structure and documentation

### Loading SQL credentials when running notebooks

The main notebook here is fed from live SQL connection to either the dummy or real data held on OpenSAFELY. 
For developers re-running this within OpenSAFELY, you need to create a local file `environ.txt` in your local drive with SQL server details/credentials as follows:
`DBCONN="DRIVER={ODBC Driver 17 for SQL Server};SERVER=[servername];DATABASE=[dbname];UID=[your_UID];PWD=[your_pw]"` (do not keep the square brackets). 
This can also be run in dummy data (outside of the secure server), launch the notebook using command `py run.py` in Windows.
Within the server, add `--env-file <path>/environ.txt` to the docker run command (replacing `<path>` with the location of the `environ.txt` file.

The credentials are loaded into notebooks as follows:
```python
dbconn = os.environ.get('DBCONN', None).strip('"')
def closing_connection(dbconn):
    cnxn = pyodbc.connect(dbconn)
    try:
        yield cnxn
    finally:
        cnxn.close()
```
        
# About the OpenSAFELY framework

The OpenSAFELY framework is a new secure analytics platform for
electronic health records research in the NHS.

Instead of requesting access for slices of patient data and
transporting them elsewhere for analysis, the framework supports
developing analytics against dummy data, and then running against the
real data *within the same infrastructure that the data is stored*.
Read more at [OpenSAFELY.org](https://opensafely.org).

The framework is under fast, active development to support rapid
analytics relating to COVID19; we're currently seeking funding to make
it easier for outside collaborators to work with our system.  You can
read our current roadmap [here](ROADMAP.md).
