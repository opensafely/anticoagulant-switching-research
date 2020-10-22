# OpenSAFELY Research Template

This is a template repository for making new OpenSAFELY resarch projects.  Eventually it'll become a framework. To get started, create a new repo using this repo as a template, delete this front matter, and edit the text that follows.

# _title goes here_

This is the code and configuration for our paper, _name goes here_

* The paper is [here]()
* Raw model outputs, including charts, crosstabs, etc, are in `released_analysis_results/`
* If you are interested in how we defined our variables, take a look at the [study definition](analysis/study_definition.py); this is written in `python`, but non-programmers should be able to understand what is going on there
* If you are interested in how we defined our code lists, look in the [codelists folder](./codelists/).
* Developers and epidemiologists interested in the code should review
[DEVELOPERS.md](./docs/DEVELOPERS.md).

## How to view notebooks

Notebooks live in the `notebooks/` folder (with an `ipynb`
extension). You can most easily view them [on
nbviewer](https://nbviewer.jupyter.org/github/ebmdatalab/<repo>/tree/master/notebooks/),
though looking at them in Github should also work.

To do development work, you'll need to set up a local jupyter server
and git repository - see notes below and `DEVELOPERS.md` for more detail.

## Getting started re-running these notebooks

This project uses reproducible, cross-platform
analysis notebook, using Docker.  It also includes:

* configuration for `jupytext`, to support easier code review
* cross-platform startup scripts
* best practice folder structure and documentation

Developers and analysts using this repo should
refer to [`DEVELOPERS.md`](DEVELOPERS.md) for instructions on getting
started. 

If you have not yet installed Docker, please see the [`INSTALLATION_GUIDE.md`](INSTALLATION_GUIDE.md)

### Loading SQL credentials when running notebooks
Create a file `environ.txt` in the root and set the SQL server details/credentials as follows:
`DBCONN="DRIVER={ODBC Driver 17 for SQL Server};SERVER=[servername];DATABASE=[dbname];UID=[your_UID];PWD=[your_pw]"`. 
This is referred to in `run.py` so run the notebook using command `py run.py` in Windows (outside of the server) rather than using `run.exe`.
The credentials are loaded into notebooks as follows:
`dbconn = os.environ.get('DBCONN', None).strip('"')`
`def closing_connection(dbconn):
    cnxn = pyodbc.connect(dbconn)
    try:
        yield cnxn
    finally:
        cnxn.close()'
        
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
