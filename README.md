# OpenSAFELY Anticoagulant Switching Research

This is the code and configuration for our paper, 'OpenSAFELY: impact of the NHS guidance of switching from warfarin to direct anticoagulants (DOACs) in early phase of COVID-19 pandemic'

* The paper is [here]()
* The main analysis is in a notebook [here](https://github.com/opensafely/anticoagulant-switching-research/blob/master/notebooks/Warfarin_DOAC_rpt.ipynb), with an additional notebook assessing the associated prescribing costs [here](https://github.com/opensafely/anticoagulant-switching-research/blob/master/notebooks/DOAC_costings.ipynb).
* Raw model outputs from the "factors associated with switching" analysis, including charts, crosstabs, etc, are [here](https://github.com/opensafely/anticoagulant-switching-research/tree/master/released_outputs)
* If you are interested in how we defined our code lists, look in the [codelists folder](./codelists/).
* If you are interested in how we defined our variables (for the "factors associated" analysis), take a look at the [study definition](analysis/study_definition.py); this is written in `python`, but non-programmers should be able to understand what is going on there
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
