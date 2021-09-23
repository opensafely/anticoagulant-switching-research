from cohortextractor import (
    StudyDefinition,
    patients,
    codelist_from_csv,
    filter_codes_by_category,
    combine_codelists,
)

from codelists import *


study = StudyDefinition(
    # Configure the expectations framework (optional)
    default_expectations={
        "date": {"earliest": "1970-01-01", "latest": "today"},
        "rate": "uniform",
        "incidence": 0.7,
    },
    ## STUDY POPULATION (required)
    population=patients.satisfying(
        """
        has_follow_up
        """,
        has_follow_up=patients.registered_with_one_practice_between(
            "2019-09-16", "2020-03-15"
        ),
    ),


    # first DOAC prescription in follow-up period
    doac_next_three_months=patients.with_these_medications(
        doac_codes,
        between=["2020-03-16", "2020-06-15"],
        return_expectations={"incidence": 0.3},
    ),

    ## DEMOGRAPHIC INFORMATION
    age_18_110= patients.satisfying(
        "age >=18 AND age <= 110",
        age = patients.age_as_of(
            "2020-03-16"),
        return_expectations={
            "incidence": 0.9,
        },
    ),

    ##  MEDICATIONS
    warfarin_6_months=patients.with_these_medications(
        warfarin_codes,
        on_or_before="2019-09-16",
        return_expectations={"incidence": .9},
    ),

    warfarin_last_three_months=patients.with_these_medications(
        warfarin_codes,
        between=["2019-12-16", "2020-03-15"],
        returning="date",
        find_last_match_in_period=True,
        include_month=True,
        include_day=False,
        return_expectations={
            "date": {"earliest": "2019-09-16", "latest": "2020-03-15"}
        },
    ),
    warfarin_next_three_months=patients.with_these_medications(
        warfarin_codes,
        between=["2020-03-16", "2020-06-15"],
        returning="date",
        find_last_match_in_period=False,
        include_month=True,
        include_day=False,
        return_expectations={
            "incidence": 0.8,
            "date": {"earliest": "2020-03-16", "latest": "2020-06-15"}
        },
    ),
    doac_last_three_months=patients.with_these_medications(
        doac_codes,
        between=["2019-12-16", "2020-03-15"],
        returning="date",
        find_last_match_in_period=True,
        include_month=True,
        include_day=False,
        return_expectations={
            "date": {"earliest": "2019-09-16", "latest": "2020-03-15"}
        },
    ),

   
)
