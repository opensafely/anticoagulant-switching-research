from cohortextractor import (
    StudyDefinition,
    patients,
    codelist_from_csv,
    filter_codes_by_category,
    combine_codelists
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
        (age >=18 AND age <= 110) AND
        has_follow_up AND
        warfarin_last_three_months AND
        NOT doac_last_three_months AND 
        (warfarin_next_three_months OR
        doac_next_three_months)
        """,
        has_follow_up=patients.registered_with_one_practice_between(
            "2019-09-16", "2020-03-15"
        ),
    ),
    ## OUTCOMES (at least one outcome or covariate is required)
    
    # first DOAC prescription in follow-up period
    doac_next_three_months=patients.with_these_medications(
        doac_codes,
        between=["2020-03-16", "2020-06-15"],
        returning="date",
        find_last_match_in_period=False,
        include_month=True,
        include_day=True,
        return_expectations={
            "date": {"earliest": "2020-03-16", "latest": "2020-06-15"}
        },


    died_date_ons=patients.died_from_any_cause(
        on_or_after="2020-03-16",
        returning="date_of_death",
        include_month=True,
        include_day=True,
        return_expectations={"date": {"earliest": "2020-03-16"}},
    ),
            
    dereg_date=patients.date_deregistered_from_all_supported_practices(
        on_or_after="2020-03-16", date_format="YYYY-MM",
    ),
    ## DEMOGRAPHIC INFORMATION
    age=patients.age_as_of(
        "2020-03-16",
        return_expectations={
            "rate": "universal",
            "int": {"distribution": "population_ages"},
        },
    ),
    sex=patients.sex(
        return_expectations={
            "rate": "universal",
            "category": {"ratios": {"M": 0.49, "F": 0.51}},
        }
    ),
    
    # practice 
    practice_id=patients.registered_practice_as_of(
    "2020-03-16",
    returning="pseudo_id",
    return_expectations={
        "int": {"distribution": "normal", "mean": 1000, "stddev": 100},
        "incidence": 1,
        },
            ),
    
    # STP
    stp=patients.registered_practice_as_of(
        "2020-03-16",
        returning="stp_code",
        return_expectations={
            "rate": "universal",
            "category": {
                "ratios": {
                    "STP1": 0.1,
                    "STP2": 0.1,
                    "STP3": 0.1,
                    "STP4": 0.1,
                    "STP5": 0.1,
                    "STP6": 0.1,
                    "STP7": 0.1,
                    "STP8": 0.1,
                    "STP9": 0.1,
                    "STP10": 0.1,
                }
            },
        },
    ),

    
    # ethnicity
    ethnicity=patients.with_these_clinical_events(
        ethnicity_codes,
        returning="category",
        find_last_match_in_period=True,
        on_or_before="2020-03-16",
        return_expectations={
            "category": {"ratios": {"1": 0.8, "5": 0.1, "3": 0.1}},
            "incidence": 0.75,
        },
    ),
                    
    # IMD             
    imd=patients.address_as_of(
        "2020-03-16",
        returning="index_of_multiple_deprivation",
        round_to_nearest=100,
        return_expectations={
            "rate": "universal",
            "category": {"ratios": {"100": 0.1, "200": 0.2, "300": 0.7}},
        },
    ),

    ##  MEDICATIONS
    
    warfarin_earliest=patients.with_these_medications(
        warfarin_codes,
        on_or_before="2020-03-15",
        returning="date",
        find_last_match_in_period=False,
        include_month=True,
        include_day=False,
        return_expectations={
            "date": {"earliest": "2000-09-16", "latest": "2020-03-15"}
        },
        
    warfarin_last_three_months=patients.with_these_medications(
        warfarin_codes,
        between=["2020-03-16", "2020-06-15"],
        returning="date",
        find_last_match_in_period=True,
        include_month=True,
        include_day=False,
        return_expectations={
            "date": {"earliest": "2019-09-16", "latest": "2020-03-15"}
        },
    warfarin_next_three_months=patients.with_these_medications(
        warfarin_codes,
        between=["2020-03-16", "2020-06-15"],
        returning="date",
        find_last_match_in_period=False,
        include_month=True,
        include_day=False,
        return_expectations={
            "date": {"earliest": "2019-09-16", "latest": "2020-03-15"}
        },
        
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
    
    
    ## COVARIATES
    # atrial fibrillation    
    atrial_fibrillation=patients.with_these_clinical_events(
        atrial_fibrillation_codes,
        on_or_before="2020-03-15",
        returning="binary_flag",
        return_expectations={"date": {"latest": "2020-03-15"}},
    ),

    #DOAC contrainidcation
    doac_contraindication=patients.with_these_clinical_events(
        doac_contraindication_codes,
        on_or_before="2020-03-15",
        returning="binary_flag",
        return_expectations={"date": {"latest": "2020-03-15"}},
    ),
            
    # creatinine level; to stratify kidney function (6 months)
    creatinine=patients.with_these_clinical_events(
        creatinine_codes,
        find_last_match_in_period=True,
        between=["2019-09-16", "2020-03-15"],
        returning="numeric_value",
        return_expectations={
            "float": {"distribution": "normal", "mean": 150.0, "stddev": 200.0},
            "date": {"earliest": "2019-09-16",  "latest": "2020-03-15"},
            "incidence": 0.95,
        },
    ),
            
    # chronic-kidney-disease
    ckd=patients.with_these_clinical_events(
        ckd_codes,
        on_or_before="2020-03-15",
        returning="binary_flag",
        return_expectations={"date": {"latest": "2020-03-15"}},    

    # number of INR tests in last 3 months
    inr_test_count=patients.with_these_clinical_events(
        inr_codes,
        between=["2019-12-16", "2020-03-15"],
        returning="number_of_matches_in_period",
        return_expectations={"date": {"latest": "2020-03-15"}},
    ),
            
    # most recent INR TTR value  (6 months)      
    ttr_value=patients.with_these_clinical_events(
        ttr_codes,
        returning="numeric_value",
        between=["2019-09-16", "2020-03-15"],
        find_last_match_in_period=True,
        return_expectations={"date": {"latest": "2020-03-15"}},
    ),

    # renal function test prior to march? (6 months)       
    prior_rft=patients.with_these_clinical_events(
        renal_function_test_codes,
        returning="numeric_value",
        between=["2019-09-16", "2020-03-15"],
        find_last_match_in_period=True,
        return_expectations={"date": {"latest": "2020-03-15"}},
    ),

    # renal function test after march       
    latest_rft=patients.with_these_clinical_events(
        renal_function_test_codes,
        returning="numeric_value",
        between=["2020-03-16", "2020-06-15"],
        find_last_match_in_period=True,
        return_expectations={"date": {"latest": "2020-06-15"}},
    ),

    # CAREHOME STATUS
    care_home_type=patients.care_home_status_as_of(
        "2020-03-16",
        categorised_as={
            "PC": """
              IsPotentialCareHome
              AND LocationDoesNotRequireNursing='Y'
              AND LocationRequiresNursing='N'
            """,
            "PN": """
              IsPotentialCareHome
              AND LocationDoesNotRequireNursing='N'
              AND LocationRequiresNursing='Y'
            """,
            "PS": "IsPotentialCareHome",
            "U": "DEFAULT",
        },
        return_expectations={
            "rate": "universal",
            "category": {"ratios": {"PC": 0.05, "PN": 0.05, "PS": 0.05, "U": 0.85,},},
        },
    ),

    has_consultation_history=patients.with_complete_gp_consultation_history_between(
        "2019-09-16", "2020-03-15", return_expectations={"incidence": 0.9},
    ),
)
