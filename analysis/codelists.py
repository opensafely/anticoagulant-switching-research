from cohortextractor import (
    codelist_from_csv,
    codelist,
)






ckd_codes = codelist_from_csv(
    "codelists/opensafely-chronic-kidney-disease.csv", system="ctv3", column="CTV3ID",
)

atrial_fibrillation_codes = codelist_from_csv(
    "codelists/opensafely-atrial-fibrillation-clinical-finding.csv", system="ctv3", column="CTV3Code",
)

doac_contraindication_codes = codelist_from_csv(
    "codelists/opensafely-explicit-contraindication-to-doacs-direct-acting-anticoagulants.csv", system="ctv3", column="id",
)

# DEMOGRAPHIC CODELIST
ethnicity_codes = codelist_from_csv(
    "codelists/opensafely-ethnicity.csv",
    system="ctv3",
    column="Code",
    category_column="Grouping_6",
)

# MEDICATIONS
warfarin_codes = codelist_from_csv(
    "codelists/opensafely-warfarin.csv", system="snomed", column="id",
)

doac_codes = codelist_from_csv(
    "codelists/opensafely-direct-acting-oral-anticoagulants-doac.csv", system="snomed", column="id",
)

## TESTS
inr_codes = codelist_from_csv(
    "codelists/opensafely-international-normalised-ratio-inr.csv", system="ctv3", column="id",
)

high_inr_codes = codelist_from_csv(
    "codelists/opensafely-high-international-normalised-ratio-inr.csv", system="ctv3", column="id",
)

ttr_codes = codelist(["Xaa68"], system="ctv3")

renal_function_test_codes = codelist(["451..","XE2q5","XacUK"], system="ctv3")

creatinine_codes = codelist(["XE2q5"], system="ctv3")
