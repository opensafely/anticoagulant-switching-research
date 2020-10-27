cap log close
log using "output/model", text replace

import delimited `c(pwd)'/output/input.csv, clear

recode doac_next_three_months .=0
egen agecat = cut(age), at(18, 65, 75)
recode ethnicity .=0
xtile imd_cat = imd,nq(5)
gen care_home_binary = 0
replace care_home_binary = 1 if care_home_type == "PC" | care_home_type == "PN" |care_home_type == "PS"
recode atrial_fibrillation .=0
recode ckd .=0
recode prior_rft .=0
egen inr_cat = cut(inr_test_count), at(0, 1, 4, 7)
*warfarin length should already be all non-missing
recode doac_previously .=0
recode doac_contraindication .=0


global fixed_variables			///
		i.agecat				///
		i.ethnicity				///
		i.imd					///
		i.care_home_binary		///
		i.atrial_fibrillation	///
		i.ckd					///
		i.prior_rft				///
		i.inr_cat				///
		i.warfarin_length		///
		i.doac_previously		///
		i.doac_contraindication

		
foreach var in $fixed_variables{
	melogit doac_next_three_months `var' || stp: || practice_id:, or
}


melogit doac_next_three_months $fixed_variables || stp: || practice_id:, or




log close
