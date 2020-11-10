cap log close
log using "output/model", text replace

import delimited `c(pwd)'/output/input.csv, clear

recode doac_next_three_months .=0
egen agecat = cut(age), at(18, 65, 75, 120)
recode ethnicity .=0
xtile imd_cat = imd,nq(5)
gen care_home_binary = 0
replace care_home_binary = 1 if care_home_type == "PC" | care_home_type == "PN" |care_home_type == "PS"
recode atrial_fibrillation .=0

* Sex
gen male = 1 if sex == "M"
replace male = 0 if sex == "F"
*********************************eGFR ****************************************
* Set implausible creatinine values to missing (Note: zero changed to missing)
replace creatinine = . if !inrange(creatinine, 20, 3000) 

* Divide by 88.4 (to convert umol/l to mg/dl)
gen SCr_adj = creatinine/88.4

gen min = .
replace min = SCr_adj/0.7 if male==0
replace min = SCr_adj/0.9 if male==1
replace min = min^-0.329  if male==0
replace min = min^-0.411  if male==1
replace min = 1 if min<1

gen max=.
replace max=SCr_adj/0.7 if male==0
replace max=SCr_adj/0.9 if male==1
replace max=max^-1.209
replace max=1 if max>1

gen egfr=min*max*141
replace egfr=egfr*(0.993^age)
replace egfr=egfr*1.018 if male==0
label var egfr "egfr calculated using CKD-EPI formula with no eth"

* Categorise into ckd stages
egen egfr_cat_all = cut(egfr), at(0, 15, 30, 45, 60, 5000)
recode egfr_cat_all 0 = 5 15 = 4 30 = 3 45 = 2 60 = 0, generate(ckd_egfr)
/* 
0 "No CKD, eGFR>60" 	or missing -- have been shown reasonable in CPRD
2 "stage 3a, eGFR 45-59" 
3 "stage 3b, eGFR 30-44" 
4 "stage 4, eGFR 15-29" 
5 "stage 5, eGFR <15"
*/
gen egfr_cat = .
recode egfr_cat . = 3 if egfr < 30
recode egfr_cat . = 2 if egfr < 60
recode egfr_cat . = 1 if egfr < .
replace egfr_cat = 0 if egfr >= .
label define egfr_cat 	1 ">=60" 		///
						2 "30-59"		///
						3 "<30"			///
						0 "Unknown"
label values egfr_cat egfr_cat
***************************************************************
recode prior_rft .=0
egen inr_cat = cut(inr_test_count), at(0, 1, 4, 7, 1000)
*warfarin length should already be all non-missing
recode doac_previously .=0
recode doac_contraindication .=0

global count_variables			///
		agecat					///
		/*ethnicity*/			///
		imd_cat					///
		care_home_binary		///
		atrial_fibrillation		///
		egfr_cat				///
		prior_rft				///
		inr_cat					///
		warfarin_length			///
		doac_previously			///
		doac_contraindication


foreach var in $count_variables{
	table `var' doac_next_three_months
}

global fixed_variables			///
		i.agecat				///
		/*i.ethnicity*/			///
		i.imd_cat				///
		i.care_home_binary		///
		i.atrial_fibrillation	///
		i.egfr_cat				///
		i.prior_rft				///
		i.inr_cat				///
		i.warfarin_length		///
		i.doac_previously		///
		i.doac_contraindication

		
foreach var in $fixed_variables{
	melogit doac_next_three_months `var' || stp:, or
}

melogit doac_next_three_months $fixed_variables || stp:, or

predict predictions_f,xb
qui corr doac_next_three_months predictions_f
di "R-squared - fixed effects (%): " round(r(rho)^2*100,.1)

predict predictions_r, reffects
qui corr doac_next_three_months predictions_r
di "R-squared - random effects (%): " round(r(rho)^2*100,.1)


log close
