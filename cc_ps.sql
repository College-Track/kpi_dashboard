WITH get_contact_data AS
(
    SELECT
    contact_Id,
    Gender_c,
    Ethnic_background_c,
    site_short,
-- % of students graduating from college within 6 years (numerator)
-- uses alumni who've already graduated in current AY + active PS w/ credit pace < 6 years and enrolled full-time. cohort based. year 6.
--will need to re-work for final as grade switches over 9/1 each year
    CASE
      WHEN
        grade_c = 'Year 6'
        AND indicator_completed_ct_hs_program_c = true
        AND
        (
         (college_track_status_c = '15A'
         AND Credit_Accumulation_Pace_c NOT IN ("6+ Years", 'Credit Data Missing')
         AND credits_accumulated_most_recent_c >= 80
         AND Current_Enrollment_Status_c = "Full-time"
         AND Current_School_Type_c_degree = "Predominantly bachelor's-degree granting"
         )
        OR
         college_track_status_c = '17A') THEN 1
        ELSE 0
        END AS cc_ps_projected_grad_num,
-- % of students graduating from college within 6 years (denominator)
-- cohort based, year 6
    CASE
        WHEN
        (grade_c = 'Year 6'
        AND indicator_completed_ct_hs_program_c = true) THEN 1
        ELSE 0
    END AS cc_ps_projected_grad_denom,
--% of 2yr students transferring to a 4yr within 3 years
--cohort based, trending is students in year 3. final calc is done in fall of year 4.  Will need to be reworked for final
--numerator for trending shows which of the 2-year starters, currently in year 2 have already enrolled in a 4-year.
    CASE
        WHEN
        (indicator_completed_ct_hs_program_c = true
        AND college_track_status_c = '15A'
        AND grade_c = 'Year 3'
        AND school_type = '4-Year'
        AND current_enrollment_status_c IN ("Full-time","Part-time")
        AND college_first_enrolled_school_type_c IN ("Predominantly associate's-degree granting","Predominantly certificate-degree granting")) THEN 1
        ELSE 0
        END AS x_2_yr_transfer_num,
--denom for trending, currently in year 2 started at 2-year school or lower. Will need to be reworked for final
    CASE
        WHEN
        (indicator_completed_ct_hs_program_c = true
        AND grade_c = 'Year 3'
        AND college_first_enrolled_school_type_c IN ("Predominantly associate's-degree granting","Predominantly certificate-degree granting")) THEN 1
        ELSE 0
        END AS x_2_yr_transfer_denom,

--% of students graduating with 1+ internships
--Hardcoded AY will need to be updated next year
--trending used both alumni who've already graduated in AY + students anticipated to graduate in AY
--numerator
    CASE
        WHEN
        (indicator_completed_ct_hs_program_c = true
        AND
        ps_internships_c > 0
        AND
        ((anticipated_date_of_graduation_ay_c = 'AY 2020-21'
         AND college_track_status_c = '15A'
         AND Credit_Accumulation_Pace_c NOT IN ("6+ Years", 'Credit Data Missing')
         AND credits_accumulated_most_recent_c >= 80
         AND Current_Enrollment_Status_c = "Full-time"
         AND Current_School_Type_c_degree = "Predominantly bachelor's-degree granting")
        OR
        academic_year_4_year_degree_earned_c = 'AY 2020-21')) THEN 1
        ELSE 0
    END AS cc_ps_grad_internship_num,
--denominator
    CASE
        WHEN
        (indicator_completed_ct_hs_program_c = true
        AND
        ((anticipated_date_of_graduation_ay_c = 'AY 2020-21'
         AND college_track_status_c = '15A'
         AND Credit_Accumulation_Pace_c NOT IN ("6+ Years", 'Credit Data Missing')
         AND credits_accumulated_most_recent_c >= 80
         AND Current_Enrollment_Status_c = "Full-time"
         AND Current_School_Type_c_degree = "Predominantly bachelor's-degree granting")
        OR
        academic_year_4_year_degree_earned_c = 'AY 2020-21')) THEN 1
        ELSE 0
    END AS cc_ps_grad_internship_denom,

--% of students with a 2.5+ cumulative GPA
--Will need to be reworked for final if we want to ensure we're pulling GPA from uniform point in time
    CASE
        WHEN
        (indicator_completed_ct_hs_program_c = true
        AND college_track_status_c = '15A'
        AND Prev_AT_Cum_GPA >= 2.5) THEN 1
        ELSE 0
    END AS cc_ps_gpa_2_5_num,

    FROM `data-warehouse-289815.salesforce_clean.contact_template`
--needed widest reporting group to ensure I had all students I need to evaluate if they were in one of my custom denoms. Added in more detail filter logic into case statements above.
    WHERE indicator_completed_ct_hs_program_c = true
),

-- adv rubric data from current AT
--numerators needed only, denom is all active PS students for these
get_at_data AS
(
    SELECT
    AT_Id,
    Gender_c AS at_gender,
    Ethnic_background_c AS at_ethnic_background,
    Contact_Id AS at_contact_id,
    site_short AS at_site,
--% of students completing FAFSA or equivalent
    CASE
        WHEN filing_status_c = 'FS_G' THEN 1
        ELSE 0
    END AS indicator_fafsa_complete,
--% of students on-track to have less than $30K loan debt
    CASE
        WHEN
        loans_c IN ('LN_G','LN_Y') THEN 1
        ELSE 0
    END AS indicator_loans_less_30k_loans,
--% of students attaining a well-balanced lifestyle
    CASE
        WHEN Overall_Rubric_Color = 'Green' THEN 1
        ELSE 0
    END AS indicator_well_balanced,
--% of students understand that technical and interpersonal skills are needed to create opportunities now and in the future
    CASE
        WHEN advising_rubric_career_readiness_v_2_c = 'Green'
        AND
        (academic_networking_50_cred_c = 'AN1_G'
        OR academic_networking_over_50_credits_c = 'AN2_G') THEN 1
        ELSE 0
    END AS indicator_tech_interpersonal_skills,

    FROM `data-warehouse-289815.salesforce_clean.contact_at_template`
    WHERE college_track_status_c = '15A'
    AND(
    (CURRENT_DATE() < '2021-07-01'
    AND current_as_c = TRUE)
    OR
    (CURRENT_DATE() > '2021-07-01'
    AND previous_as_c = TRUE))
),

--% of students who persist into the following year (all college students)
--first pulling in all ATs for time period I want (last fall to next fall)


set_reporting_group AS
(
    SELECT
    contact_id,
    CASE
--students who were active post-secondary & enrolled in any college during fall term
        WHEN
        (enrolled_in_any_college_c = true
        AND student_audit_status_c = 'Active: Post-Secondary') THEN 1
        ELSE 0
    END AS include_in_reporting_group
    FROM `data-warehouse-289815.salesforce_clean.contact_at_template`
    WHERE AY_Name = 'AY 2019-20'
    AND term_c = 'Fall'
),

get_persist_at_data AS
(
  SELECT
    CAT.contact_id AS persist_contact_id,
    CAT.site_short AS persist_site_short,
    CAT.Gender_c AS persist_contact_gender,
    CAT.Ethnic_background_c AS persist_contact_ethnic_background,
--count number of PATs for each student
    COUNT(AT_Id) AS at_count,
--for those same records, counting # of ATs for each student in which they met term to term persistence definition
    SUM(CASE
        WHEN
        student_audit_status_c = 'CT Alumni'
        OR indicator_persisted_at_c = 1 THEN 1
        ELSE 0
    END) AS persist_count,
    MAX(set_reporting_group.include_in_reporting_group) AS include_in_reporting_group,

    FROM `data-warehouse-289815.salesforce_clean.contact_at_template` CAT
    LEFT JOIN set_reporting_group ON set_reporting_group.contact_id = CAT.Contact_Id
--evaluating the remaining terms in 2019-20 besides summer as well as the following fall
    WHERE
        (AY_Name = 'AY 2019-20'
        AND term_c IN ('Winter','Spring'))
        OR
        (AY_Name = 'AY 2020-21'
        AND term_c = 'Fall')
    GROUP BY CAT.contact_id,
    CAT.Gender_c,
    CAT.Ethnic_background_c,
    CAT.site_short
),
--actually comparing the # terms vs. # of terms meeting persistence defintion, per student
persist_calc AS
(
    SELECT
    persist_contact_id,
    persist_contact_gender,
    persist_contact_ethnic_background,

    MAX(include_in_reporting_group) AS cc_persist_denom,
  -- if # terms = # of terms meeting persistence defintion, student will be in numerator
    MAX(
    CASE
        WHEN at_count = persist_count THEN 1
        ELSE 0
    END) AS indicator_persisted
    FROM get_persist_at_data
-- filter out any students who weren't enrolled last fall. denominator
    WHERE include_in_reporting_group = 1
    GROUP BY persist_contact_id,
    persist_contact_gender,
    persist_contact_ethnic_background
),
--trending alumni survey measures, using FY20 survey data that's been uploaded into BigQuery
get_fy20_alumni_survey_data AS
(
    SELECT
    Contact_Id AS alum_contact_id,
    gender AS alum_gender,
    ethnicity AS alum_ethnic_background,
--% of graduates with meaningful employment
    CASE
        WHEN i_feel_my_current_job_is_meaningful IN ('Strongly Agree', "Agree") THEN 1
        ELSE 0
    END AS fy20_alumni_survey_meaningful_num,
--% of graduates meeting gainful employment standard
    CASE
        WHEN
        (indicator_annual_loan_repayment_amount_current_loan_debt_125 / indicator_income_proxy) <=.08 THEN 1
        ELSE 0
    END AS fy20_alumni_survey_gainful_num,
--meaningful & gainful denom - all survey respondents
    CASE
        WHEN Contact_Id IS NOT NULL THEN 1
        ELSE 0
    END AS fy20_alumni_survey_meaningful_gainful_denom,
--% of graduates with full-time employment or enrolled in graduate school within 6 months of graduation
    CASE
        WHEN (survey_year = 'FD20'
        AND indicator_ft_job_or_grad_school_within_6_months	= 1) THEN 1
        ELSE 0
    END AS fy20_alumni_survey_employed_grad_6_months_num,
-- denom is all recent grad survey respondets (ie FD survey)
    CASE
        WHEN survey_year = 'FD20' THEN 1
        ELSE 0
    END AS fy20_alumni_survey_employed_grad_6_months_denom,

    FROM `data-warehouse-289815.surveys.fy20_alumni_survey`
-- BH had one college student graduate in spring and then somehow got access / took fy20 alum survey.
    WHERE ct_site !='College Track Boyle Heights'
),
join_data AS
(
    SELECT
    get_contact_data.*,
    get_at_data.indicator_fafsa_complete,
    get_at_data.indicator_loans_less_30k_loans,
    get_at_data.indicator_well_balanced,
    get_at_data.indicator_tech_interpersonal_skills,
    persist_calc.indicator_persisted,
    persist_calc.cc_persist_denom,
    get_fy20_alumni_survey_data.fy20_alumni_survey_meaningful_num,
    get_fy20_alumni_survey_data.fy20_alumni_survey_gainful_num,
    get_fy20_alumni_survey_data.fy20_alumni_survey_meaningful_gainful_denom,
    get_fy20_alumni_survey_data.fy20_alumni_survey_employed_grad_6_months_num,
    get_fy20_alumni_survey_data.fy20_alumni_survey_employed_grad_6_months_denom,

    FROM get_contact_data
    LEFT JOIN get_at_data ON at_contact_id = contact_id AND at_gender = Gender_c AND at_ethnic_background = Ethnic_background_c
    LEFT JOIN persist_calc ON persist_calc.persist_contact_id = contact_id AND persist_contact_gender = Gender_c AND persist_contact_ethnic_background = Ethnic_background_c
    LEFT JOIN get_fy20_alumni_survey_data ON get_fy20_alumni_survey_data.alum_contact_id = contact_id AND alum_gender = Gender_c AND alum_ethnic_background = Ethnic_background_c
),

cc_ps AS
(
    SELECT
    site_short,
    Gender_c,
    Ethnic_background_c,
    sum(cc_ps_projected_grad_num) AS cc_ps_projected_grad_num,
    sum(cc_ps_projected_grad_denom) AS cc_ps_projected_grad_denom,
    sum(x_2_yr_transfer_num) AS cc_ps_2_yr_transfer_num,
    sum(x_2_yr_transfer_denom) AS cc_ps_2_yr_transfer_denom,
    sum(cc_ps_grad_internship_num) AS cc_ps_grad_internship_num,
    sum(cc_ps_grad_internship_denom) AS cc_ps_grad_internship_denom,
    sum(cc_ps_gpa_2_5_num) AS cc_ps_gpa_2_5_num,
    sum(indicator_loans_less_30k_loans) AS cc_ps_loans_30k,
    sum(indicator_fafsa_complete) AS cc_ps_fasfa_complete,
    sum(indicator_well_balanced) AS cc_ps_well_balanced_lifestyle,
    sum(indicator_tech_interpersonal_skills) AS cc_ps_tech_interpersonal_skills,
    sum(indicator_persisted) AS cc_ps_persist_num,
    sum(cc_persist_denom) AS cc_persist_denom,
    sum(fy20_alumni_survey_meaningful_num) AS cc_ps_meaningful_num,
    sum(fy20_alumni_survey_gainful_num) AS cc_ps_gainful_num,
    sum(fy20_alumni_survey_meaningful_gainful_denom) AS cc_ps_meaningful_gainful_denom,
    sum(fy20_alumni_survey_employed_grad_6_months_num) AS cc_ps_employed_grad_6_months_num,
    sum(fy20_alumni_survey_employed_grad_6_months_denom) AS cc_ps_employed_grad_6_months_denom,

    FROM join_data
    GROUP BY site_short,
    Gender_c,
    Ethnic_background_c
)
    SELECT
    *
    FROM
    cc_ps