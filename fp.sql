WITH gather_survey_data AS
(
    SELECT
    site_short AS survey_site_short,
    Gender_c AS survey_gender,
    Ethnic_background_c AS survey_ethnic_background,
    SUM(
    CASE
        WHEN contact_id IS NOT NULL THEN 1
        ELSE 0
    END) AS ps_survey_scholarship_denom,
    SUM(
    CASE
        WHEN i_am_able_to_receive_my_scholarship_funds_from_college_track IN ('StronglyAgree', 'Strongly Agree', 'Agree') THEN 1
        ELSE 0
    END) AS ps_survey_scholarship_num

    FROM  `data-studio-260217.surveys.fy21_ps_survey_wide_prepped`
    WHERE i_am_able_to_receive_my_scholarship_funds_from_college_track IS NOT NULL
    GROUP BY site_short,
    Gender_c,
    Ethnic_background_c
),

get_at_data AS
(
    SELECT
    site_short AS at_site,
    Gender_c AS at_gender,
    Ethnic_background_c AS at_ethnic_background,
--% of college students saying that they know how to apply for Emergency Fund in PAT advising rubric
    SUM(
    CASE
        WHEN e_fund_c = 'EF_G' THEN 1
        ELSE 0
    END) AS indicator_efund,

    FROM `data-warehouse-289815.salesforce_clean.contact_at_template`
    WHERE college_track_status_c = '15A'
-- dates are used as part of filter here to handle the switch from spring AT to summer.
    AND(
    (CURRENT_DATE() < '2021-07-01'
    AND current_as_c = TRUE)
    OR
    (CURRENT_DATE() > '2021-07-01'
    AND previous_as_c = TRUE))
    GROUP BY site_short,
    Gender_c,
    Ethnic_background_c
),

gather_contact_data AS
(
--% of high school seniors with EFC by end of March. Right now we are not accounting for the timebound part as we don't currently have a way to track when it was completed.
    SELECT
    site_short,
    Gender_c AS contact_gender,
    Ethnic_background_c AS contact_ethnic_background,
    SUM(
    CASE
        WHEN (college_track_status_c = '11A'
        AND (grade_c = "12th Grade" OR (grade_c='Year 1' AND indicator_years_since_hs_graduation_c = 0))
        AND fa_req_expected_financial_contribution_c IS NOT NULL) THEN 1
        ELSE 0
    END) AS fp_12_efc_num
    FROM `data-warehouse-289815.salesforce_clean.contact_template`
    WHERE college_track_status_c IN ('11A', '12A', '15A')
    GROUP BY site_short,
    Gender_c,
    Ethnic_background_c
),

--For KPI % of students admitted to Best-Fit College choose to enroll at Best-Fit College
gather_best_fit_data AS (
    SELECT
        contact_id,
        site_short,
        Gender_c AS bf_gender,
        Ethnic_background_c AS bf_ethnic_background,

    --Students accepted to Best Fit
        (SELECT student_c
        FROM `data-warehouse-289815.salesforce_clean.college_application_clean`AS subq1
        WHERE (admission_status_c = "Accepted" AND College_Fit_Type_Applied_c = "Best Fit")
        AND Contact_Id=student_c
        group by student_c
        ) AS accepted_best_fit,

    --Students enrolled in Best Fit
        (SELECT student_c
        FROM `data-warehouse-289815.salesforce_clean.college_application_clean`AS subq2
        WHERE admission_status_c IN ("Accepted and Enrolled", "Accepted and Deferred") AND fit_type_enrolled_c = "Best Fit"
        AND Contact_Id=student_c
        group by student_c
        ) AS accepted_enrolled_best_fit

    FROM `data-warehouse-289815.salesforce_clean.contact_template`
    WHERE  college_track_status_c = '11A'
    AND (grade_c = "12th Grade" OR (grade_c='Year 1' AND indicator_years_since_hs_graduation_c = 0))
),

--Define numerator and denominator for KPI % of students admitted to Best-Fit College enroll at Best-Fit College
prep_best_fit_enrollment_kpi AS (
SELECT
    site_short AS bf_site_short,
    bf_gender,
    bf_ethnic_background,
    SUM(CASE
        WHEN accepted_best_fit IS NOT NULL
        THEN 1
        ELSE 0
    END) AS fp_accepted_best_fit_denom,

    SUM(CASE
        WHEN accepted_enrolled_best_fit IS NOT NULL
        THEN 1
        ELSE 0
    END) AS fp_enrolled_best_fit_numerator,

FROM gather_best_fit_data
GROUP BY site_short,
bf_gender,
bf_ethnic_background
),

join_data AS
(
    SELECT
    a.site_short AS fp_site_short,
    a.contact_gender AS fp_gender,
    a.contact_ethnic_background AS fp_ethnic_background,
    fp_12_efc_num AS fp_12_efc_num,
    gsd.ps_survey_scholarship_denom,
    gsd.ps_survey_scholarship_num,
    cat.indicator_efund AS fp_efund_num,
    fp_accepted_best_fit_denom,
    fp_enrolled_best_fit_numerator


    FROM gather_contact_data AS a
    LEFT JOIN gather_survey_data AS gsd ON gsd.survey_site_short = a.site_short AND gsd.survey_gender = a.contact_gender AND gsd.survey_ethnic_background = a.contact_ethnic_background
    LEFT JOIN get_at_data AS cat ON cat.at_site = a.site_short AND cat.at_gender = a.contact_gender AND cat.at_ethnic_background = a.contact_ethnic_background
    LEFT JOIN prep_best_fit_enrollment_kpi AS bfp ON bfp.bf_site_short=a.site_short AND bfp.bf_gender = a.contact_gender AND bfp.bf_ethnic_background = a.contact_ethnic_background
),

fp AS
(
    SELECT
    fp_site_short as site_short,
    fp_gender AS Gender_c,
    fp_ethnic_background AS Ethnic_background_c,
    SUM(fp_12_efc_num) AS fp_12_efc_num,
    SUM(ps_survey_scholarship_denom) AS ps_survey_scholarship_denom,
    SUM(ps_survey_scholarship_num) AS ps_survey_scholarship_num,
    SUM(fp_efund_num) AS fp_efund_num,
    SUM(fp_accepted_best_fit_denom) AS fp_accepted_best_fit_denom,
    SUM(fp_enrolled_best_fit_numerator) AS fp_enrolled_best_fit_numerator
    FROM join_data
    WHERE fp_site_short NOT IN ('The Durant Center', 'Ward 8', 'Crenshaw')
    GROUP BY fp_site_short,
    fp_gender,
    fp_ethnic_background
)

    SELECT
    *
    FROM fp