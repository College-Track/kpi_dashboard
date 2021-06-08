WITH gather_survey_data AS (
    SELECT
        site_short AS survey_site_short,
        SUM(
            CASE
                WHEN contact_id IS NOT NULL THEN 1
                ELSE 0
            END
        ) AS ps_survey_scholarship_denom,
        SUM(
            CASE
                WHEN i_am_able_to_receive_my_scholarship_funds_from_college_track IN ('StronglyAgree', 'Strongly Agree', 'Agree') THEN 1
                ELSE 0
            END
        ) AS ps_survey_scholarship_num
    FROM
        `data-studio-260217.surveys.fy21_ps_survey_wide_prepped`
    WHERE
        i_am_able_to_receive_my_scholarship_funds_from_college_track IS NOT NULL
    GROUP BY
        site_short
),
get_at_data AS (
    SELECT
        site_short AS at_site,
        --% of college students saying that they know how to apply for Emergency Fund in PAT advising rubric
        SUM(
            CASE
                WHEN e_fund_c = 'EF_G' THEN 1
                ELSE 0
            END
        ) AS indicator_efund,
    FROM
        `data-warehouse-289815.salesforce_clean.contact_at_template`
    WHERE
        college_track_status_c = '15A'
        AND(
            (
                CURRENT_DATE() < '2021-07-01'
                AND current_as_c = TRUE
            )
            OR (
                CURRENT_DATE() > '2021-07-01'
                AND previous_as_c = TRUE
            )
        )
    GROUP BY
        site_short
),
/*get_first_year_loan_debt AS
 (
 SELECT
 AT_Id,
 Contact_Id AS year_1_spring_contact_id,
 site_short AS year_1_spring_AT_site,
 CASE
 WHEN 
 
 FROM `data-warehouse-289815.salesforce_clean.contact_at_template`
 WHERE college_track_status_c = '15A'
 */
gather_contact_data AS (
    SELECT
        site_short,
        SUM(
            CASE
                WHEN fa_req_fafsa_c = 'Submitted' THEN 1
                ELSE 0
            END
        ) AS fp_12_fafsa_complete_num
    FROM
        `data-warehouse-289815.salesforce_clean.contact_template`
    WHERE
        college_track_status_c = '11A'
        AND (
            grade_c = "12th Grade"
            OR (
                grade_c = 'Year 1'
                AND indicator_years_since_hs_graduation_c = 0
            )
        )
    GROUP BY
        site_short
),
--For KPI % of students admitted to Best-Fit College choose to enroll at Best-Fit College
gather_best_fit_data AS (
    SELECT
        contact_id,
        site_short,
        --Students accepted to Best Fit
        (
            SELECT
                student_c
            FROM
                `data-warehouse-289815.salesforce_clean.college_application_clean` AS subq1
            WHERE
                (
                    admission_status_c = "Accepted"
                    AND College_Fit_Type_Applied_c = "Best Fit"
                )
                AND Contact_Id = student_c
            group by
                student_c
        ) AS accepted_best_fit,
        --Students enrolled in Best Fit
        (
            SELECT
                student_c
            FROM
                `data-warehouse-289815.salesforce_clean.college_application_clean` AS subq2
            WHERE
                admission_status_c IN ("Accepted and Enrolled", "Accepted and Deferred")
                AND fit_type_enrolled_c = "Best Fit"
                AND Contact_Id = student_c
            group by
                student_c
        ) AS accepted_enrolled_best_fit
    FROM
        `data-warehouse-289815.salesforce_clean.contact_template`
    WHERE
        college_track_status_c = '11A'
        AND (
            grade_c = "12th Grade"
            OR (
                grade_c = 'Year 1'
                AND indicator_years_since_hs_graduation_c = 0
            )
        )
),
--Define numerator and denominator for KPI % of students admitted to Best-Fit College enroll at Best-Fit College
prep_best_fit_enrollment_kpi AS (
    SELECT
        site_short,
        SUM(
            CASE
                WHEN accepted_best_fit IS NOT NULL THEN 1
                ELSE 0
            END
        ) AS fp_accepted_best_fit_numerator,
        SUM(
            CASE
                WHEN accepted_enrolled_best_fit IS NOT NULL THEN 1
                ELSE 0
            END
        ) AS fp_enrolled_best_fit_denom,
    FROM
        gather_best_fit_data
    GROUP BY
        site_short
),
join_data AS (
    SELECT
        a.site_short,
        fp_12_fafsa_complete_num AS fp_12_fasfa_num,
        gather_survey_data.ps_survey_scholarship_denom,
        gather_survey_data.ps_survey_scholarship_num,
        get_at_data.indicator_efund AS fp_efund_num,
        fp_accepted_best_fit_numerator,
        fp_enrolled_best_fit_denom
    FROM
        gather_contact_data AS a
        LEFT JOIN gather_survey_data ON gather_survey_data.survey_site_short = site_short
        LEFT JOIN get_at_data ON get_at_data.at_site = site_short
        LEFT JOIN prep_best_fit_enrollment_kpi AS prep_best_fit_enrollment_kpi ON prep_best_fit_enrollment_kpi.site_short = a.site_short
)
SELECT
    *
FROM
    join_data