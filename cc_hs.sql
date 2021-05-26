WITH gather_data AS (
    SELECT
        contact_id,
        site_short,
        --10th Grade EFC, FAFSA4Caster only
        CASE
            WHEN (
                c.grade_c = "10th Grade"
                AND FA_Req_Expected_Financial_Contribution_c IS NOT NULL
                AND fa_req_efc_source_c = 'FAFSA4caster'
            ) THEN 1
            ELSE 0
        END AS hs_EFC_10th_count,
        CASE
            WHEN (
                c.grade_c = "10th Grade"
                AND college_track_status_c = '11A'
            ) THEN 1
            ELSE 0
        END AS hs_EFC_10th_denom_count,
        --11th Grade Aspirations, any Aspiration
        CASE
            WHEN (
                c.grade_c = '11th Grade'
                AND college_track_status_c = '11A'
                AND a.id IS NOT NULL
            ) THEN 1
            ELSE 0
        END AS aspirations_any_count,
        --11th Grade Aspirations, Affordable colleges
        CASE
            WHEN (
                c.grade_c = '11th Grade'
                AND fit_type_current_c IN ("Best Fit", "Good Fit", "Local Affordable")
            ) THEN 1
            ELSE 0
        END AS aspirations_affordable_count,
        --11th Grade Aspirations reporting group        
        CASE
            WHEN (
                c.grade_c = '11th Grade'
                AND college_track_status_c = '11A'
            ) THEN 1
            ELSE 0
        END AS aspirations_denom_count
    FROM
        `data-warehouse-289815.salesforce_clean.contact_template` AS c
        LEFT JOIN `data-warehouse-289815.salesforce.college_aspiration_c` a ON c.contact_id = a.student_c
    WHERE
        college_track_status_c = '11A'
),
--for 12th grade KPI: 80% CC attendance
gather_attendance_data AS (
    SELECT
        c.student_c,
        CASE
            WHEN SUM(Attendance_Denominator_c) = 0 THEN NULL
            ELSE SUM(Attendance_Numerator_c) / SUM(Attendance_Denominator_c)
        END AS attendance_rate
    FROM
        `data-warehouse-289815.salesforce_clean.class_template` AS c
        LEFT JOIN `data-warehouse-289815.salesforce_clean.contact_at_template` CAT ON CAT.global_academic_semester_c = c.global_academic_semester_c
    WHERE
        Department_c = "College Completion"
        AND Cancelled_c = FALSE
        AND CAT.AY_Name = 'AY 2020-21'
    GROUP BY
        c.student_c
),
--Affordable college KPIs: Applying and Acceptance to Affordable Colleges and/or Best Fit/Good Fit schools only
gather_data_twelfth_grade AS (
    SELECT
        contact_id,
        attendance_rate,
        site_short,
        --% students completing the financial aid submission and verification processes
        --May need to confirm which college applications should be included (e.g. accepted and enrolled/deferred). Currently pulling any college app
        (
            SELECT
                student_c
            FROM
                `data-warehouse-289815.salesforce_clean.college_application_clean` AS subq1
            WHERE
                FA_Req_FAFSA_c = 'Submitted'
                AND (
                    verification_status_c IN ('Submitted', 'Not Applicable')
                )
                AND Contact_Id = student_c
            group by
                student_c
        ) AS gather_fafsa_verification,
        --% of acceptances to Affordable colleges. This will be done in 2 parts since this can live in 2 fields: Fit Type (Applied), Fit Type (Enrolled)
        --Pulling in students that were accepted to an affordable option. 
        --Accepted & Enrolled/Deferred Affordable options will also be pulled in, but in another query below since these will live in Fit Type (enrolled) field
        (
            SELECT
                student_c
            FROM
                `data-warehouse-289815.salesforce_clean.college_application_clean` AS subq1
            WHERE
                (
                    admission_status_c = "Accepted"
                    AND College_Fit_Type_Applied_c IN ("Best Fit", "Good Fit", "Local Affordable")
                )
                AND Contact_Id = student_c
            group by
                student_c
        ) AS applied_accepted_affordable,
        --% of acceptances to Affordable colleges; % hs seniors who matriculate to affordable college
        --Pulling in from Fit Type (enrolled)
        --Will also be used to project matriculation to affordable colleges
        (
            SELECT
                student_c
            FROM
                `data-warehouse-289815.salesforce_clean.college_application_clean` AS subq2
            WHERE
                admission_status_c IN ("Accepted and Enrolled", "Accepted and Deferred")
                AND fit_type_enrolled_c IN (
                    "Best Fit",
                    "Good Fit",
                    "Local Affordable",
                    "Situational"
                )
                AND Contact_Id = student_c
            group by
                student_c
        ) AS accepted_enrolled_affordable,
        --% accepted to Best Fit, Good Fit
        --Same logic as the 2 queries above, except only looking at Good Fit and Best Fit in Fit Type (Applied)     
        (
            SELECT
                student_c
            FROM
                `data-warehouse-289815.salesforce_clean.college_application_clean` AS subq4
            WHERE
                admission_status_c = "Accepted"
                AND College_Fit_Type_Applied_c IN ("Best Fit", "Good Fit", "Situational")
                AND Contact_Id = student_c
            group by
                student_c
        ) AS applied_accepted_best_good_situational,
        --% accepted to Best Fit, Good Fit; % hs seniors who matriculate to Good/Best/Situational
        --Same logic as the 2 queries above, except only looking at Good Fit and Best Fit in Fit Type (Enrolled) 
        -- Will also be used to project matriculation
        (
            SELECT
                student_c
            FROM
                `data-warehouse-289815.salesforce_clean.college_application_clean` AS subq5
            WHERE
                admission_status_c IN ("Accepted and Enrolled", "Accepted and Deferred")
                AND fit_type_enrolled_c IN ("Best Fit", "Good Fit", "Situational")
                AND Contact_Id = student_c
            group by
                student_c
        ) AS accepted_enrolled_best_good_situational,
        --% applying to Best Fit and Good Fit colleges
        (
            SELECT
                student_c
            FROM
                `data-warehouse-289815.salesforce_clean.college_application_clean` AS subq3
            WHERE
                College_Fit_Type_Applied_c IN ("Best Fit", "Good Fit")
                AND Contact_Id = student_c
            group by
                student_c
        ) AS applied_best_good_situational,
    FROM
        `data-warehouse-289815.salesforce_clean.contact_template`
        LEFT JOIN gather_attendance_data ON contact_id = student_c
    WHERE
        college_track_status_c = '11A'
        AND grade_c = '12th Grade'
),
--Prepping 11th grade College Aspirations for aggregation
gather_eleventh_grade_metrics AS (
    SELECT
        site_short,
        aspirations_denom_count,
        CASE
            WHEN (
                SUM(g.aspirations_any_count) >= 6
                AND SUM(g.aspirations_affordable_count) >= 3
            ) THEN 1
            ELSE 0
        END AS cc_hs_aspirations_num_prep
    FROM
        gather_data as g
    GROUP BY
        contact_id,
        site_short,
        aspirations_denom_count
),
--Prepping attendance data, FAFSA Verification KPI, Affordable college data for aggregation
gather_twelfth_grade_metrics AS(
    SELECT
        site_short,
        CASE
            WHEN attendance_rate >= 0.8 THEN 1
            ELSE 0
        END AS cc_hs_above_80_cc_attendance,
        CASE
            WHEN gather_fafsa_verification IS NOT NULL THEN 1
            ELSE 0
        END AS fafsa_verification_prep,
        CASE
            WHEN accepted_enrolled_affordable IS NOT NULL THEN 1
            WHEN applied_accepted_affordable IS NOT NULL THEN 1
            ELSE 0
        END AS cc_hs_accepted_affordable,
        CASE
            WHEN applied_best_good_situational IS NOT NULL THEN 1
            ELSE 0
        END AS cc_hs_applied_best_good_situational,
        CASE
            WHEN accepted_enrolled_best_good_situational IS NOT NULL THEN 1
            WHEN applied_accepted_best_good_situational IS NOT NULL THEN 1
            ELSE 0
        END AS cc_hs_accepted_best_good_situational,
        --Matriculation data not available yet, so projecting matriculation based on College Application enrollment data
        CASE
            WHEN accepted_enrolled_best_good_situational IS NOT NULL THEN 1
            ELSE 0
        END AS cc_hs_enrolled_best_good_situational,
        --Matriculation data not available yet, so projecting matriculation based on College Application enrollment data
        CASE
            WHEN accepted_enrolled_affordable IS NOT NULL THEN 1
            ELSE 0
        END AS cc_hs_enrolled_affordable,
    FROM
        gather_data_twelfth_grade
),
--Aggregating 10th Grade EFC KPI
prep_tenth_grade_metrics AS (
    SELECT
        site_short,
        SUM(hs_EFC_10th_count) AS cc_hs_EFC_10th_num,
        SUM(hs_EFC_10th_denom_count) AS cc_hs_EFC_10th_denom
    FROM
        gather_data
    GROUP BY
        site_short
),
--Aggregating 11th grade College Aspirations KPI
prep_eleventh_grade_metrics AS (
    SELECT
        site_short,
        SUM(cc_hs_aspirations_num_prep) AS cc_hs_aspirations_num,
        SUM(aspirations_denom_count) AS cc_hs_aspirations_denom
    FROM
        gather_eleventh_grade_metrics
    GROUP BY
        site_short
),
--Aggregating 12th grade KPIs: CC Attendance 80%, Affordable Colleges, Best Fit/Good Fit colleges, FAFSA verification
prep_twelfth_grade_metrics AS (
    SELECT
        site_short,
        SUM(cc_hs_above_80_cc_attendance) AS cc_hs_above_80_cc_attendance,
        SUM(cc_hs_accepted_affordable) AS cc_hs_accepted_affordable,
        SUM(cc_hs_applied_best_good_situational) AS cc_hs_applied_best_good_situational,
        SUM(cc_hs_accepted_best_good_situational) AS cc_hs_accepted_best_good_situational,
        SUM(fafsa_verification_prep) AS cc_hs_financial_aid_submission_verification,
        SUM(cc_hs_enrolled_best_good_situational) AS cc_hs_enrolled_best_good_situational,
        SUM(cc_hs_enrolled_affordable) AS cc_hs_enrolled_affordable
    FROM
        gather_twelfth_grade_metrics
    GROUP BY
        site_short
) #final kpi join
SELECT
    gd.site_short,
    kpi_10th.*
EXCEPT
(site_short),
    kpi_11th.*
EXCEPT
(site_short),
    kpi_12th.*
EXCEPT
(site_short)
FROM
    gather_data as gd
    LEFT JOIN prep_tenth_grade_metrics AS kpi_10th ON gd.site_short = kpi_10th.site_short
    LEFT JOIN prep_eleventh_grade_metrics AS kpi_11th ON gd.site_short = kpi_11th.site_short
    LEFT JOIN prep_twelfth_grade_metrics AS kpi_12th ON gd.site_short = kpi_12th.site_short
GROUP BY
    site_short,
    cc_hs_EFC_10th_num,
    cc_hs_EFC_10th_denom,
    cc_hs_aspirations_num,
    cc_hs_aspirations_denom,
    cc_hs_above_80_cc_attendance,
    cc_hs_financial_aid_submission_verification,
    cc_hs_accepted_affordable,
    cc_hs_applied_best_good_situational,
    cc_hs_accepted_best_good_situational,
    cc_hs_enrolled_best_good_situational,
    #projecting matriculation
    cc_hs_enrolled_affordable #projecting matriculation