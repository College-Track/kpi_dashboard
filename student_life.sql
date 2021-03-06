WITH gather_contact_data AS(
    SELECT
        contact_id,
        site_short,
        high_school_graduating_class_c,
        Dream_Statement_filled_out_c,
        CASE
            WHEN (
                Dream_Statement_filled_out_c = True
                AND college_track_status_c = '11A'
            ) THEN 1
            ELSE 0
        END AS dream_declared,
        Ethnic_background_c,
        Gender_c
    FROM
        `data-warehouse-289815.salesforce_clean.contact_template` AS C
),

mse_reporting_group AS (
      SELECT
        student_c,
        site_short,
    SUM(
            CASE
            WHEN student_audit_status_c = 'Current CT HS Student' THEN 1
            ELSE 0
        END) AS current_at_count,
        Ethnic_background_c,
        Gender_c
    FROM
        `data-warehouse-289815.salesforce_clean.contact_at_template`
    WHERE
    --CT Status (AT) = Current CT HS Student during Spring 2019-20 AND Summer 2019-20
    GAS_Name IN ('Spring 2019-20 (Semester)', 'Summer 2019-20 (Semester)')
    AND grade_c != '8th Grade'
    GROUP BY
    student_c,
    site_short,
    Ethnic_background_c,
    Gender_c
),
--Pull meaningful summer experience data from current and previous AY (hard-entry 2019,20, 2020-21)
gather_mse_data AS (
    SELECT
        contact_id,
        c.site_short,
        --The following KPIs will pull in: Current CT HS Students and those that Completed the CT HS program. 
        --This will allow us to include PS student MSEs completed last Summer while still in HS
        --pull completed MSE last Summer
        MAX(
            CASE
                WHEN (
                    AY_name = 'AY 2019-20'
                    AND term_c = 'Summer'
                ) THEN 1
                ELSE 0
            END
        ) AS mse_completed_prev_AY,
        --pull competitive MSE last Summer
        MAX(
            CASE
                WHEN (
                    competitive_c = True
                    AND AY_name = 'AY 2019-20'
                    AND term_c = 'Summer'
                ) THEN 1
                ELSE 0
            END
        ) AS mse_competitive_prev_AY,
        --pull completed internship last AY. Should this be limited to Summer only?
        MAX(
            CASE
                WHEN (
                    type_c = 'Internship'
                    AND AY_name = 'AY 2019-20'
                ) THEN 1
                ELSE 0
            END
        ) AS mse_internship_prev_AY,
        C.Ethnic_background_c,
        C.Gender_c
    FROM
        `data-warehouse-289815.salesforce.student_life_activity_c` AS sl 
        LEFT JOIN `data-warehouse-289815.salesforce_clean.contact_at_template` AS c ON c.at_id = sl.semester_c
    WHERE
        sl.record_type_id = '01246000000ZNi8AAG' #Summer Experience
        AND AY_name IN ('AY 2020-21', 'AY 2019-20')
        AND term_c = 'Summer'
        AND experience_meaningful_c = True
        AND status_c = 'Approved'
    GROUP BY
        contact_id,
        site_short,
        Ethnic_background_c,
        Gender_c
),

--pull in students that have at least 1 workshop attendance session
--this indicates some threshold of participation in programming, even if attendance numerator < 1
gather_attendance_data AS (
    #group attendance data by student first, aggregate at site level in prep_attendance_kpi
    SELECT
        c.student_c,
        CASE
            WHEN SUM(Attendance_Denominator_c) = 0 THEN NULL
            ELSE SUM(Attendance_Numerator_c) / SUM(Attendance_Denominator_c)
        END AS sl_attendance_rate
    FROM
        `data-warehouse-289815.salesforce_clean.class_template` AS c
        LEFT JOIN `data-warehouse-289815.salesforce_clean.contact_at_template` CAT ON CAT.global_academic_semester_c = c.global_academic_semester_c
    WHERE
        Department_c = "Student Life"
        AND Cancelled_c = FALSE
        AND CAT.AY_Name = 'AY 2020-21'
    GROUP BY
        c.student_c
),
prep_attendance_kpi AS (
    SELECT
        site_short,
        CASE
            WHEN sl_attendance_rate >= 0.8 THEN 1
            ELSE 0
        END AS sl_above_80_attendance,
        Ethnic_background_c,
        Gender_c
    FROM
        gather_contact_data as gd
        LEFT JOIN gather_attendance_data AS attendance ON gd.contact_id = attendance.student_c
),

gather_survey_data AS (
  SELECT
    CT.site_short,
    -- % of students that agree or strongly agree to a statement that they have a hobby, interest, subject that they are passionate about this year'
    -- The denominator for this metric is housed in the join_prep query.
    SUM(CASE
      WHEN i_have_a_hobby_or_interest_that_i_am_passionate_about_this_year IN ("Strongly Agree", 'Agree') THEN 1
      ELSE 0
    END) AS agree_they_have_hobby,
    CT.Ethnic_background_c,
    CT.Gender_c
  FROM
    `data-studio-260217.surveys.fy21_hs_survey` S
    LEFT JOIN `data-warehouse-289815.salesforce_clean.contact_template` CT ON CT.Contact_Id = S.contact_id
    GROUP BY 
        CT.site_short,
        Ethnic_background_c,
        Gender_c
 
),
-- % % of students that have improved Belief-in-Self domain within the year
-- This KPI is done over four CTEs (could probaly be made more efficient). The majority of the logic is done in the second CTE.
gather_covi_data AS (
  SELECT
    contact_name_c,
    site_short,
    AT_Name,
    start_date_c,
    MIN(
      belief_in_self_raw_score_c
    ) AS belief_in_self_raw_score_c,
    CAT.Ethnic_background_c,
    CAT.Gender_c
  FROM
    `data-warehouse-289815.salesforce_clean.test_clean` T
    LEFT JOIN `data-warehouse-289815.salesforce_clean.contact_at_template` CAT ON CAT.AT_Id = T.academic_semester_c
  WHERE
    T.record_type_id = '0121M000001cmuDQAQ'
    AND AY_Name IN ('AY 2019-20', 'AY 2020-21')
    AND CAT.College_track_status_c = '11A'
  GROUP BY
    site_short,
    contact_name_c,
    AT_Name,
    start_date_c,
    Ethnic_background_c,
    Gender_c
  ORDER BY
    site_short,
    contact_name_c,
    start_date_c
),
calc_covi_growth AS (
  SELECT
    site_short,
    contact_name_c,
    belief_in_self_raw_score_c - lag(belief_in_self_raw_score_c) over (
      partition by contact_name_c
      order by
        start_date_c
    ) AS covi_growth,
    Ethnic_background_c,
    Gender_c
  FROM
    gather_covi_data
),
determine_covi_indicators AS (
  SELECT
    site_short,
    contact_name_c,
    CASE
      WHEN covi_growth > 0 THEN 1
      ELSE 0
    END AS covi_student_grew,
    Ethnic_background_c,
    Gender_c
  FROM
    calc_covi_growth
  WHERE
    covi_growth IS NOT NULL
),
aggregate_covi_data AS (
SELECT
  site_short,
  SUM(covi_student_grew) AS SL_covi_belief_student_grew,
  COUNT(contact_name_c) AS SL_covi_belief_denominator,
  Ethnic_background_c,
  Gender_c
FROM
  determine_covi_indicators
GROUP BY
    site_short,
    Ethnic_background_c,
    Gender_c
  ),


aggregate_attendance_kpi AS (
    SELECT
        site_short,
        SUM(sl_above_80_attendance) AS sl_above_80_attendance,
        Ethnic_background_c,
        Gender_c
    FROM
        prep_attendance_kpi
    GROUP BY
        site_short,
        Ethnic_background_c,
        Gender_c
),
aggregate_dream_kpi AS (
    SELECT
        site_short,
        SUM(dream_declared) as sl_dreams_declared,
        Ethnic_background_c,
        Gender_c
    FROM
        gather_contact_data
    GROUP BY
        site_short,
        Ethnic_background_c,
        Gender_c
),
aggregate_mse_reporting_group AS (
    SELECT
    site_short,
    COUNT(student_c) AS sl_mse_reporting_group_prev_AY,
    Ethnic_background_c,
    Gender_c
    FROM mse_reporting_group
    WHERE current_at_count = 2
    GROUP BY
    site_short,
    Ethnic_background_c,
    Gender_c
),
aggregate_mse_kpis AS (
    SELECT
        a.site_short,
        SUM(mse_completed_prev_AY) AS sl_mse_completed_prev_AY,
        SUM(mse_competitive_prev_AY) AS sl_mse_competitive_prev_AY,
        SUM(mse_internship_prev_AY) AS sl_mse_internship_prev_AY,
        Ethnic_background_c,
        Gender_c
    FROM
        gather_mse_data AS A
    GROUP BY
        site_short,
        Ethnic_background_c,
        Gender_c
),
sl_final_join AS (
SELECT
    CT.site_short,
    CT.Ethnic_background_c,
    CT.Gender_c,
    sl_dreams_declared,
    attendance_kpi.*
        EXCEPT (site_short, Ethnic_background_c, gender_c),
    mse_kpi.*
        EXCEPT (site_short,Ethnic_background_c, gender_c),
    sl_mse_reporting_group_prev_AY,
    GSD.agree_they_have_hobby AS sl_agree_they_have_hobby,
    ACD.SL_covi_belief_student_grew,
    ACD.SL_covi_belief_denominator
    
FROM
    gather_contact_data AS CT
    LEFT JOIN aggregate_dream_kpi AS dream ON dream.site_short = CT.site_short AND dream.Ethnic_background_c = CT.Ethnic_background_c AND dream.gender_c=CT.gender_c
    LEFT JOIN aggregate_attendance_kpi AS attendance_kpi ON CT.site_short = attendance_kpi.site_short AND CT.Ethnic_background_c = attendance_kpi.Ethnic_background_c AND CT.gender_c=attendance_kpi.gender_c
    LEFT JOIN aggregate_mse_kpis AS mse_kpi ON CT.site_short = mse_kpi.site_short AND CT.Ethnic_background_c = mse_kpi.Ethnic_background_c AND CT.gender_c=mse_kpi.gender_c
    LEFT JOIN aggregate_mse_reporting_group AS mse_grp ON mse_grp.site_short = CT.site_short AND mse_grp.Ethnic_background_c = CT.Ethnic_background_c AND mse_grp.gender_c=CT.gender_c
    LEFT JOIN gather_survey_data AS GSD ON GSD.site_short = CT.site_short AND CT.Ethnic_background_c = GSD.Ethnic_background_c AND CT.gender_c=GSD.gender_c
    LEFT JOIN aggregate_covi_data AS ACD ON ACD.site_short = CT.site_short AND CT.Ethnic_background_c = ACD.Ethnic_background_c AND CT.gender_c=ACD.gender_c

group by 
    site_short,
    Ethnic_background_c,
    Gender_c,
    sl_dreams_declared,
    sl_above_80_attendance,
    sl_mse_completed_prev_AY,
    sl_mse_competitive_prev_AY,
    sl_mse_internship_prev_AY ,
    sl_mse_reporting_group_prev_AY ,
    agree_they_have_hobby,
    SL_covi_belief_student_grew ,
    SL_covi_belief_denominator
)
SELECT *
FROM sl_final_join 
