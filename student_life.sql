WITH gather_contact_data AS(
    SELECT
        contact_id,
        site_short,
        high_school_graduating_class_c,
        Dream_Statement_filled_out_c,
        CASE
            WHEN Dream_Statement_filled_out_c = True THEN 1
            ELSE 0
        END AS dream_declared
    FROM
        `data-warehouse-289815.salesforce_clean.contact_template` AS C
    WHERE
        college_track_status_c = '11A'
),
gather_mse_data AS (
    #current AY.  #IR note - add term 'Summer'?
    SELECT
        contact_id,
        site_short,
        type_c,
        semester_c,
        AY_name,
        CASE
            WHEN (
                sl.id IS NOT NULL
                AND AY_name = 'AY 2019-20'
                AND term_c = 'Summer'
                AND (
                    indicator_completed_ct_hs_program_c = TRUE
                    OR college_track_status_c = '11A'
                )
            ) THEN 1
            ELSE 0
        END AS mse_completed_prev_AY,
        CASE
            WHEN (
                sl.id IS NOT NULL
                AND AY_name = 'AY 2020-21'
                AND term_c = 'Summer'
            ) THEN 1
            ELSE 0
        END AS mse_completed_current_AY,
        CASE
            WHEN (
                competitive_c = True
                AND AY_name = 'AY 2019-20'
                AND term_c = 'Summer'
                AND (
                    indicator_completed_ct_hs_program_c = TRUE
                    OR college_track_status_c = '11A'
                )
            ) THEN 1
            ELSE 0
        END AS mse_competitive_prev_AY,
        CASE
            WHEN (
                competitive_c = True
                AND term_c = 'Summer'
                AND AY_name = 'AY 2020-21'
            ) THEN 1
            ELSE 0
        END AS mse_competitive_current_AY,
        CASE
            WHEN (
                type_c = 'Internship'
                AND AY_name = 'AY 2019-20'
                AND (
                    indicator_completed_ct_hs_program_c = TRUE
                    OR college_track_status_c = '11A'
                )
            ) THEN 1
            ELSE 0
        END AS mse_internship_prev_AY,
        CASE
            WHEN (
                type_c = 'Internship'
                AND AY_name = 'AY 2020-21'
            ) THEN 1
            ELSE 0
        END AS mse_internship_current_AY
    FROM
        `data-warehouse-289815.salesforce_clean.contact_at_template` AS c
        LEFT JOIN `data-warehouse-289815.salesforce.student_life_activity_c` AS sl ON c.at_id = sl.semester_c
    WHERE
        sl.record_type_id = '01246000000ZNi8AAG' #Summer Experience
        AND AY_name IN ('AY 2020-21', 'AY 2019-20') #AND term_c = 'Summer'
        AND experience_meaningful_c = True
        AND status_c = 'Approved'
),
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
    FROM
        gather_contact_data as gd
        LEFT JOIN gather_attendance_data AS attendance ON gd.contact_id = attendance.student_c
),
aggregate_attendance_kpi AS (
    SELECT
        site_short,
        SUM(sl_above_80_attendance) AS sl_above_80_attendance
    FROM
        prep_attendance_kpi
    GROUP BY
        site_short
),
aggregate_dream_kpi AS (
    SELECT
        site_short,
        SUM(dream_declared) as sl_dreams_declared
    FROM
        gather_contact_data
    GROUP BY
        site_short
),
aggregate_mse_kpis AS (
    SELECT
        site_short,
        SUM(mse_completed_prev_AY) AS sl_mse_completed_prev_AY,
        SUM(mse_completed_current_AY) AS sl_mse_completed_current_AY,
        SUM(mse_competitive_prev_AY) AS sl_mse_competitive_prev_AY,
        SUM(mse_competitive_current_AY) AS sl_mse_competitive_current_AY,
        SUM(mse_internship_prev_AY) AS sl_mse_internship_prev_AY,
        SUM(mse_internship_current_AY) AS sl_mse_internship_current_AY
    FROM
        gather_mse_data
    GROUP BY
        site_short
)
SELECT
    d.site_short,
    sl_dreams_declared,
    attendance_kpi.*
EXCEPT
    (site_short),
    mse_kpi.*
EXCEPT
    (site_short)
FROM
    aggregate_dream_kpi AS d
    LEFT JOIN aggregate_attendance_kpi AS attendance_kpi ON d.site_short = attendance_kpi.site_short
    LEFT JOIN aggregate_mse_kpis AS mse_kpi ON d.site_short = mse_kpi.site_short
GROUP BY
    site_short,
    sl_mse_completed_prev_AY,
    sl_mse_completed_current_AY,
    sl_mse_competitive_prev_AY,
    sl_mse_competitive_current_AY,
    sl_mse_internship_prev_AY,
    sl_mse_internship_current_AY,
    sl_dreams_declared,
    sl_above_80_attendance