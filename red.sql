WITH gather_data AS (
    SELECT
        Contact_Id,
        site_short,
        -- % of seniors with GPA 3.25+ AND Composite Ready
        -- Will need to update this to be more dynamic to account for lag in GPA entry.
        CASE
            WHEN (
                Prev_AT_Cum_GPA >= 3.25
                AND contact_official_test_prep_withdrawal IS NULL
                AND composite_readiness_most_recent_c = '1. Ready'
                AND college_track_status_c = '11A'
                AND (
                    grade_c = "12th Grade"
                    OR (
                        grade_c = 'Year 1'
                        AND indicator_years_since_hs_graduation_c = 0
                    )
                )
            ) THEN 1
            ELSE 0
        END AS gpa_3_25__test_ready,
    FROM
        `data-warehouse-289815.salesforce_clean.contact_at_template`
    WHERE
        current_as_c = true
        AND college_track_status_c IN ('11A')
),
aggregate_metrics AS (
    SELECT
        GD.site_short,
        SUM(GD.gpa_3_25__test_ready) AS red_gpa_3_25_test_ready
    FROM
        gather_data GD
    GROUP BY
        GD.site_short
),
-- % of high school students retained annually 
-- Done over two CTEs. The main logic is housed in the first one though. 
gather_retention_data AS (
    SELECT
        DISTINCT CT.student_c,
        CASE
            WHEN college_track_status_c IN ('11A', '18a', '12A') THEN 1
            ELSE 0
        END AS currently_active,
        site_short
    FROM
        `data-warehouse-289815.salesforce_clean.class_template` CT
        LEFT JOIN `data-warehouse-289815.salesforce_clean.contact_at_template` CAT ON CAT.AT_Id = CT.Academic_Semester_c
    WHERE
        Attendance_Numerator_c > 0
        AND dosage_types_c NOT LIKE '%NSO%'
        AND AY_Name = "AY 2020-21"
        AND grade_c != '8th Grade'
        AND Outcome_c != 'Cancelled'
),
aggregate_retention_data AS (
    SELECT
        site_short,
        COUNT(student_c) AS retention_denom,
        SUM(currently_active) AS retention_num
    FROM
        gather_retention_data
    GROUP BY
        site_short
)
SELECT
    AM.site_short,
    AM.red_gpa_3_25_test_ready,
    ARD.retention_denom,
    ARD.retention_num
FROM
    aggregate_metrics AM
    LEFT JOIN aggregate_retention_data ARD ON ARD.site_short = AM.site_short