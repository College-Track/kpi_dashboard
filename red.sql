WITH gather_data AS (
    SELECT
        Contact_Id,
        site_short,
        Ethnic_background_c,
        -- % of seniors with GPA 3.25+ AND Composite Ready
        -- Will need to update this to be more dynamic to account for lag in GPA entry.
        CASE
            WHEN (
                    most_recent_valid_cumulative_gpa >= 3.25
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
        -- % of juniors with GPA 3.25+ AND Composite Ready eleventh proxy
        -- Will need to update this to be more dynamic to account for lag in GPA entry.
        CASE
            WHEN (
                    most_recent_valid_cumulative_gpa >= 3.25
                    AND composite_readiness_most_recent_c = '1. Ready'
                    AND college_track_status_c = '11A'
                    AND (
                        grade_c = "11th Grade"
                        )
                ) THEN 1
            ELSE 0
            END AS gpa_3_25__test_ready_eleventh,
    FROM `data-warehouse-289815.salesforce_clean.contact_at_template`
    WHERE current_as_c = TRUE
      AND college_track_status_c IN ('11A', '12A', '13A', '15A', '16A', '17A')
),

-- % of high school students retained annually 
-- Done over two CTEs. The main logic is housed in the first one though.
-- MANUAL UPDATE REQUIRED
     gather_retention_data AS (
         SELECT DISTINCT
             CT.student_c,
             CAT.Ethnic_background_c,
             CAT.Gender_c,
             CASE
                 WHEN college_track_status_c IN ('11A', '18a', '12A') THEN 1
                 ELSE 0
                 END AS currently_active,
             site_short
         FROM `data-warehouse-289815.salesforce_clean.class_template` CT
              LEFT JOIN `data-warehouse-289815.salesforce_clean.contact_at_template` CAT
                        ON CAT.AT_Id = CT.Academic_Semester_c
         WHERE Attendance_Numerator_c > 0
           AND dosage_types_c NOT LIKE '%NSO%'
           AND AY_Name = "AY 2020-21"
           AND grade_c != '8th Grade'
           AND Outcome_c != 'Cancelled'
     ),
     aggregate_data AS (
         SELECT
             GRD.site_short,
             GRD.Ethnic_background_c,
             GRD.Gender_c,
             COUNT(GRD.student_c) AS retention_denom,
             SUM(GRD.currently_active) AS retention_num,
             SUM(GD.gpa_3_25__test_ready) AS red_gpa_3_25_test_ready,
             SUM(GD.gpa_3_25__test_ready_eleventh) AS red_gpa_3_25__test_ready_eleventh


         FROM gather_retention_data GRD
              LEFT JOIN gather_data GD ON GD.contact_id = GRD.student_c
         GROUP BY GRD.site_short,
                  GRD.Ethnic_background_c,
                  GRD.Gender_c
     )
SELECT *
FROM aggregate_data
