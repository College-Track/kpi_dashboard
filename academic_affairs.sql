WITH gather_data AS (
  SELECT
    Contact_Id,
    site_short,
    -- % of students with a 3.25 GPA
    -- Will need to make this more dynamic to account for GPA lag
    CASE
      WHEN Prev_AT_Cum_GPA >= 3.25 THEN 1
      ELSE 0
    END AS above_325_gpa,
    -- % of seniors who are composite ready
    -- Will need to add in the opt-out indicator once it is made.
    CASE
      WHEN Readiness_Composite_Off_c = '1. Ready'
      AND (
        grade_c = '12th Grade'
        OR (
          grade_c = 'Year 1'
          AND indicator_years_since_hs_graduation_c = 0
        )
      )
      AND contact_official_test_prep_withdrawal IS NULL THEN 1
      ELSE 0
    END AS composite_ready,
    -- % of seniors who are composite ready (11th grade proxy)
    CASE
      WHEN Readiness_Composite_Off_c = '1. Ready'
      AND contact_official_test_prep_withdrawal IS NULL
      AND (grade_c = '11th Grade') THEN 1
      ELSE 0
    END AS composite_ready_eleventh_grade,
    -- % of not ready and near ready students who become composite ready by the highest official exam score
    CASE
      WHEN Readiness_10_th_Composite_c IN('2. Near Ready', '3. Not Ready')
      AND contact_official_test_prep_withdrawal IS NULL
      AND grade_c NOT IN ('9th Grade', '10th Grade') THEN 1
      ELSE 0
    END AS tenth_grade_test_not_ready,
    CASE
      WHEN (Readiness_Composite_Off_c = '1. Ready')
      AND contact_official_test_prep_withdrawal IS NULL
      AND (
        Readiness_10_th_Composite_c IN('2. Near Ready', '3. Not Ready')
      ) THEN 1
      ELSE 0
    END AS offical_test_ready
  FROM
    `data-warehouse-289815.salesforce_clean.contact_at_template`
  WHERE
    current_as_c = true
    AND college_track_status_c IN ('11A')
),
-- % of students with 80% attendance in AA workshops
-- This KPI is done over two CTEs the filtering occurs in the first and the logic occurs in the second
aa_attendance_prep AS (
  SELECT
    C.student_c,
    CASE
      WHEN SUM(Attendance_Denominator_c) = 0 THEN NULL
      ELSE SUM(Attendance_Numerator_c) / SUM(Attendance_Denominator_c)
    END AS attendance_rate
  FROM
    `data-warehouse-289815.salesforce_clean.class_template` C
    LEFT JOIN `data-warehouse-289815.salesforce_clean.contact_at_template` CAT ON CAT.global_academic_semester_c = C.global_academic_semester_c
  WHERE
    department_c = "Academic Affairs"
    AND CAT.AY_Name = 'AY 2020-21'
  GROUP BY
    student_c
),
aa_attendance_kpi AS (
  SELECT
    student_c,
    CASE
      WHEN attendance_rate >= 0.8 THEN 1
      ELSE 0
    END AS above_80_aa_attendance
  FROM
    aa_attendance_prep
),
gather_survey_data AS (
  SELECT
    CT.site_short,
    S.contact_id,
    --  % of students that agree or strongly agree to this statement: “I am in control of my academic performance”
    CASE
      WHEN ct_helps_me_better_understand_that_i_am_in_control_of_my_academic_performance = "Strongly Agree" THEN 1
      WHEN ct_helps_me_better_understand_that_i_am_in_control_of_my_academic_performance = 'Agree' THEN 1
      ELSE 0
    END AS i_am_in_control_of_my_academic_performance,
    -- Growth in the % of students that agree or strongly agree to this statement: “I feel prepared to engage in academic stretch opportunities.”
    -- Not doing any growth calculation here, just displaying the raw percent. Can follow up with VS if that is a concern.
    CASE
      WHEN i_feel_prepared_to_engage_in_academic_stretch_opportunities = "Strongly Agree" THEN 1
      WHEN i_feel_prepared_to_engage_in_academic_stretch_opportunities = 'Agree' THEN 1
      ELSE 0
    END AS i_feel_prepared_to_engage_in_academic_stretch_opportunities
  FROM
    `data-studio-260217.surveys.fy21_hs_survey` S
    LEFT JOIN `data-warehouse-289815.salesforce_clean.contact_template` CT ON CT.Contact_Id = S.contact_id
)
SELECT
  GD.site_short,
  SUM(above_325_gpa) AS aa_above_325_gpa,
  SUM(composite_ready) AS aa_composite_ready,
  SUM(composite_ready_eleventh_grade) AS aa_composite_ready_eleventh_grade,
  SUM(tenth_grade_test_not_ready) AS aa_tenth_grade_test_not_ready,
  SUM(offical_test_ready) AS aa_offical_test_ready,
  SUM(above_80_aa_attendance) AS aa_above_80_aa_attendance,
  SUM(i_am_in_control_of_my_academic_performance) AS aa_i_am_in_control_of_my_academic_performance,
  SUM(
    i_feel_prepared_to_engage_in_academic_stretch_opportunities
  ) AS aa_i_feel_prepared_to_engage_in_academic_stretch_opportunities
FROM
  gather_data GD
  LEFT JOIN aa_attendance_kpi AA ON GD.Contact_Id = AA.student_c
  LEFT JOIN gather_survey_data GSD ON GSD.contact_id = GD.contact_id
GROUP BY
  site_short