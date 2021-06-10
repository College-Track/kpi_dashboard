WITH gather_student_data AS (
  SELECT
    "National" AS national,
    site_short,
    site_sort,
    region_abrev,
    Contact_Record_Type_Name,
    grade_c,
    indicator_completed_ct_hs_program_c,
    college_track_status_c,
    indicator_years_since_hs_graduation_c,
    COUNT(Contact_Id) AS student_count
  FROM
    `data-warehouse-289815.salesforce_clean.contact_template`
  WHERE
    college_track_status_c IN ('11A', '15A', '16A', '17A')
    AND indicator_years_since_hs_graduation_c <= 6
  GROUP BY
    site_short,
    site_sort,
    region_abrev,
    Contact_Record_Type_Name,
    grade_c,
    indicator_completed_ct_hs_program_c,
    college_track_status_c,
    indicator_years_since_hs_graduation_c
),
survey_completion AS (
  SELECT
    site_short,
    SUM(student_count) AS hs_survey_completion_denominator,
    SUM(completed_survey_count) AS hs_survey_completion_count
  FROM
    `data-studio-260217.surveys.fy21_hs_survey_completion`
  GROUP BY
    site_short
),
prep_student_counts AS (
  SELECT
    national,
    site_short,
    site_sort,
    region_abrev,
    SUM(
      IF(
        (
          Contact_Record_Type_Name = "Student: Post-Secondary"
        )
        AND (college_track_status_c = '15A'),
        student_count,
        NULL
      )
    ) AS active_ps_student_count,
    SUM(
      IF(
        Contact_Record_Type_Name = "Student: High School",
        student_count,
        NULL
      )
    ) AS hs_student_count,
    SUM(
      IF(
        (
          grade_c = "12th Grade"
          OR (
            grade_c = 'Year 1'
            AND indicator_years_since_hs_graduation_c = 0
          )
        ),
        student_count,
        NULL
      )
    ) AS hs_senior_student_count,
    SUM(
      IF(
        grade_c = "9th Grade",
        student_count,
        NULL
      )
    ) AS hs_ninth_grade_student_count,
    SUM(
      IF(
        indicator_completed_ct_hs_program_c = true,
        student_count,
        NULL
      )
    ) AS completed_hs_program_count,
  FROM
    gather_student_data
  GROUP BY
    national,
    site_short,
    site_sort,
    region_abrev
)
SELECT
  PSC.*,
  SC.hs_survey_completion_denominator,
  SC.hs_survey_completion_count
FROM
  prep_student_counts PSC
  LEFT JOIN survey_completion SC ON SC.site_short = PSC.site_short