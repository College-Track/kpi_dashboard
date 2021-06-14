WITH gather_hs_data AS (
  SELECT
    Contact_Id,
    site_short,
    site_c,
    Ethnic_background_c,
    Gender_c,
    -- % of seniors with GPA 3.25+
    -- The denominator for this is created in join_prep
    CASE
      WHEN grade_c = '12th Grade'
      AND Prev_AT_Cum_GPA >= 3.25 THEN 1
      ELSE 0
    END AS above_325_gpa,
    -- % of entering 9th grade students who are male
    -- The denominator for this is created in join_prep
    CASE
      WHEN grade_c = '9th Grade'
      AND Gender_c = 'Male' THEN 1
      ELSE 0
    END as male_student,
    -- % of entering 9th grade students who are low-income AND first-gen
    -- The denominator for this is created in join_prep

    CASE
      WHEN (
        grade_c = '9th Grade'
        AND indicator_low_income_c = 'Yes'
        AND indicator_first_generation_c = true
      ) THEN 1
      ELSE 0
    END AS first_gen_and_low_income,
    -- % of students with meaningful summer experiences
    -- I might need to create a new denominator for this metric
    CASE
      WHEN summer_experiences_previous_summer_c > 0 THEN 1
      ELSE 0
    END AS summer_experience
  FROM
    `data-warehouse-289815.salesforce_clean.contact_template`
  WHERE
    college_track_status_c = '11A'
),
gather_ps_count_no_gap_year AS (
SELECT
DISTINCT
Contact_Id,
site_short,
site_c,
credit_accumulation_pace_c,
current_enrollment_status_c,
college_track_status_c,
Ethnic_background_c,
Gender_c

FROM `data-warehouse-289815.salesforce_clean.contact_at_template`
WHERE AT_Grade_c = 'Year 1'
AND AT_Enrollment_Status_c != 'Approved Gap Year'
AND indicator_years_since_hs_graduation_c <= 6
AND indicator_completed_ct_hs_program_c = true
),

prep_on_track_denom AS (
SELECT site_short,
Ethnic_background_c,
Gender_c,
COUNT(Contact_Id) AS on_track_student_count
FROM gather_ps_count_no_gap_year
GROUP BY site_short,
Ethnic_background_c,
Gender_c
),

prep_on_track_data AS (
  SELECT
    Contact_Id,
    site_short,
    site_c,
        Ethnic_background_c,
    Gender_c,

    -- % of students with enough credits accumulated to graduate in 6 years
    -- The denominator for this is created in join_prep
    CASE
      WHEN (
        Credit_Accumulation_Pace_c NOT IN ("6+ Years", 'Credit Data Missing')
        AND Current_Enrollment_Status_c = "Full-time"
      ) THEN 1
      ELSE 0
    END AS on_track
  FROM
    gather_ps_count_no_gap_year
  WHERE
    college_track_status_c = '15A'
),
-- % of students meeting 80% attendance
-- The second part of the attendance kpi is done in the join_hs_data CTE
gather_ay_attendance AS (
  SELECT
    Contact_Id,
    SUM(attended_workshops_c) AS attended_workshops_c,
    SUM(enrolled_sessions_c) AS enrolled_sessions_c
  FROM
    `data-warehouse-289815.salesforce_clean.contact_at_template`
  WHERE
    AY_Name = "AY 2020-21"
  GROUP BY
    Contact_Id
),
join_hs_data AS (
  SELECT
    GHSD.*,
    CASE
      WHEN enrolled_sessions_c = 0 THEN NULL
      WHEN (attended_workshops_c / enrolled_sessions_c) >= 0.8 THEN 1
      ELSE 0
    END AS above_80_attendance
  FROM
    gather_hs_data GHSD
    LEFT JOIN gather_ay_attendance GAA ON GAA.Contact_Id = GHSD.Contact_Id
),
prep_hs_metrics AS (
  SELECT
    GSD.site_short,
        Ethnic_background_c,
    Gender_c,
    SUM(above_325_gpa) AS SD_senior_above_325,
    SUM(male_student) AS SD_ninth_grade_male,
    SUM(first_gen_and_low_income) AS SD_ninth_grade_first_gen_low_income,
    SUM(above_80_attendance) AS SD_above_80_attendance,
    SUM(summer_experience) AS SD_summer_experience,
    MAX(Account.College_Track_High_School_Capacity_c) AS hs_cohort_capacity,
  FROM
    join_hs_data GSD
    LEFT JOIN `data-warehouse-289815.salesforce.account` Account ON Account.Id = GSD.site_c
  GROUP BY
    site_short,
        Ethnic_background_c,
    Gender_c
),
prep_ps_metrics AS (
  SELECT
    site_short,
        Ethnic_background_c,
    Gender_c,
    SUM(on_track) AS SD_on_track
  FROM
    prep_on_track_data
  GROUP BY
    site_short,
        Ethnic_background_c,
    Gender_c
),

-- % of students growing toward average or above cialcial-emotional strengths
-- This KPI is done over four CTEs (could probaly be made more efficient). The majority of the logic is done in the second CTE.
gather_covi_data AS (
  SELECT
    contact_name_c,
    site_short,
    AY_Name,
        Ethnic_background_c,
    Gender_c,
    MIN(
      belief_in_self_raw_score_c + engaged_living_raw_score_c + belief_in_others_raw_score_c + emotional_competence_raw_score_c
    ) AS covi_raw_score
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
    AY_Name,
        Ethnic_background_c,
    Gender_c
  ORDER BY
    site_short,
    contact_name_c,
    AY_Name
),
calc_covi_growth AS (
  SELECT
    site_short,
        Ethnic_background_c,
    Gender_c,
    contact_name_c,
    covi_raw_score - lag(covi_raw_score) over (
      partition by contact_name_c
      order by
        AY_Name
    ) AS covi_growth
  FROM
    gather_covi_data
),
determine_covi_indicators AS (
  SELECT
    site_short,
        Ethnic_background_c,
    Gender_c,
    contact_name_c,
    CASE
      WHEN covi_growth > 0 THEN 1
      ELSE 0
    END AS covi_student_grew
  FROM
    calc_covi_growth
  WHERE
    covi_growth IS NOT NULL
),
aggregate_covi_data AS (
SELECT
  site_short,
      Ethnic_background_c,
    Gender_c,
  SUM(covi_student_grew) AS SD_covi_student_grew,
  COUNT(contact_name_c) AS SD_covi_denominator
FROM
  determine_covi_indicators
GROUP BY
  site_short,
      Ethnic_background_c,
    Gender_c
  ),

join_metrics AS (SELECT
  HS_Data.*,
  PS_Data.*
EXCEPT(site_short,Ethnic_background_c,
    Gender_c),
aggregate_covi_data.* EXCEPT (site_short,Ethnic_background_c,
    Gender_c),
POTD.on_track_student_count AS SD_on_track_student_count
FROM
  prep_hs_metrics HS_Data
  LEFT JOIN prep_ps_metrics PS_Data ON PS_Data.site_short = HS_Data.site_short AND PS_Data.Gender_c = HS_Data.Gender_c AND HS_Data.Ethnic_background_c = PS_Data.Ethnic_background_c
  LEFT JOIN aggregate_covi_data ON aggregate_covi_data.site_short = HS_Data.site_short AND aggregate_covi_data.Gender_c = HS_Data.Gender_c AND aggregate_covi_data.Ethnic_background_c = HS_Data.Ethnic_background_c
  LEFT JOIN prep_on_track_denom POTD ON POTD.site_short = HS_Data.site_short AND POTD.Gender_c = HS_Data.Gender_c AND POTD.Ethnic_background_c = HS_Data.Ethnic_background_c
 )

 SELECT *
 FROM join_metrics
