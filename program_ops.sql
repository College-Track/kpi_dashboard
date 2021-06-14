WITH gather_data AS (
    SELECT
        Contact_Id,
        site_short,
        Ethnic_background_c,
        Gender_c,
        -- All sites admit cohorts that meet X% low-income, X% first-generation, and X% male demographic metrics.
        -- The denominator for these metrics is calculated in join_prep
        CASE
            WHEN grade_c = '9th Grade'
                AND indicator_first_generation_c = TRUE
                AND college_track_status_c = '11A' THEN 1
            ELSE 0
            END AS incoming_cohort_first_gen,
        CASE
            WHEN grade_c = '9th Grade'
                AND (indicator_low_income_c = 'Yes')
                AND college_track_status_c = '11A' THEN 1
            ELSE 0
            END AS incoming_cohort_low_income,
    FROM `data-warehouse-289815.salesforce_clean.contact_template`
    WHERE college_track_status_c IN ('11A', '12A', '13A', '15A', '16A', '17A')
),
-- High school NPS score from student survey
     gather_survey_data AS (
         SELECT
             S.contact_id,
             CASE
                 WHEN (
                             how_likely_are_you_to_recommend_college_track_to_a_student_who_wants_to_get =
                             '10 - extremely likely'
                         OR how_likely_are_you_to_recommend_college_track_to_a_student_who_wants_to_get = '9'
                     ) THEN 1
                 ELSE 0
                 END AS nps_promoter,
             CASE
                 WHEN (
                             how_likely_are_you_to_recommend_college_track_to_a_student_who_wants_to_get !=
                             '10 - extremely likely'
                         AND how_likely_are_you_to_recommend_college_track_to_a_student_who_wants_to_get != '9'
                         AND how_likely_are_you_to_recommend_college_track_to_a_student_who_wants_to_get != '8'
                         AND how_likely_are_you_to_recommend_college_track_to_a_student_who_wants_to_get != '7'
                     ) THEN 1
                 ELSE 0
                 END AS nps_detractor
         FROM `data-studio-260217.surveys.fy21_hs_survey` S
     ),

     aggregate_metrics AS (SELECT
                               GD.site_short,
                               GD.Ethnic_background_c,
                               GD.Gender_c,
                               SUM(incoming_cohort_first_gen) AS pro_ops_incoming_cohort_first_gen,
                               SUM(incoming_cohort_low_income) AS pro_ops_incoming_cohort_low_income,
                               SUM(nps_promoter) AS nps_promoter,
                               SUM(nps_detractor) AS nps_detractor

                           FROM gather_data GD
                                LEFT JOIN gather_survey_data GSD ON GSD.contact_id = GD.Contact_Id
                           GROUP BY GD.site_short,
                                    GD.Ethnic_background_c,
                                    GD.Gender_c
     )

SELECT *
FROM aggregate_metrics



