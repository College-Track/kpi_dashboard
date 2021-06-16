WITH

--Gather contact and academic term data to join with COVI data to set reporting groups
gather_at_data AS
(
SELECT
    full_name_c,
    at_id,
    contact_id,
    AY_Name,
    site_short,
    Ethnic_background_c,
    Gender_c

FROM `data-warehouse-289815.salesforce_clean.contact_at_template`
WHERE College_Track_Status_Name = 'Current CT HS Student'
    AND
        (grade_c IN ('9th Grade','10th Grade','11th Grade')
        OR
        (grade_c = "12th Grade" OR (grade_c='Year 1' AND indicator_years_since_hs_graduation_c = 0)))
),

--Pull Covi assessments completed within appropriate AYs (2020-21)
gather_covi_data AS (
SELECT
    contact_name_c AS contact_id_covi,
    contact_id, --from contact_at
    id AS test_record_id,
    site_short,
    AY_Name,
    co_vitality_scorecard_color_c,
    belief_in_self_raw_score_c,
    engaged_living_raw_score_c,
    belief_in_others_raw_score_c,
    emotional_competence_raw_score_c,
    GAD.Ethnic_background_c,
    GAD.Gender_c

FROM `data-warehouse-289815.salesforce_clean.test_clean` AS COVI
LEFT JOIN gather_at_data AS GAD
    ON GAD.at_id = COVI.academic_semester_c

WHERE COVI.record_type_id ='0121M000001cmuDQAQ' --Covitality test record type
    AND status_c = 'Completed'
    AND AY_Name = 'AY 2020-21'
),

--Setting groundwork for KPI indicator: % of students who have taken the the CoVi assessment each academic year (2020-21AY)
--Gathered in 2 CTEs
completing_covi_data AS (
SELECT
    site_short,
    contact_id,
    CASE
        WHEN test_record_id IS NOT NULL
        THEN 1
        ELSE 0
        END AS covi_assessment_completed_ay,
    Ethnic_background_c,
    Gender_c
FROM gather_covi_data
WHERE AY_Name = 'AY 2020-21'
GROUP BY
    contact_id,
    site_short,
    test_record_id,
    Ethnic_background_c,
    Gender_c
),

--Isolate students that completed a Covitality assessment in 2020-21AY
students_that_completed_covi AS (
SELECT
    COUNT(DISTINCT contact_id) AS wellness_covi_assessment_completed_ay,
    site_short,
    Ethnic_background_c,
    Gender_c

FROM completing_covi_data
WHERE covi_assessment_completed_ay = 1
GROUP BY
    site_short,
    Ethnic_background_c,
    Gender_c
),

-- KPI: % of students served by Wellness who "strongly agree" wellness services assisted them in managing their stress, helping them engage in self-care practices and/or enhancing their mental health
-- The denominator for this metric will be students who answered "yes" to receiving wellness services
gather_wellness_survey_data AS (
SELECT
    CT.site_short,
    S.contact_id AS students_receiving_wellness_services,

    CASE
        WHEN (
            working_with_college_track_wellness_services_has_assisted_you_in_managing_your_s IN ("Strongly Agree", "Totalmente de acuerdo")
            OR working_with_college_tracks_wellness_programming_has_helped_you_engage_in_self_c IN ("Strongly Agree", "Totalmente de acuerdo")
            OR working_with_college_tracks_wellness_services_has_enhanced_your_mental_health IN ("Strongly Agree", "Totalmente de acuerdo")
            )
        THEN 1
        ELSE 0
        END AS strongly_agree_wellness_services_assisted_them,
        CT.Ethnic_background_c,
        CT.Gender_c

FROM `data-studio-260217.surveys.fy21_hs_survey` S
    LEFT JOIN `data-warehouse-289815.salesforce_clean.contact_template` CT ON CT.Contact_Id = S.contact_id
WHERE did_you_engage_with_wellness_services_at_your_site = "Yes"
),

aggregate_wellness_survey_data AS (
SELECT
    COUNT (DISTINCT students_receiving_wellness_services) AS wellness_survey_wellness_services_assisted_denom,
    SUM(strongly_agree_wellness_services_assisted_them) AS wellness_survey_wellness_services_assisted_num,
    site_short,
    Ethnic_background_c,
    Gender_c
FROM gather_wellness_survey_data
GROUP BY
    site_short,
    Ethnic_background_c,
    Gender_c
),


--Average # of sessions for reb/blue Covi students or 1:1 (case notes)
--% of students/# of sessions/amt of time that students with red and blue CoVi scores have spent receiving support/counseling/coaching for their social emotional wellbeing health (either through a workshop, small group or 1:1s)
--Broken down into various CTEs to gather students with red/blue covi, wellness sessions attended, and case notes logged

--Pulling students with red/blue covi from academic term (2020-21AY)
gather_red_blue_covi_at AS (
SELECT
        contact_id,
        site_short,
        co_vitality_scorecard_color_c,
   MAX(CASE
            WHEN co_vitality_scorecard_color_c IN ('Blue','Red')
            THEN 1
            ELSE NULL
        END) AS wellness_blue_red_denom,

        Ethnic_background_c,
        Gender_c
FROM `data-warehouse-289815.salesforce_clean.contact_at_template`
WHERE grade_c != '8th Grade'
    AND college_track_status_c = '11A'
    AND AY_NAME = "AY 2020-21"
    AND Term_c = "Fall"
GROUP BY
    contact_Id,
    site_short,
    co_vitality_scorecard_color_c,
    Ethnic_background_c,
    Gender_c
),

--Sum students that have a red or blue covitality color at some point during 2020-21AY
sum_of_blue_red_covi AS (
SELECT
        site_short,
        SUM(wellness_blue_red_denom) AS sum_of_blue_red_covi_for_avg, #students with blue/red Covitality scorecard colors for denominator
        Ethnic_background_c,
        Gender_c
FROM gather_red_blue_covi_at
GROUP BY
    site_short,
    Ethnic_background_c,
    Gender_c
),

 --gather Wellness sessions attended during 2020-21
gather_wellness_attendance_data AS (
SELECT
    SUM(attendance_numerator_c) AS sum_attended_wellness_sessions,
    site_short,
    CAT.Ethnic_background_c,
    CAT.Gender_c
    FROM `data-warehouse-289815.salesforce_clean.class_template` CT
    LEFT JOIN `data-warehouse-289815.salesforce_clean.contact_at_template` CAT ON CAT.AT_Id = CT.Academic_Semester_c
        WHERE
        Attendance_Numerator_c > 0
        AND department_c = 'Wellness'
        AND dosage_types_c NOT LIKE '%NSO%'
        AND AY_NAME = "AY 2020-21"
        AND grade_c != '8th Grade'
        AND Outcome_c != 'Cancelled'
        AND college_track_status_c = '11A'
        AND contact_id IN (SELECT contact_id FROM `data-warehouse-289815.salesforce_clean.contact_at_template` where co_vitality_scorecard_color_c IN  ('Blue','Red') AND AY_name = "AY 2020-21" AND Term_c = "Fall")
    GROUP BY
            site_short,
            Ethnic_background_c,
            Gender_c
),

--Prepare case note data for aggregation: red/blue covi as receiving 1:1 support by site. Will be used to add together with Wellness sessions attended
--1 casenote = 1 session
gather_case_note_data AS (
SELECT
    COUNT (DISTINCT CSE.id) AS case_note_count, #case note id
    site_short,
    CAT.Ethnic_background_c,
    CAT.Gender_c

FROM `data-warehouse-289815.salesforce_clean.contact_at_template` CAT
LEFT JOIN `data-warehouse-289815.salesforce.progress_note_c` CSE ON CAT.AT_Id = CSE.Academic_Semester_c
WHERE Type_Counseling_c = TRUE
    AND AY_name = 'AY 2020-21'
    AND college_track_status_c = '11A'
    AND contact_id IN (SELECT contact_id FROM `data-warehouse-289815.salesforce_clean.contact_at_template` where co_vitality_scorecard_color_c IN  ('Blue','Red') AND AY_name = "AY 2020-21" AND Term_c = "Fall")
GROUP BY
    site_short,
    Ethnic_background_c,
    Gender_c
),

--Add 1:1 case notes and sessions attended by student to average out later
combine_sessions_and_case_notes AS (
SELECT
    ATTNDCE.site_short,
    SUM(sum_attended_wellness_sessions + case_note_count) AS sum_wellness_support_received,
    ATTNDCE.Ethnic_background_c,
    ATTNDCE.Gender_c

FROM gather_wellness_attendance_data AS ATTNDCE
LEFT JOIN gather_case_note_data AS CSE ON ATTNDCE.site_short = CSE.site_short
GROUP BY
    site_short,
    Ethnic_background_c,
    Gender_c
),

calculate_avg_wellness_services_per_blue_red_covi AS (
SELECT
    a.site_short,
    CASE
        WHEN wellness_blue_red_denom IS NOT NULL
        THEN (sum_wellness_support_received/sum_of_blue_red_covi_for_avg)
        ELSE NULL
        END AS wellness_avg_support, # of sessions or 1:1 / Students with reb,blue Covi scorecard color
    a.Ethnic_background_c,
    a.Gender_c

FROM combine_sessions_and_case_notes AS a
LEFT JOIN sum_of_blue_red_covi AS b ON a.site_short=b.site_short AND a.ethnic_background_c=b.ethnic_background_c AND a.Gender_c=b.Gender_c
LEFT JOIN gather_red_blue_covi_at AS C ON a.site_short=c.site_short AND a.ethnic_background_c=c.ethnic_background_c AND c.Gender_c=b.Gender_c
WHERE wellness_blue_red_denom IS NOT NULL

),
prep_avg_wellness_services AS (
SELECT
    site_short,
    wellness_avg_support, # of sessions or 1:1 / Students with reb,blue Covi scorecard color
    Ethnic_background_c,
    Gender_c

FROM calculate_avg_wellness_services_per_blue_red_covi AS a
GROUP BY
    a.site_short,
    wellness_avg_support,
    Ethnic_background_c,
    Gender_c
),

aggregate_kpis_data AS(
SELECT
    a.site_short,
    MAX(wellness_covi_assessment_completed_ay) AS wellness_covi_assessment_completed_ay,
    wellness_avg_support,
    MAX(sum_attended_wellness_sessions) AS sum_attended_wellness_sessions,
    MAX(case_note_count) AS case_note_count,
    MAX(sum_of_blue_red_covi_for_avg) AS sum_of_blue_red_covi_for_avg,
    MAX(wellness_survey_wellness_services_assisted_denom) AS wellness_survey_wellness_services_assisted_denom,
    MAX(wellness_survey_wellness_services_assisted_num) AS wellness_survey_wellness_services_assisted_num,
    a.Ethnic_background_c,
    a.Gender_c

FROM students_that_completed_covi AS a
LEFT JOIN aggregate_wellness_survey_data AS b ON b.site_short = a.site_short AND a.ethnic_background_c = b.ethnic_background_c AND a.Gender_c=b.Gender_c
LEFT JOIN prep_avg_wellness_services AS c ON c.site_short = a.site_short AND a.ethnic_background_c = c.ethnic_background_c AND a.Gender_c=c.Gender_c
LEFT JOIN gather_wellness_attendance_data AS d ON a.site_short = d.site_short AND a.ethnic_background_c = d.ethnic_background_c AND a.Gender_c=d.Gender_c--for total sessions
LEFT JOIN gather_case_note_data AS e ON a.site_short=e.site_short AND a.ethnic_background_c = e.ethnic_background_c AND a.Gender_c=e.Gender_c --for total case notes
LEFT JOIN sum_of_blue_red_covi AS f ON a.site_short=f.site_short AND a.ethnic_background_c = f.ethnic_background_c AND a.Gender_c=f.Gender_c --for stotal number of red/blue covi students
GROUP BY
    site_short,
    wellness_avg_support,
    Ethnic_background_c,
    Gender_c
)

SELECT *
FROM aggregate_kpis_data