WITH --Gather contact and academic term data to join with COVI data to set reporting groups
gather_at_data AS (
    SELECT
        full_name_c,
        at_id,
        contact_id,
        AY_Name,
        site_short
    FROM
        `data-warehouse-289815.salesforce_clean.contact_at_template`
    WHERE
        College_Track_Status_Name = 'Current CT HS Student'
        AND (
            grade_c IN ('9th Grade', '10th Grade', '11th Grade')
            OR (
                grade_c = "12th Grade"
                OR (
                    grade_c = 'Year 1'
                    AND indicator_years_since_hs_graduation_c = 0
                )
            )
        )
),
--Pull Covi assessments completed within appropriate AYs (2019-20, 2020-21)
gather_covi_data AS (
    SELECT
        contact_name_c AS contact_id_covi,
        contact_id,
        --from contact_at
        id AS test_record_id,
        site_short,
        AY_Name,
        belief_in_self_raw_score_c,
        engaged_living_raw_score_c,
        belief_in_others_raw_score_c,
        emotional_competence_raw_score_c
    FROM
        `data-warehouse-289815.salesforce_clean.test_clean` AS COVI
        LEFT JOIN gather_at_data AS GAD ON GAD.at_id = COVI.academic_semester_c
    WHERE
        COVI.record_type_id = '0121M000001cmuDQAQ' --Covitality test record type
        AND status_c = 'Completed'
        AND AY_Name = 'AY 2020-21'
),
--Setting groundwork for KPI indicator: % of students who have taken the the CoVi assessment each academic year (2020-21AY)
--Data gathered through 2 CTEs. Denom should be Current CT HS Student
--Wondering if this should be 2019-20AY instead?
completing_covi_data AS (
    SELECT
        site_short,
        contact_id,
        CASE
            WHEN test_record_id IS NOT NULL THEN 1
            ELSE 0
        END AS covi_assessment_completed_ay
    FROM
        gather_covi_data
    WHERE
        AY_Name = 'AY 2020-21'
    GROUP BY
        contact_id,
        site_short,
        test_record_id
),
--Isolate students that completed a Covitality assessment in 2020-21AY
students_that_completed_covi AS (
    SELECT
        COUNT(DISTINCT contact_id) AS wellness_covi_assessment_completed_ay,
        site_short
    FROM
        completing_covi_data
    WHERE
        covi_assessment_completed_ay = 1
    GROUP BY
        site_short
),
-- % of students served by Wellness who "strongly agree" wellness services assisted them in managing their stress, helping them engage in self-care practices and/or enhancing their mental health
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
            ) THEN 1
            ELSE 0
        END AS strongly_agree_wellness_services_assisted_them
    FROM
        `data-studio-260217.surveys.fy21_hs_survey` S
        LEFT JOIN `data-warehouse-289815.salesforce_clean.contact_template` CT ON CT.Contact_Id = S.contact_id
    WHERE
        did_you_engage_with_wellness_services_at_your_site = "Yes"
),
aggregate_wellness_survey_data AS (
    SELECT
        COUNT (DISTINCT students_receiving_wellness_services) AS wellness_survey_wellness_services_assisted_denom,
        SUM(strongly_agree_wellness_services_assisted_them) AS wellness_survey_wellness_services_assisted_num,
        site_short
    FROM
        gather_wellness_survey_data
    GROUP BY
        site_short
),
--Growth KPIs and students completing Covitality KPI
prep_all_wellness_kpis AS (
    SELECT
        A.site_short,
        wellness_covi_assessment_completed_ay,
        wellness_survey_wellness_services_assisted_num,
        wellness_survey_wellness_services_assisted_denom
    FROM
        students_that_completed_covi AS A
        LEFT JOIN aggregate_wellness_survey_data AS B ON A.site_short = B.site_short
)
SELECT
    *
FROM
    prep_all_wellness_kpis