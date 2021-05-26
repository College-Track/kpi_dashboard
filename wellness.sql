WITH --gather contact and academic term data to join with COVI data
gather_at_data AS (
    SELECT
        full_name_c,
        at_id,
        at_name,
        contact_id,
        AY_Name,
        site
    FROM
        `data-warehouse-289815.salesforce_clean.contact_at_template`
    WHERE
        record_type_id = '01246000000RNnSAAW' --HS student contact record type
        AND site != 'College Track Arlen'
        AND College_Track_Status_Name = 'Current CT HS Student'
        AND AY_Name = 'AY 2020-21' --To pull in Covi test records within this given AY 
),
gather_covi_data AS (
    SELECT
        contact_name_c AS contact_id_covi,
        academic_semester_c AS covi_at,
        co_vitality_scorecard_color_c,
        belief_in_self_raw_score_c,
        belief_in_others_raw_score_c,
        emotional_competence_raw_score_c,
        engaged_living_raw_score_c,
        --add Covi Domain scores to obtain total raw Covitality score
        SUM(
            belief_in_self_raw_score_c + belief_in_others_raw_score_c + emotional_competence_raw_score_c + engaged_living_raw_score_c
        ) AS raw_covi_score,
        version_c,
        status_c,
        --test_date_c, 
        co_vitality_test_completed_date_c,
        id AS test_record_id,
        student_site_c,
        record_type_id
    FROM
        `data-warehouse-289815.salesforce_clean.test_clean` AS COVI
    WHERE
        record_type_id = '0121M000001cmuDQAQ' --Covitality test record type
        AND status_c = 'Completed'
    GROUP BY
        contact_name_c,
        academic_semester_c,
        co_vitality_scorecard_color_c,
        belief_in_self_raw_score_c,
        belief_in_others_raw_score_c,
        emotional_competence_raw_score_c,
        engaged_living_raw_score_c,
        version_c,
        status_c,
        co_vitality_test_completed_date_c,
        id,
        --test record id
        student_site_c,
        record_type_id
) --Join contact_at data with COVI data to obtain contact id and pull in 2020-21AY Covi data
--join_term_data_with_covi AS (
SELECT
    full_name_c,
    contact_id_covi,
    co_vitality_test_completed_date_c,
    at_id,
    covi_at,
    --at_name,
    student_site_c,
    raw_covi_score,
    contact_id,
    AY_NAME,
    test_record_id,
    CASE
        WHEN test_record_id IS NOT NULL THEN 1
        ELSE 0
    END AS covi_assessment_completed_ay
FROM
    gather_at_data AS A --LEFT JOIN gather_covi_data AS C ON A.contact_id = C.contact_id_covi
    LEFT JOIN gather_covi_data AS C ON A.at_id = C.covi_at
GROUP BY
    full_name_c,
    contact_id_covi,
    co_vitality_test_completed_date_c,
    at_id,
    covi_at,
    --at_name,
    student_site_c,
    raw_covi_score,
    contact_id,
    AY_NAME,
    test_record_id
),
gather_students_with_more_than_1_covi AS (
    SELECT
        contact_id,
        SUM(covi_assessment_completed_ay) AS sum_of_covi_tests_taken_ay #does sudent have more than 1 covi assessment?
    FROM
        join_term_data_with_covi
    WHERE
        covi_assessment_completed_ay = 1
    GROUP BY
        contact_id
),
gather_first_covi_test_date AS (
    SELECT
        (
            SELECT
                MIN(co_vitality_test_completed_date_c)
            FROM
                join_term_data_with_covi j2
            WHERE
                j.contact_id = j2.contact_id
        ) AS first_covi_ay,
        j.contact_id,
        co_vitality_test_completed_date_c,
        raw_covi_score,
        raw_covi_score AS first_covi_score,
        student_site_c
    FROM
        gather_students_with_more_than_1_covi AS c
        LEFT JOIN join_term_data_with_covi AS j ON c.contact_id = j.contact_id
    WHERE
        AY_Name = 'AY 2020-21'
        AND sum_of_covi_tests_taken_ay > 1 #only students with 2 or more tests to assess growth
    GROUP BY
        contact_id,
        co_vitality_test_completed_date_c,
        raw_covi_score,
        student_site_c
),
gather_last_covi_test_date AS (
    SELECT
        (
            SELECT
                MAX(co_vitality_test_completed_date_c)
            FROM
                join_term_data_with_covi j2
            WHERE
                j.contact_id = j2.contact_id
        ) AS last_covi_ay,
        j.contact_id,
        co_vitality_test_completed_date_c,
        student_site_c,
        raw_covi_score AS last_covi_score,
        raw_covi_score
    FROM
        gather_students_with_more_than_1_covi AS c
        LEFT JOIN join_term_data_with_covi AS j ON c.contact_id = j.contact_id
    WHERE
        AY_Name = 'AY 2019-20'
        AND sum_of_covi_tests_taken_ay > 1 #only students with 2 or more tests to assess growth
    GROUP BY
        contact_id,
        co_vitality_test_completed_date_c,
        raw_covi_score,
        student_site_c
),
covi_score_first_test_ay AS (
    SELECT
        contact_id,
        student_site_c,
        PERCENTILE_CONT(first_covi_score,.5) OVER (PARTITION by student_site_c) AS prep_first_raw_covi_score_median_ay,
        #median
        first_covi_ay
    FROM
        gather_first_covi_test_date AS A
    WHERE
        co_vitality_test_completed_date_c = first_covi_ay #filter for first test date
        AND first_covi_score = (
            select
                MIN(A2.first_covi_score)
            FROM
                gather_first_covi_test_date AS A2
            where
                A.contact_id = A2.contact_id
        )
        AND raw_covi_score = first_covi_score --pull lowest CoVi score if student has more than 1 test on the same date
    GROUP BY
        contact_id,
        first_covi_score,
        first_covi_ay,
        student_site_c
),
covi_score_last_test_ay AS (
    SELECT
        contact_id,
        student_site_c,
        PERCENTILE_CONT(last_covi_score,.5) OVER (PARTITION by student_site_c) AS prep_last_raw_covi_score_median_ay,
        #median
        last_covi_ay
    FROM
        gather_last_covi_test_date AS A
    WHERE
        co_vitality_test_completed_date_c = last_covi_ay #filter for last test date
        AND last_covi_score = (
            select
                MIN(A2.last_covi_score)
            FROM
                gather_last_covi_test_date AS A2
            where
                A.contact_id = A2.contact_id
        )
        AND raw_covi_score = last_covi_score --pull lowest CoVi score if student has more than 1 test on the same date
    GROUP BY
        contact_id,
        last_covi_score,
        last_covi_ay,
        student_site_c
),
/*
 gather_casenotes_data AS (
 SELECT 
 )
 */
prep_median_growth_kpi AS (
    SELECT
        j.student_site_c,
        MAX(prep_first_raw_covi_score_median_ay) AS first_raw_covi_score_median_ay,
        MAX(prep_last_raw_covi_score_median_ay) AS last_raw_covi_score_median_ay
    FROM
        join_term_data_with_covi AS j
        LEFT JOIN covi_score_first_test_ay AS A ON j.student_site_c = A.student_site_c
        LEFT JOIN covi_score_last_test_ay AS B ON j.student_site_c = B.student_site_c
    GROUP BY
        j.student_site_c
),
prep_kpi AS (
    SELECT
        A.student_site_c,
        first_raw_covi_score_median_ay,
        last_raw_covi_score_median_ay,
        SUM(covi_assessment_completed_ay) AS wellness_students_completing_covi_ay,
        CASE
            WHEN last_raw_covi_score_median_ay > first_raw_covi_score_median_ay THEN 1
            ELSE 0
        END AS wellness_covi_median_growth
    FROM
        join_term_data_with_covi as A --LEFT JOIN gather_covi_data as C ON C.academic_semester_c = A.at_id
        LEFT JOIN covi_score_first_test_ay AS CF ON CF.contact_id = A.contact_id
        LEFT JOIN covi_score_last_test_ay AS CL ON CL.contact_id = A.contact_id
        LEFT JOIN prep_median_growth_kpi AS M ON M.student_site_c = A.student_site_c
    GROUP BY
        student_site_c,
        first_raw_covi_score_median_ay,
        last_raw_covi_score_median_ay,
        last_covi_ay,
        first_covi_ay
)
SELECT
    wellness_students_completing_covi_ay,
    wellness_covi_median_growth,
    first_raw_covi_score_median_ay,
    last_raw_covi_score_median_ay,
    student_site_c
FROM
    prep_kpi
GROUP BY
    wellness_students_completing_covi_ay,
    student_site_c,
    wellness_covi_median_growth,
    first_raw_covi_score_median_ay,
    last_raw_covi_score_median_ay
    /*
     --Commenting out subqueries - too much processing    
     gather_first_and_last_covi_test_date AS (
     SELECT 
     co_vitality_test_completed_date_c,
     raw_covi_score, 
     student_site_c,
     j.contact_id,
     (SELECT MIN(co_vitality_test_completed_date_c)
     FROM join_term_data_with_covi j2 
     WHERE j.contact_id = j2.contact_id
     ) AS first_covi_ay,
     
     (SELECT MAX(co_vitality_test_completed_date_c)
     FROM join_term_data_with_covi j2 
     WHERE j.contact_id = j2.contact_id
     ) AS last_covi_ay
     
     FROM gather_students_with_more_than_1_covi AS c
     LEFT JOIN join_term_data_with_covi AS j ON c.contact_id = j.contact_id
     WHERE AY_Name = 'AY 2019-20'
     AND sum_of_covi_tests_taken_ay > 1 #only students with 2 or more tests to assess growth
     GROUP BY
     student_site_c,
     raw_covi_score, 
     j.contact_id,
     co_vitality_test_completed_date_c
     ),*/