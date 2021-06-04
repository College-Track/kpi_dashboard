WITH gather_survey_data AS (
    SELECT
        site_short AS survey_site_short,
        SUM(
            CASE
                WHEN contact_id IS NOT NULL THEN 1
                ELSE 0
            END
        ) AS ps_survey_scholarship_denom,
        SUM(
            CASE
                WHEN i_am_able_to_receive_my_scholarship_funds_from_college_track IN ('StronglyAgree', 'Strongly Agree', 'Agree') THEN 1
                ELSE 0
            END
        ) AS ps_survey_scholarship_num
    FROM
        `data-studio-260217.surveys.fy21_ps_survey_wide_prepped`
    WHERE
        i_am_able_to_receive_my_scholarship_funds_from_college_track IS NOT NULL
    GROUP BY
        site_short
),
gather_contact_data AS (
    SELECT
        site_short,
        SUM(
            CASE
                WHEN fa_req_fafsa_c = 'Submitted' THEN 1
                ELSE 0
            END
        ) AS fp_12_fafsa_complete_num
    FROM
        `data-warehouse-289815.salesforce_clean.contact_template`
    WHERE
        college_track_status_c = '11A'
        AND (
            grade_c = "12th Grade"
            OR (
                grade_c = 'Year 1'
                AND indicator_years_since_hs_graduation_c = 0
            )
        )
    GROUP BY
        site_short
),
join_data AS (
    SELECT
        site_short,
        fp_12_fafsa_complete_num AS fp_12_fasfa_num,
        gather_survey_data.ps_survey_scholarship_denom,
        gather_survey_data.ps_survey_scholarship_num
    FROM
        gather_contact_data
        LEFT JOIN gather_survey_data ON gather_survey_data.survey_site_short = site_short
)
SELECT
    *
FROM
    join_data