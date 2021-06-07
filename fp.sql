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
get_at_data AS (
    SELECT
        site_short AS at_site,
        --% of college students saying that they know how to apply for Emergency Fund in PAT advising rubric
        SUM(
            CASE
                WHEN e_fund_c = 'EF_G' THEN 1
                ELSE 0
            END
        ) AS indicator_efund,
    FROM
        `data-warehouse-289815.salesforce_clean.contact_at_template`
    WHERE
        college_track_status_c = '15A'
        AND(
            (
                CURRENT_DATE() < '2021-07-01'
                AND current_as_c = TRUE
            )
            OR (
                CURRENT_DATE() > '2021-07-01'
                AND previous_as_c = TRUE
            )
        )
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
        gather_survey_data.ps_survey_scholarship_num,
        get_at_data.indicator_efund AS fp_efund_num,
    FROM
        gather_contact_data
        LEFT JOIN gather_survey_data ON gather_survey_data.survey_site_short = site_short
        LEFT JOIN get_at_data ON get_at_data.at_site = site_short
)
SELECT
    *
FROM
    join_data