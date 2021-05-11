WITH gather_survey_data AS (
  SELECT
    CT.site_short,
    S.contact_id,
    -- % of students that agree or strongly agree to 'my site is run effectively'
    -- The denominator for this metric is housed in the join_prep query.
    CASE
      WHEN my_site_is_run_effectively_examples_i_know_how_to_find_zoom_links_i_receive_site = "Strongly Agree" THEN 1
      WHEN my_site_is_run_effectively_examples_i_know_how_to_find_zoom_links_i_receive_site = 'Agree' THEN 1
      ELSE 0
    END AS agree_site_is_run_effectively
  FROM
    `data-studio-260217.surveys.fy21_hs_survey` S
    LEFT JOIN `data-warehouse-289815.salesforce_clean.contact_template` CT ON CT.Contact_Id = S.contact_id
 
)


SELECT
  GSD.site_short,
  SUM(GSD.agree_site_is_run_effectively) AS OM_agree_site_is_run_effectively
from
  gather_survey_data GSD
GROUP BY
  GSD.site_short
