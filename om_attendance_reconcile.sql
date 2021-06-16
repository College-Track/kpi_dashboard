-- % of Incomplete Attendance Records addressed bi-weekly
-- This will be a unique query. It will only run on the 5th of each month and pull data from the previous month or earlier. It will just count the number of WSA records that are still scheduled.
SELECT
  site_short,
  Ethnic_background_c,
  Gender_c,
  COUNT(Class_Attendance_Id) AS OM_incomplete_wsa_records
FROM
  `data-warehouse-289815.salesforce_clean.class_template` CT
  LEFT JOIN `data-warehouse-289815.salesforce_clean.contact_at_template` CAT ON CAT.AT_Id = CT.Academic_Semester_c
WHERE
  Outcome_c = 'Scheduled'
  AND (
    EXTRACT(
      MONTH
      FROM
        date_c
    ) < EXTRACT(
      MONTH
      FROM
        CURRENT_DATE()
    )
  )
  AND current_as_c = true
  AND is_deleted = false
GROUP BY
  site_short,
    Ethnic_background_c,
    Gender_c