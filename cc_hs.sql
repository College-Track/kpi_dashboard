WITH gather_contact_data AS (
SELECT 
    contact_id,
    site_short,
    Ethnic_background_c,
    Gender_c

FROM `data-warehouse-289815.salesforce_clean.contact_template` AS c 
),


gather_11th_aspiration_data AS ( 
    SELECT
        contact_id,
        site_short,
            
     --11th Grade Aspirations, any Aspiration
        SUM(CASE 
            WHEN a.id IS NOT NULL 
            THEN 1
            ELSE 0
            END) AS aspirations_any_count,
            
    --11th Grade Aspirations, Affordable colleges
        SUM(CASE
            WHEN fit_type_current_c IN ("Best Fit","Good Fit","Local Affordable") 
            THEN 1
            ELSE 0
            END) AS aspirations_affordable_count,
            
    --11th Grade Aspirations reporting group        
        MAX(CASE 
            WHEN (c.grade_c = '11th Grade'
            AND college_track_status_c = '11A') THEN 1
            ELSE 0
            END) AS aspirations_denom_count,
            
        c.Ethnic_background_c,
        c.Gender_c
            
    FROM `data-warehouse-289815.salesforce_clean.contact_template` AS c
    LEFT JOIN`data-warehouse-289815.salesforce.college_aspiration_c` a ON c.contact_id=a.student_c
    WHERE college_track_status_c = '11A'
    AND c. grade_c = '11th Grade'
    GROUP BY
        contact_id,
        site_short,
        Ethnic_background_c,
        Gender_c
),

--CT students for 10th grade EFC KPI
gather_10th_efc_data AS (
SELECT
        contact_id,
        site_short,
        
         --10th Grade EFC, FAFSA4Caster only
        CASE
            WHEN (c.grade_c = "10th Grade" 
            AND FA_Req_Expected_Financial_Contribution_c IS NOT NULL 
            AND fa_req_efc_source_c = 'FAFSA4caster') THEN 1
            ELSE 0
            END AS hs_EFC_10th_count,
            
        CASE
            WHEN (c.grade_c = "10th Grade" 
            AND college_track_status_c = '11A') THEN 1
            ELSE 0
            END AS hs_EFC_10th_denom_count,
            
        Ethnic_background_c,
        Gender_c

FROM `data-warehouse-289815.salesforce_clean.contact_template` AS c
WHERE college_track_status_c = '11A'
    AND grade_c = '10th Grade'
),

--for 12th grade KPI: 80% CC attendance
gather_attendance_data AS ( 
    SELECT 
        c.student_c, 
        CASE
            WHEN SUM(Attendance_Denominator_c) = 0 THEN NULL
            ELSE SUM(Attendance_Numerator_c) / SUM(Attendance_Denominator_c)
            END AS attendance_rate
    FROM `data-warehouse-289815.salesforce_clean.class_template` AS c
        LEFT JOIN `data-warehouse-289815.salesforce_clean.contact_at_template` CAT 
        ON CAT.global_academic_semester_c = c.global_academic_semester_c
    WHERE Department_c = "College Completion"
    AND Cancelled_c = FALSE
    AND (workshop_display_name_c LIKE '%Junior Advisory%'
        OR workshop_display_name_c LIKE '%Senior Advisory%'
        OR workshop_display_name_c LIKE '%Senior Seminar%'
        OR workshop_display_name_c LIKE '%College Exposure%')
    AND CAT.AY_Name = 'AY 2020-21'
    AND college_track_status_c = '11A'
    AND (grade_c = "12th Grade" OR (grade_c='Year 1' AND indicator_years_since_hs_graduation_c = 0))
    GROUP BY 
        c.student_c
),

--Affordable college KPIs: Applying and Acceptance to Affordable Colleges and/or Best Fit/Good Fit schools only
gather_data_twelfth_grade AS (
    SELECT 
        CT.contact_id,
        attendance_rate,
        CT.site_short,
        
    --% students completing the financial aid submission and verification processes
        --May need to confirm which college applications should be included (e.g. accepted and enrolled/deferred). Currently pulling any college app
        (SELECT student_c
        FROM `data-warehouse-289815.salesforce_clean.college_application_clean`AS subq1
        WHERE FA_Req_FAFSA_c = 'Submitted' 
        AND (verification_status_c IN ('Submitted','Not Applicable')) 
        AND Contact_Id=student_c
        group by student_c
        ) AS gather_fafsa_verification,
    
    --% of acceptances to Affordable colleges. This will be done in 2 parts since this can live in 2 fields: Fit Type (Applied), Fit Type (Enrolled)
        --Pulling in students that were accepted to an affordable option. 
        --Accepted & Enrolled/Deferred Affordable options will also be pulled in, but in another query below since these will live in Fit Type (enrolled) field
        (SELECT student_c
        FROM `data-warehouse-289815.salesforce_clean.college_application_clean`AS subq1
        WHERE (admission_status_c = "Accepted" AND College_Fit_Type_Applied_c IN ("Best Fit","Good Fit","Local Affordable"))
        AND Contact_Id=student_c
        group by student_c
        ) AS applied_accepted_affordable,
        
     --% of acceptances to Affordable colleges; % hs seniors who matriculate to affordable college
        --Pulling in from Fit Type (enrolled)
        --Will also be used to project matriculation to affordable colleges
        (SELECT student_c
        FROM `data-warehouse-289815.salesforce_clean.college_application_clean`AS subq2
        WHERE admission_status_c IN ("Accepted and Enrolled", "Accepted and Deferred") AND fit_type_enrolled_c IN ("Best Fit","Good Fit","Local Affordable","Situational")
        AND Contact_Id=student_c
        group by student_c
        ) AS accepted_enrolled_affordable,
    
    --% accepted to Best Fit, Good Fit
        --Same logic as the 2 queries above, except only looking at Good Fit and Best Fit in Fit Type (Applied)     
        (SELECT student_c
        FROM `data-warehouse-289815.salesforce_clean.college_application_clean`AS subq4
        WHERE admission_status_c = "Accepted" AND College_Fit_Type_Applied_c IN ("Best Fit","Good Fit")
        AND Contact_Id=student_c
        group by student_c
        ) AS applied_accepted_best_good,
    
    --% accepted to Best Fit, Good Fit; % hs seniors who matriculate to Good/Best/Situational
        --Same logic as the 2 queries above, except only looking at Good Fit and Best Fit in Fit Type (Enrolled) 
    -- Will also be used to project matriculation
        (SELECT student_c
        FROM `data-warehouse-289815.salesforce_clean.college_application_clean`AS subq5
        WHERE admission_status_c IN ("Accepted and Enrolled", "Accepted and Deferred") AND fit_type_enrolled_c IN ("Best Fit","Good Fit","Situational")
        AND Contact_Id=student_c
        group by student_c
        ) AS accepted_enrolled_best_good_situational,
    
    --% applying to Best Fit and Good Fit colleges
        (SELECT student_c
        FROM `data-warehouse-289815.salesforce_clean.college_application_clean` AS subq3
        WHERE College_Fit_Type_Applied_c IN ("Best Fit","Good Fit")  
        AND Contact_Id=student_c
        group by student_c
        ) AS applied_best_good,
        
    --Students and families are prepared to enter strong college situations
    --The following assumes that when you accept and enroll, are waitlisted, or deferred for FY22,
    --you have had a conversation with a parent and student, which is part of dosage. 
        (SELECT student_c
        FROM `data-warehouse-289815.salesforce_clean.college_application_clean`AS subq2
        WHERE admission_status_c IN ("Accepted and Enrolled", "Accepted and Deferred","Wait-listed")
        AND Contact_Id=student_c
        group by student_c
        ) AS accepted_enrolled_waitlisted,
        
        CT.Ethnic_background_c,
        CT.Gender_c
            
      
    FROM `data-warehouse-289815.salesforce_clean.contact_template` AS CT
        LEFT JOIN gather_attendance_data ON CT.contact_id=student_c
    WHERE  college_track_status_c = '11A'
    AND (grade_c = "12th Grade" OR (grade_c='Year 1' AND indicator_years_since_hs_graduation_c = 0))
),

--Prepping 11th grade College Aspirations for aggregation
gather_eleventh_grade_metrics AS ( 
 SELECT
    site_short,
    aspirations_denom_count,
    CASE 
        WHEN (aspirations_any_count >= 6 AND aspirations_affordable_count >= 3) 
        THEN 1
        ELSE 0
        END AS cc_hs_aspirations_num_prep,
    Ethnic_background_c,
    Gender_c
    
    FROM gather_11th_aspiration_data
    GROUP BY contact_id,site_short, aspirations_denom_count,aspirations_any_count,aspirations_affordable_count,Ethnic_background_c,Gender_c
),

--Prepping attendance data, FAFSA Verification KPI, Affordable college data for aggregation
gather_twelfth_grade_metrics AS(
    SELECT  
        site_short,
        CASE 
            WHEN attendance_rate >= 0.8 THEN 1
            ELSE 0
            END AS cc_hs_above_80_cc_attendance,
        CASE
            WHEN gather_fafsa_verification IS NOT NULL THEN 1
            ELSE 0
            END AS fafsa_verification_prep,
        CASE 
            WHEN accepted_enrolled_affordable IS NOT NULL THEN 1
            WHEN applied_accepted_affordable IS NOT NULL THEN 1
            ELSE 0
            END AS cc_hs_accepted_affordable,
        CASE 
            WHEN applied_best_good IS NOT NULL THEN 1
            ELSE 0
            END AS cc_hs_applied_best_good,
        CASE 
            WHEN accepted_enrolled_best_good_situational IS NOT NULL THEN 1
            WHEN applied_accepted_best_good IS NOT NULL THEN 1
            ELSE 0
            END AS cc_hs_accepted_best_good_situational,
            
    --Matriculation data not available yet, so projecting matriculation based on College Application enrollment data
        CASE 
            WHEN accepted_enrolled_best_good_situational IS NOT NULL THEN 1
            ELSE 0
            END AS cc_hs_enrolled_best_good_situational,  
            
    --Matriculation data not available yet, so projecting matriculation based on College Application enrollment data
        CASE 
            WHEN accepted_enrolled_affordable IS NOT NULL THEN 1
            ELSE 0
            END AS cc_hs_enrolled_affordable, 
            
    --% of 12th graders and families participating in 1+ college choice conversation
    --Students and families are prepared to enter strong college situations
        CASE 
            WHEN accepted_enrolled_waitlisted IS NOT NULL THEN 1
            ELSE 0
            END AS cc_hs_strong_college_situations,
            
    Ethnic_background_c,
    Gender_c
            
    FROM gather_data_twelfth_grade
),


--Aggregating 10th Grade EFC KPI
prep_tenth_grade_metrics AS ( 
    SELECT 
        site_short,
        SUM(hs_EFC_10th_count) AS cc_hs_EFC_10th_num,
        SUM(hs_EFC_10th_denom_count) AS cc_hs_EFC_10th_denom,
        Ethnic_background_c,
        Gender_c
    FROM gather_10th_efc_data
    GROUP BY 
        site_short,
        Ethnic_background_c,
        Gender_c
),

--Aggregating 11th grade College Aspirations KPI
prep_eleventh_grade_metrics AS ( 
    SELECT 
        site_short,
        SUM(cc_hs_aspirations_num_prep) AS cc_hs_aspirations_num,
        SUM(aspirations_denom_count) AS cc_hs_aspirations_denom,
        Ethnic_background_c,
        Gender_c
    FROM gather_eleventh_grade_metrics
    GROUP BY 
        site_short,
        Ethnic_background_c,
        Gender_c
),
--Aggregating 12th grade KPIs: CC Attendance 80%, Affordable Colleges, Best Fit/Good Fit colleges, FAFSA verification
prep_twelfth_grade_metrics AS (
    SELECT 
        site_short,
        SUM(cc_hs_above_80_cc_attendance) AS cc_hs_above_80_cc_attendance,
        SUM(cc_hs_accepted_affordable) AS cc_hs_accepted_affordable,
        SUM(cc_hs_applied_best_good) AS cc_hs_applied_best_good,
        SUM(cc_hs_accepted_best_good_situational) AS cc_hs_accepted_best_good_situational,
        SUM(cc_hs_strong_college_situations) AS cc_hs_strong_college_situations,
        SUM(fafsa_verification_prep) AS cc_hs_financial_aid_submission_verification,
        SUM(cc_hs_enrolled_best_good_situational) AS cc_hs_enrolled_best_good_situational,
        SUM(cc_hs_enrolled_affordable) AS cc_hs_enrolled_affordable,
        Ethnic_background_c,
        Gender_c
    FROM gather_twelfth_grade_metrics
    GROUP BY 
        site_short,
        Ethnic_background_c,
        Gender_c
)

#final kpi join
SELECT 
    gd.site_short,
    kpi_10th.* EXCEPT(site_short,Ethnic_background_c,Gender_c),
    kpi_11th.* EXCEPT(site_short,Ethnic_background_c,Gender_c),
    kpi_12th.* EXCEPT(site_short,Ethnic_background_c,Gender_c),
    gd.Ethnic_background_c,
    gd.Gender_c
    FROM gather_contact_data as gd
        LEFT JOIN prep_tenth_grade_metrics AS kpi_10th ON gd.site_short = kpi_10th.site_short AND gd.Ethnic_background_c=kpi_10th.Ethnic_background_c AND gd.gender_c=kpi_10th.Gender_c
        LEFT JOIN prep_eleventh_grade_metrics AS kpi_11th ON gd.site_short = kpi_11th.site_short AND gd.Ethnic_background_c=kpi_11th.Ethnic_background_c AND gd.Gender_c=kpi_11th.Gender_c
        LEFT JOIN prep_twelfth_grade_metrics AS kpi_12th ON gd.site_short = kpi_12th.site_short AND gd.Ethnic_background_c=kpi_12th.Ethnic_background_c AND gd.Gender_c=kpi_12th.Gender_c
GROUP BY
    site_short, 
    cc_hs_EFC_10th_num,
    cc_hs_EFC_10th_denom,
    cc_hs_aspirations_num,
    cc_hs_aspirations_denom,
    cc_hs_above_80_cc_attendance,
    cc_hs_strong_college_situations,
    cc_hs_financial_aid_submission_verification,
    cc_hs_accepted_affordable,
    cc_hs_applied_best_good,
    cc_hs_accepted_best_good_situational,
    cc_hs_enrolled_best_good_situational, #projecting matriculation
    cc_hs_enrolled_affordable, #projecting matriculation
    Ethnic_background_c,
    Gender_c