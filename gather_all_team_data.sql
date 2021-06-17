WITH join_prep AS (
  SELECT
    *
  FROM
    `data-studio-260217.kpi_dashboard.join_prep`
),
join_team_kpis AS (
  SELECT
    JP.*,
    AA.* EXCEPT(site_short, Ethnic_background_c, Gender_c),
    SD.* EXCEPT(site_short, Ethnic_background_c, Gender_c),
    SL.* EXCEPT(site_short, Ethnic_background_c, Gender_c),
    CC_HS.* EXCEPT(site_short,Ethnic_background_c, Gender_c),
    CC_PS.* EXCEPT(site_short, Ethnic_background_c, Gender_c),
    OM.* EXCEPT(site_short, Ethnic_background_c, Gender_c),
    RED.* EXCEPT(site_short, Ethnic_background_c, Gender_c),
    PRO_OPS.* EXCEPT(site_short, Ethnic_background_c, Gender_c),
    OM_ATTEND.* EXCEPT(site_short, Ethnic_background_c, Gender_c),
    WLLNSS.* EXCEPT(site_short, Ethnic_background_c, Gender_c),
    FP.* EXCEPT(site_short, Ethnic_background_c, Gender_c)
  FROM
    join_prep JP
    LEFT JOIN `data-studio-260217.kpi_dashboard.academic_affairs` AA ON AA.site_short = JP.site_short AND AA.Ethnic_background_c = JP.Ethnic_background_c AND AA.Gender_c = JP.Gender_c
    LEFT JOIN `data-studio-260217.kpi_dashboard.site_directors` SD ON SD.site_short = JP.site_short AND SD.Ethnic_background_c = JP.Ethnic_background_c AND SD.Gender_c = JP.Gender_c
    LEFT JOIN `data-studio-260217.kpi_dashboard.cc_hs` CC_HS ON CC_HS.site_short = JP.site_short AND CC_HS.Ethnic_background_c = JP.Ethnic_background_c AND CC_HS.Gender_c = JP.Gender_c
    LEFT JOIN `data-studio-260217.kpi_dashboard.cc_ps` CC_PS ON CC_PS.site_short = JP.site_short AND CC_PS.Ethnic_background_c = JP.Ethnic_background_c AND CC_PS.Gender_c = JP.Gender_c
    LEFT JOIN `data-studio-260217.kpi_dashboard.student_life` SL ON SL.site_short = JP.site_short AND SL.Ethnic_background_c = JP.Ethnic_background_c AND SL.Gender_c = JP.Gender_c
    LEFT JOIN `data-studio-260217.kpi_dashboard.om` OM ON OM.site_short = JP.site_short AND OM.Ethnic_background_c = JP.Ethnic_background_c AND OM.Gender_c = JP.Gender_c
    LEFT JOIN `data-studio-260217.kpi_dashboard.red`RED ON RED.site_short = JP.site_short AND RED.Ethnic_background_c = JP.Ethnic_background_c AND RED.Gender_c = JP.Gender_c
    LEFT JOIN `data-studio-260217.kpi_dashboard.program_ops` PRO_OPS ON PRO_OPS.site_short = JP.site_short AND PRO_OPS.Ethnic_background_c = JP.Ethnic_background_c AND PRO_OPS.Gender_c = JP.Gender_c
    LEFT JOIN `data-studio-260217.kpi_dashboard.om_incomplete_attendance` OM_ATTEND ON OM_ATTEND.site_short = JP.site_short AND OM_ATTEND.Ethnic_background_c = JP.Ethnic_background_c AND OM_ATTEND.Gender_c = JP.Gender_c
    LEFT JOIN `data-studio-260217.kpi_dashboard.wellness` WLLNSS ON WLLNSS.site_short = JP.site_short AND WLLNSS.Ethnic_background_c = JP.Ethnic_background_c AND WLLNSS.Gender_c = JP.Gender_c
    LEFT JOIN `data-studio-260217.kpi_dashboard.fp` FP ON FP.site_short = JP.site_short AND FP.Ethnic_background_c = JP.Ethnic_background_c AND FP.Gender_c = JP.Gender_c

)
SELECT
  *
FROM
  join_team_kpis