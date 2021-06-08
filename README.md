# KPI Dashboard

## Background

In FY21 College Track started the planning process to implement a Key Performance Indicator (KPI) process for both internal evaluation and individual performance evaluations. 

Starting in the Summer of 2020, and continuing through Spring 2021, each College Track team identified a collection of KPIs that would be valuable in understanding their work and impact on the organization. During this period, each KPI was approved by the appropriate manager as meaningful and important. 

As part of a separate process, teams also identified which KPIs they would like to be held accountable for as part of a performance evaluation process. While these processes are related, the documentation here only reflects the KPIs process broadly and not the related performance management process. 

A list of all the KPIs select for each team can be found [here](https://docs.google.com/spreadsheets/d/1JGYAzdcO0MAmLKjSF9pevfQRqtWs8Z4QEu2j1IcAda8/edit#gid=1648503645).

Using this list of KPIs, the Organization Performance (OP) team was tasked with building out a Data Studio [dashboard](https://datastudio.google.com/reporting/9597052e-72d8-41dc-b1b1-8c7a9410ec06) that contained the most accurate and up-to-date data for each KPI. For the first iteration of this dashboard, OP focused on providing Program-related KPIs as this data was most readily accessible. In addition, of the Program KPIs, not all of them were possible to build immediately. These have been identified and a plan to track this data is being established for FY22. 

OP maintains a separate document that tracks the build status of each KPI and any internal notes relevant to the KPI from a technical standpoint. That document is [here](https://docs.google.com/spreadsheets/d/1zpU8cNqVBbxyPaXr_KB4qjyzPY6pa52p8mCxz2IwJ0Q/edit#gid=992211052)

## Technical Background

### Process Overview

All data used for the KPI dashboard is processed in Google Big Query. Data comes into Big Query from multiple sources, with the vast majority coming from Salesforce (via Fivetran). 

Like most of OP's Big Query projects, the raw data is stored in the Big Query project `data-warehouse-289815`. While all modified data used for the KPI calculations and dashboard are in the project `data-studio-260217` under the dataset `kpi_dashboard`. 

Each distinct team that identified KPIs has their own table in the `kpi_dashboard` dataset. The team name is abbreviated for simplicity. For example, the Regional Executive Director team's KPIs are housed in the table `red`. 

Contained in this repository is an analogous .sql file that contains the SQL code used to generate each table. Each SQL file is titled with the name of the table the code generates.

All of the program KPIs are formatted such that they "roll-up" from a site to regional to national level. That is to say, we must create a KPI metric for Ward 8 and The Durant Center that rolls up in the DC region and then, combined with all other regions, can roll up to a National level.

Most KPIs are represented by a percent. For example, percent of 9th grade students who are male. To represent this data correctly in Data Studio, we create a numerator and denominator metric for the KPI for each site. For example the data might be displayed like this:


| site_short | ninth_grade_males_numerator | ninth_grade_males_denominator | 
| --- | --- | --- |
| Ward 8 | 30 | 60 |
| The Durant Center | 25 | 75 |

In Data Studio, we then can create a calculated field with the following syntax:

```sql
SUM(ninth_grade_males_numerator) / SUM(ninth_grade_males_denominator)
```

The resulting field will allow us to display a percent that is accurate for Ward 8 (50%) and also roll up correctly to create a percent for the DC region (41%).

Many KPIs have a common denominator. For example, all HS students or all PS students. In order to streamline this process, if a denominator is shared across multiple KPIs, it is created in the query `join_prep`.  

Finally, all the team KPIs in conjunction with the `join_prep` table are merged together to form one large table housing all the KPIs for all teams. This table is `team_kpi_table` and it is created in the query `gather_all_team_data.sql`


The majority of these queries are run every night in order to display the most up-to-date data in the KPI Dashboard. 


### Internal Tracker

OP internally manages the KPI process through a [spreadsheet](https://docs.google.com/spreadsheets/d/1zpU8cNqVBbxyPaXr_KB4qjyzPY6pa52p8mCxz2IwJ0Q/edit#gid=961255889) containing all the possible KPIs.

Each KPI is identified and assigned an Id based on it's program area and objective. 

The format for KPI IDs is <program_area_abbreviation>_<objective_number>_<kpi_number>

For example, `AA_2_3` would be the third KPI from the second objective in the academic affairs team. The ordering of objectives and KPIs is meaningless besides a helpful way to identify a KPI. If a new KPI is added, it can simply be appended to the end of the numbering system. 

Each KPI is assigned an owner, who is responsible for building and maintaining the KPI. Generally speaking, a single person "owns" all the KPIs for a given program area, but in some instances, there are multiple owners for a program area if it is appropriate. 

The most important column is the Status column which identifies the current status for each KPI. 

The choices for this column are as follows:

* Not Currently Possible
    * It isn't possible to build this KPI given the current data or infrastructure available. 
* Requires External Data
    * This KPI requires external data that isn't readily accessible to OP. This likely means the data needs to be acquired from another team and then imported into Big Query.
* Needs Clarification
    * This KPI requires additional clarification from the relevant team (or OP manager) before it can be built.
* In Development
    * This KPI is currently being built by the identified OP owner.
* Changes Pending
    * The KPI is built and is currently being reviewed by other OP team members to ensure accuracy. Once this is complete, it can be put into production.
* In Dashboard
     * The KPI is built and currently live in the dashboard. Nothing more needs to be done.

It is the responsibility of the assigned OP owner to update the status of each KPI. The OP team member leading the KPI process will periodically review the statuses and discuss any concerns or thoughts when appropriate. 

### Naming / Style Conventions

Within each query, the start of each KPI should be identified with a SQL comment containing the full text of the KPI. If there the KPI has a known deficiency, or manual update required at certain points in time (for example when a term switches over) that should also be identified at the top in the comment. 

The column containing the numerator for each KPI (or just the number field if there is no related denominator) should be prefixed with the abbreviation for the program area the KPI relates to. Following this abbreviation, the field name should be a short description of the KPI. 

For example, `aa_above_325_gpa` is the academic affairs KPI relating to students who are above a 3.25 GPA. 

If a KPI isn't using a denominator from `join_prep` then the relevant denominator field should have `_denom` appended at the end. 

In Data Studio, each KPI that is a percent will have to have a calculated field created in the format described above. This field should be titled the same as the numerator field, but with `_percent` appended at the end. 



### Notable Exceptions to Process

* om_attendance_reconcile
    * This table is created for a single KPI (# of Incomplete Attendance Records) that is only updated once a month (on the 5th of the month). Otherwise, it follows the same general process as outlined above.



### KPI Update Process

When OP creates or updates a KPI we follow a process to ensure other OP team members have signed off and verified the KPI's accuracy. 

The Github repo for this project contains two constant branches: master and devleop. 

When a change is made, a branch will be created from develop and the changes will be committed to this new branch. A pull request will be created to merge these changes back into develop. 

The OP team will review this pull request, comment on any concerns, and then ultimately approve - or reject - the change. If approved, the pull request will be made first to the develop branch and then a second pull request made to merge data from develop to master. 

Once the code is in the master branch, the scheduled query will be updated in Big Query and, if required, the KPI Dashboard will be updated with the new field(s).



