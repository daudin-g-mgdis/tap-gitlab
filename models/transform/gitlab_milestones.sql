WITH group_milestones AS (

     SELECT *
     FROM {{ref('gitlab_group_milestones')}}

),

project_milestones AS (

     SELECT *
     FROM {{ref('gitlab_project_milestones')}}

)

SELECT
    milestone_id,

    'project_milestone' as milestone_type,
    NULL as group_id,
    project_id,
    iid,

    title,
    description,
    state,

    start_date,
    substring(start_date from 1 for 4) as start_date_year,
    substring(start_date from 6 for 2) as start_date_month,
    substring(start_date from 9 for 2) as start_date_day,    

    due_date,
    substring(due_date from 1 for 4) as due_date_year,
    substring(due_date from 6 for 2) as due_date_month,
    substring(due_date from 9 for 2) as due_date_day, 

    created_at,
    updated_at

FROM project_milestones


UNION

SELECT
    milestone_id,

    'group_milestone' as milestone_type,
    group_id,
    NULL as project_id,
    iid,

    title,
    description,
    state,

    start_date,
    substring(start_date from 1 for 4) as start_date_year,
    substring(start_date from 6 for 2) as start_date_month,
    substring(start_date from 9 for 2) as start_date_day,    

    due_date,
    substring(due_date from 1 for 4) as due_date_year,
    substring(due_date from 6 for 2) as due_date_month,
    substring(due_date from 9 for 2) as due_date_day, 
    
    created_at,
    updated_at

FROM group_milestones
