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
    EXTRACT(YEAR FROM start_date) as start_date_year,
    EXTRACT(MONTH FROM start_date) as start_date_month,
    EXTRACT(DAY FROM start_date) as start_date_day,

    due_date,
    EXTRACT(YEAR FROM due_date) as due_date_year,
    EXTRACT(MONTH FROM due_date) as due_date_month,
    EXTRACT(DAY FROM due_date) as due_date_day,

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
    EXTRACT(YEAR FROM start_date) as start_date_year,
    EXTRACT(MONTH FROM start_date) as start_date_month,
    EXTRACT(DAY FROM start_date) as start_date_day,

    due_date,
    EXTRACT(YEAR FROM due_date) as due_date_year,
    EXTRACT(MONTH FROM due_date) as due_date_month,
    EXTRACT(DAY FROM due_date) as due_date_day,
    
    created_at,
    updated_at

FROM group_milestones
