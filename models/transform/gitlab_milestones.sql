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
    due_date,
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
    due_date,
    created_at,
    updated_at

FROM group_milestones
