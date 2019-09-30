WITH group_labels AS (

     SELECT *
     FROM {{ref('gitlab_group_labels')}}

),

project_labels AS (

     SELECT *
     FROM {{ref('gitlab_project_labels')}}

)

SELECT
    label_id,
    NULL as group_id,
    project_id,

    label_name,
    description,

    open_issues_count,
    open_merge_requests_count,
    closed_issues_count,

    is_project_label,
    priority,

    color,
    text_color,
    subscribed

FROM project_labels

WHERE is_project_label = true

UNION

SELECT
    label_id,
    group_id,
    NULL as project_id,

    label_name,
    description,

    open_issues_count,
    open_merge_requests_count,
    closed_issues_count,

    false as is_project_label,
    NULL as priority,

    color,
    text_color,
    subscribed

FROM group_labels
