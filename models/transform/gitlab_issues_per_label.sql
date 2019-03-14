WITH labels AS (

     SELECT DISTINCT label_id, label_name
     FROM {{ref('gitlab_project_labels')}}

),

issues AS (

     SELECT *
     FROM {{ref('gitlab_issues')}}
)

-- Select each issue once per label it has, together with the label
-- So, if an issue has labels = ["Backlog", "Data Team", "Meltano"]
-- it will be selected 3 times, once per label
SELECT
    label_name,
    issues.*
FROM issues, labels
WHERE (issues.labels ? labels.label_name)

UNION

-- Also add all issues without a label
SELECT
    'No Label',
    issues.*
FROM issues
WHERE jsonb_array_length(issues.labels) = 0
