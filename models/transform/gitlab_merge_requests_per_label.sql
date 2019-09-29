WITH labels AS (

     SELECT DISTINCT label_id, label_name
     FROM {{ref('gitlab_labels')}}

),

merge_requests AS (

     SELECT *
     FROM {{ref('gitlab_merge_requests')}}
)

-- Select each merge request once per label it has, together with the label
-- So, if an merge request has labels = ["Backlog", "Data Team", "Meltano"]
-- it will be selected 3 times, once per label
SELECT
    label_name,
    merge_requests.*
FROM merge_requests, labels
WHERE (merge_requests.labels ? labels.label_name)

UNION

-- Also add all merge requests without a label
SELECT
    'No Label',
    merge_requests.*
FROM merge_requests
WHERE jsonb_array_length(merge_requests.labels) = 0
