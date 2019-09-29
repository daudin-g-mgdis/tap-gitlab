WITH labels AS (

	SELECT DISTINCT label_id, label_name
	FROM {{ref('gitlab_labels')}}

),

issue_stats AS (

	SELECT
		label_name, 
		project_id,
		milestone_id,
		count(distinct issue_id) as total_issues, 
		SUM(state_open_issues) as total_open_issues, 
		SUM(state_closed_issues) as total_closed_issues, 
		SUM(user_notes_count) as total_issue_comments

	FROM {{ref('gitlab_issues_per_label')}}

	GROUP BY label_name, project_id, milestone_id
),

merge_request_stats AS (

	SELECT
		label_name, 
		project_id,
		milestone_id,
		count(distinct merge_request_id) as total_mrs, 
		SUM(state_open_mrs) as total_open_mrs, 
		SUM(state_closed_mrs) as total_closed_mrs, 
		SUM(state_merged_mrs) as total_merged_mrs, 
		SUM(user_notes_count) as total_mr_comments

	FROM {{ref('gitlab_merge_requests_per_label')}}

	GROUP BY label_name, project_id, milestone_id
),

project AS (

	SELECT project_id, project_name
	
	FROM {{ref('gitlab_projects')}}

),

milestone AS (

	SELECT milestone_id, title, start_date, due_date
	
	FROM {{ref('gitlab_milestones')}}

)

SELECT
    labels.label_id as label_id,
    labels.label_name as label_name,

    project.project_id as project_id,
    project.project_name as project_name,

    milestone.milestone_id as milestone_id,
    milestone.title as milestone_title,
    milestone.start_date as milestone_start_date,
    milestone.due_date as milestone_due_date,

    issue_stats.total_issues as total_issues,
    issue_stats.total_open_issues as total_open_issues,
    issue_stats.total_closed_issues as total_closed_issues,
    issue_stats.total_issue_comments as total_issue_comments,

    merge_request_stats.total_mrs as total_mrs,
    merge_request_stats.total_open_mrs as total_open_mrs,
    merge_request_stats.total_closed_mrs as total_closed_mrs,
    merge_request_stats.total_merged_mrs as total_merged_mrs,
    merge_request_stats.total_mr_comments as total_mr_comments

FROM labels
  LEFT JOIN issue_stats   
    ON labels.label_name = issue_stats.label_name

  LEFT JOIN project   
    ON issue_stats.project_id = project.project_id

  LEFT JOIN merge_request_stats   
    ON issue_stats.label_name = merge_request_stats.label_name
      AND issue_stats.project_id = merge_request_stats.project_id
      AND (
      	issue_stats.milestone_id = merge_request_stats.milestone_id
      	OR (
     	  issue_stats.milestone_id is NULL
     	  AND merge_request_stats.milestone_id is NULL
      	)
      )

  LEFT JOIN milestone   
    ON issue_stats.milestone_id = milestone.milestone_id
  