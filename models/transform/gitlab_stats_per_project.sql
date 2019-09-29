WITH projects AS (

  SELECT 
    project_id, 
    project_name,
    forks_count as total_forks,
    star_count as total_stars

  FROM {{ref('gitlab_projects')}}

),

issue_stats AS (

	SELECT
		project_id,
		milestone_id,
		count(distinct issue_id) as total_issues, 
		SUM(state_open_issues) as total_open_issues, 
		SUM(state_closed_issues) as total_closed_issues, 
		SUM(user_notes_count) as total_issue_comments

	FROM {{ref('gitlab_issues')}}

	GROUP BY project_id, milestone_id
),

merge_request_stats AS (

	SELECT
		project_id,
		milestone_id,
		count(distinct merge_request_id) as total_mrs, 
		SUM(state_open_mrs) as total_open_mrs, 
		SUM(state_closed_mrs) as total_closed_mrs, 
		SUM(state_merged_mrs) as total_merged_mrs, 
		SUM(user_notes_count) as total_mr_comments

	FROM {{ref('gitlab_merge_requests')}}

	GROUP BY project_id, milestone_id
),

milestones AS (

  SELECT milestone_id, title, start_date, due_date
  
  FROM {{ref('gitlab_milestones')}}

)

SELECT
    projects.project_id as project_id,
    projects.project_name as project_name,
    projects.total_forks as total_forks,
    projects.total_stars as total_stars,

    milestones.milestone_id as milestone_id,
    milestones.title as milestone_title,
    milestones.start_date as milestone_start_date,
    milestones.due_date as milestone_due_date,

    issue_stats.total_issues as total_issues,
    issue_stats.total_open_issues as total_open_issues,
    issue_stats.total_closed_issues as total_closed_issues,
    issue_stats.total_issue_comments as total_issue_comments,

    merge_request_stats.total_mrs as total_mrs,
    merge_request_stats.total_open_mrs as total_open_mrs,
    merge_request_stats.total_closed_mrs as total_closed_mrs,
    merge_request_stats.total_merged_mrs as total_merged_mrs,
    merge_request_stats.total_mr_comments as total_mr_comments

FROM projects
  LEFT JOIN issue_stats   
    ON issue_stats.project_id = projects.project_id

  LEFT JOIN merge_request_stats   
    ON merge_request_stats.project_id = projects.project_id
      AND (
      	issue_stats.milestone_id = merge_request_stats.milestone_id
      	OR (
     	    issue_stats.milestone_id is NULL
     	    AND merge_request_stats.milestone_id is NULL
      	)
      )

  LEFT JOIN milestones   
    ON issue_stats.milestone_id = milestones.milestone_id
  