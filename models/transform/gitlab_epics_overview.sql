-- Conditionally generate this model only for Gitlab Ultimate accounts
{{
  config(
    enabled=var('ultimate_license')|lower in ["true", "1", "yes", "on"]
  )
}}

WITH children_per_epic AS (

     SELECT 

     	parent_id,
     	COUNT(*) as total_children 
     
     FROM {{ref('gitlab_epics')}}

     WHERE parent_id is NOT NULL

     GROUP BY parent_id

),

epics AS (

	SELECT 

		epics.*,

		case
		    when total_children is NULL
		        then 0
		    else total_children
		end as total_children,

		case
		    when state = 'opened'
		        then 0
		    when state = 'closed'
		        then 2
		    else 1
		end as state_order

	FROM {{ref('gitlab_epics')}} as epics
	  LEFT JOIN children_per_epic  
		ON epics.epic_id = children_per_epic.parent_id
),

issues_per_epic AS (

	SELECT 

		epic_id,
		issue_id,
		milestone_id,

		case
		    when issue_state = 'opened'
		        then 1
		    else 0
		end as open_issues,

		case
		    when issue_state = 'closed'
		        then 1
		    else 0
		end as closed_issues,

		case
		    when milestone_state = 'active'
		        then 1
		    else 0
		end as active_milestones,

		case
		    when milestone_state = 'closed'
		        then 1
		    else 0
		end as closed_milestones

	FROM {{ref('gitlab_issues_per_epic')}}

),

epic_stats AS (

	SELECT
	    epic_id,

	    COUNT(DISTINCT issue_id) as total_issues,
	    SUM(open_issues) as total_open_issues,
	    SUM(closed_issues) as total_closed_issues,

		case
		    when COUNT(DISTINCT issue_id) = 0
		        then NULL
		    else round( (100.0 * SUM(closed_issues) / COUNT(DISTINCT issue_id)), 1)
		end as issues_percent_complete,

	    COUNT(DISTINCT milestone_id) as total_milestones

	FROM issues_per_epic

	GROUP BY epic_id
)


SELECT
    epics.group_id as group_id,
    epics.epic_iid as epic_iid,
    epics.state as state,
    epics.title as title,

    total_children,

    total_issues,
    total_open_issues,
    total_closed_issues,
    issues_percent_complete,

    total_milestones,

    epics.labels as labels,
    epics.labels_str as labels_str,
    epics.upvotes as upvotes,
    epics.downvotes as downvotes,
    epics.start_date as start_date,
    epics.end_date as end_date,
    epics.due_date as due_date,
    epics.created_at as created_at,
    epics.updated_at as updated_at

FROM epics
  LEFT JOIN epic_stats  
    ON epics.epic_id = epic_stats.epic_id

ORDER BY state_order ASC, epics.end_date ASC, epics.group_id, epics.epic_iid
