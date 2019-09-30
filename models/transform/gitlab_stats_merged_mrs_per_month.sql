WITH projects AS (

	SELECT project_id, project_name
	
	FROM {{ref('gitlab_projects')}}

),

merged_merge_requests AS (

	SELECT 
		project_id, 
		merged_year, 
		merged_month, 
		merge_request_id, 
		author_id,
		days_to_merge

	FROM {{ref('gitlab_merge_requests')}}
	
	WHERE merged_at IS NOT NULL

),

toral_mrs_per_month AS (

	SELECT 
		projects.project_id as project_id,
		projects.project_name as project_name, 
		mrs.merged_year as merged_year, 
		mrs.merged_month as merged_month, 
		count(distinct mrs.merge_request_id) as total_mrs, 
		count(distinct mrs.author_id) as total_authors, 
		round(avg(mrs.days_to_merge), 2) as days_to_merge

	FROM merged_merge_requests mrs
	  LEFT JOIN projects   
	    ON mrs.project_id = projects.project_id

	GROUP BY 
	  projects.project_id, 
	  projects.project_name, 
	  mrs.merged_year, 
	  mrs.merged_month

)

SELECT 
	*,
	round(total_mrs::numeric/total_authors, 2) as mrs_per_author

FROM toral_mrs_per_month