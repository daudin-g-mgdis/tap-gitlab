WITH merged_merge_requests AS (

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
		project_id, 
		merged_year, 
		merged_month, 
		count(distinct merge_request_id) as total_mrs, 
		count(distinct author_id) as total_authors, 
		round(avg(days_to_merge), 2) as days_to_merge

	FROM merged_merge_requests

	GROUP BY project_id, merged_year, merged_month

)

SELECT 
	*,
	round(total_mrs::numeric/total_authors, 2) as mrs_per_author

FROM toral_mrs_per_month