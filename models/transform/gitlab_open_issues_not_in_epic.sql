-- Conditionally generate this model only for Gitlab Ultimate accounts
{{
  config(
    enabled=var('ultimate_license')|lower in ["true", "1", "yes", "on"]
  )
}}

WITH epic_issues AS (

    SELECT DISTINCT issue_id
    FROM {{ref('gitlab_epic_issues')}}

),

issues AS (

    SELECT *

    FROM {{ref('gitlab_issues')}}

    WHERE 
        state = 'opened' AND
        issue_id NOT IN (
            SELECT issue_id
            FROM epic_issues
        )

),

milestones AS (

    SELECT *
    FROM {{ref('gitlab_milestones')}}

)


SELECT

    issues.issue_id as issue_id,
    issues.project_id as issue_project_id,
    issues.iid as issue_iid,
    issues.author_id as issue_author_id,
    issues.assignee_id as issue_assignee_id,
    issues.state as issue_state,
    issues.title as issue_title,
    issues.labels as issue_labels,
    issues.weight as issue_weight,
    issues.confidential as issue_confidential,
    issues.upvotes as issue_upvotes,
    issues.downvotes as issue_downvotes,
    issues.user_notes_count as issue_user_notes_count,
    issues.merge_requests_count as issue_merge_requests_count,
    issues.due_date as issue_due_date,
    issues.created_at as issue_created_at,
    issues.updated_at as issue_updated_at,

    milestones.milestone_id as milestone_id,
    milestones.title as milestone_title,
    milestones.state as milestone_state,
    milestones.start_date as milestone_start_date,
    milestones.due_date as milestone_due_date

FROM issues
  LEFT JOIN milestones  
    ON issues.milestone_id = milestones.milestone_id
