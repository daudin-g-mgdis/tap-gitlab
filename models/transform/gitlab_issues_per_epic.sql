-- Conditionally generate this model only for Gitlab Ultimate accounts
{{
  config(
    enabled=var('ultimate_license')|lower in ["true", "1", "yes", "on"]
  )
}}

WITH epics AS (

     SELECT *
     FROM {{ref('gitlab_epics')}}

),

epic_issues AS (

     SELECT *
     FROM {{ref('gitlab_epic_issues')}}

),

issues AS (

     SELECT *
     FROM {{ref('gitlab_issues')}}

),

milestones AS (

     SELECT *
     FROM {{ref('gitlab_milestones')}}

)


SELECT

    epics.epic_id as epic_id,
    epics.epic_iid as epic_iid,
    epics.parent_id as epic_parent_id,
    epics.group_id as epic_group_id,
    epics.author_id as epic_author_id,
    epics.state as epic_state,
    epics.title as epic_title,
    epics.labels as epic_labels,
    epics.labels_str as epic_labels_str,
    epics.upvotes as epic_upvotes,
    epics.downvotes as epic_downvotes,
    epics.start_date as epic_start_date,
    epics.end_date as epic_end_date,
    epics.due_date as epic_due_date,
    epics.created_at as epic_created_at,
    epics.updated_at as epic_updated_at,

    epic_issues.relative_position as relative_position,

    issues.issue_id as issue_id,
    issues.project_id as issue_project_id,
    issues.iid as issue_iid,
    issues.author_id as issue_author_id,
    issues.assignee_id as issue_assignee_id,
    issues.state as issue_state,
    issues.title as issue_title,
    issues.labels as issue_labels,
    issues.labels_str as issue_labels_str,
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

FROM epics
  JOIN epic_issues  
    ON epics.epic_iid = epic_issues.epic_iid
  JOIN issues  
    ON issues.issue_id = epic_issues.issue_id
  LEFT JOIN milestones  
    ON issues.milestone_id = milestones.milestone_id
