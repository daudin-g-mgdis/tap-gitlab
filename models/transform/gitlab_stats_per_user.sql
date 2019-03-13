WITH users AS (

     SELECT *
     FROM {{ref('gitlab_users')}}

),

issues_authored AS (

     SELECT 
        author_id as user_id, 
        project_id, 
        milestone_id, 
        COUNT(*) as total_issues_authored
     FROM {{ref('gitlab_issues')}}
     GROUP BY author_id, project_id, milestone_id

),

issues_assigned AS (

     SELECT 
        assignee_id as user_id, 
        project_id, 
        milestone_id, 
        COUNT(*) as total_issues_assigned
     FROM {{ref('gitlab_issues')}}
     GROUP BY assignee_id, project_id, milestone_id

),

merge_requests_authored AS (

     SELECT 
        author_id as user_id, 
        project_id, 
        milestone_id, 
        COUNT(*) as total_mrs_authored
     FROM {{ref('gitlab_merge_requests')}}
     GROUP BY author_id, project_id, milestone_id

),

merge_requests_assigned AS (

     SELECT 
        assignee_id as user_id, 
        project_id, 
        milestone_id, 
        COUNT(*) as total_mrs_assigned
     FROM {{ref('gitlab_merge_requests')}}
     GROUP BY assignee_id, project_id, milestone_id

),

project AS (

     SELECT project_id, project_name
     FROM {{ref('gitlab_projects')}}

),

milestone AS (

     SELECT milestone_id, title
     FROM {{ref('gitlab_project_milestones')}}

     UNION 

     SELECT milestone_id, title
     FROM {{ref('gitlab_group_milestones')}}

)

SELECT
    u.user_id,
    u.user_name,
    u.username,
    i1.project_id,
    project.project_name,
    i1.milestone_id,
    milestone.title,
    i1.total_issues_authored,
    i2.total_issues_assigned,
    mr1.total_mrs_authored,
    mr2.total_mrs_assigned
FROM users u
  LEFT JOIN issues_authored i1 
    ON u.user_id = i1.user_id
  LEFT JOIN issues_assigned i2  
    ON u.user_id = i2.user_id
      AND i2.project_id = i1.project_id
      AND i2.milestone_id = i1.milestone_id
  LEFT JOIN merge_requests_authored mr1 
    ON u.user_id = mr1.user_id
      AND mr1.project_id = i1.project_id
      AND mr1.milestone_id = i1.milestone_id
  LEFT JOIN merge_requests_assigned mr2 
    ON u.user_id = mr2.user_id
      AND mr2.project_id = i1.project_id
      AND mr2.milestone_id = i1.milestone_id
  LEFT JOIN project 
    ON project.project_id = i1.project_id
  LEFT JOIN milestone 
    ON milestone.milestone_id = i1.milestone_id
ORDER BY u.user_name, project.project_name, milestone.title
