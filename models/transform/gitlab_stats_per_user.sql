WITH users AS (

     SELECT 
        user_id,
        user_name
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
        COUNT(*) as total_issues_assigned, 
        SUM(state_closed_issues) as total_assigned_issues_closed
    FROM {{ref('gitlab_issues')}}
    GROUP BY assignee_id, project_id, milestone_id

),

merge_requests_authored AS (

    SELECT 
        author_id as user_id, 
        project_id, 
        milestone_id, 
        COUNT(*) as total_mrs_authored, 
        SUM(state_merged_mrs) as total_authored_mrs_merged
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

    SELECT milestone_id, title, start_date, due_date
    FROM {{ref('gitlab_milestones')}}

)

SELECT
    users.user_id as user_id,
    users.user_name as user_name,
    project.project_id as project_id,
    project.project_name as project_name,
    milestone.milestone_id as milestone_id,
    milestone.title as milestone_title,
    milestone.start_date as milestone_start_date,
    milestone.due_date as milestone_due_date,
    i1.total_issues_authored as total_issues_authored,
    i2.total_issues_assigned as total_issues_assigned,
    i2.total_assigned_issues_closed as total_assigned_issues_closed,
    mr1.total_mrs_authored as total_mrs_authored,
    mr1.total_authored_mrs_merged as total_authored_mrs_merged,
    mr2.total_mrs_assigned as total_mrs_assigned

FROM issues_authored i1  

  FULL OUTER JOIN issues_assigned i2  
    ON i2.user_id = i1.user_id
      AND i2.project_id = i1.project_id
      AND (
        i2.milestone_id = i1.milestone_id
        OR COALESCE(i1.milestone_id, i2.milestone_id) is NULL
      )

      
  FULL OUTER JOIN merge_requests_authored mr1  
    ON mr1.user_id = COALESCE(i1.user_id, i2.user_id) 
      AND mr1.project_id = COALESCE(i1.project_id, i2.project_id)
      AND (
        mr1.milestone_id = COALESCE(i1.milestone_id, i2.milestone_id)
        OR COALESCE(i1.milestone_id, i2.milestone_id, mr1.milestone_id) is NULL
      )
      
  FULL OUTER JOIN merge_requests_assigned mr2  
    ON mr2.user_id = COALESCE(i1.user_id, i2.user_id, mr1.user_id) 
      AND mr2.project_id = COALESCE(i1.project_id, i2.project_id, mr1.project_id)
      AND (
        mr2.milestone_id = COALESCE(i1.milestone_id, i2.milestone_id, mr1.milestone_id) 
        OR COALESCE(i1.milestone_id, i2.milestone_id, mr1.milestone_id, mr2.milestone_id) is NULL
      )
      
  LEFT JOIN users  
    ON users.user_id = COALESCE(i1.user_id, i2.user_id, mr1.user_id, mr2.user_id) 
    
  LEFT JOIN project  
    ON project.project_id = COALESCE(i1.project_id, i2.project_id, mr1.project_id, mr2.project_id) 
    
  LEFT JOIN milestone  
    ON milestone.milestone_id = COALESCE(i1.milestone_id, i2.milestone_id, mr1.milestone_id, mr2.milestone_id) 

WHERE
    users.user_id is not NULL 
    AND project.project_id is not NULL
    AND (
      -- Don't keep users with nothing to show
      (total_issues_authored is not NULL) or
      (total_issues_assigned is not NULL) or
      (total_mrs_authored is not NULL) or
      (total_mrs_assigned is not NULL)
    )