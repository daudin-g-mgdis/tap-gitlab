with source as (

    select * from {{var('schema')}}.issues

),

renamed as (

    select

        id as issue_id,

        project_id as project_id,
        iid as iid,

        milestone_id as milestone_id,

        author_id as author_id,
        assignee_id as assignee_id,
        assignees as assignees,

        closed_by_id as closed_by_id,
        closed_at as closed_at,

        state as state,
        case
            when state = 'opened'
                then 1
            else 0
        end as state_open_issues,

        case
            when state = 'closed'
                then 1
            else 0
        end as state_closed_issues,
        
        title as title,
        description as description,

        labels as labels,
        (
            SELECT string_agg(trim(label_elements::text, '"'), ', ')
            FROM jsonb_array_elements(labels) label_elements
        ) AS labels_str,

        weight as weight,
        confidential as confidential,

        upvotes as upvotes,
        downvotes as downvotes,
        user_notes_count as user_notes_count,
        merge_requests_count as merge_requests_count,

        due_date::date as due_date,

        time_estimate as time_estimate,
        total_time_spent as total_time_spent,
        human_time_estimate as human_time_estimate,
        human_total_time_spent as human_total_time_spent,

        created_at as created_at,
        updated_at as updated_at,

        -- Date parts for easy grouping 
        EXTRACT(DAY FROM created_at) created_day,
        EXTRACT(WEEK FROM created_at) created_week,
        EXTRACT(MONTH FROM created_at) created_month,
        EXTRACT(QUARTER FROM created_at) created_quarter,
        EXTRACT(YEAR FROM created_at) created_year,

        EXTRACT(DAY FROM closed_at) closed_day,
        EXTRACT(WEEK FROM closed_at) closed_week,
        EXTRACT(MONTH FROM closed_at) closed_month,
        EXTRACT(QUARTER FROM closed_at) closed_quarter,
        EXTRACT(YEAR FROM closed_at) closed_year,

        -- Add time to close metrics
        round(abs(extract(epoch from closed_at - created_at))::numeric, 0) as time_to_close,
        round(abs(extract(epoch from closed_at - created_at)/3600)::numeric, 2) as hours_to_close,
        round(abs(extract(epoch from closed_at - created_at)/86400)::numeric, 2) as days_to_close
        

    from source

)

select * from renamed
