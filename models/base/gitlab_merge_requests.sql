with source as (

    select * from {{var('schema')}}.merge_requests

),

renamed as (

    select

        id as merge_request_id,

        project_id as project_id,
        iid as iid,
        
        milestone_id as milestone_id,

        author_id as author_id,

        assignee_id as assignee_id,
        assignees as assignees,

        closed_by_id as closed_by_id,
        closed_at as closed_at,

        merged_by_id as merged_by_id,
        merged_at as merged_at,

        state as state,
        case
            when state = 'opened'
                then 1
            else 0
        end as state_open_mrs,

        case
            when state = 'closed'
                then 1
            else 0
        end as state_closed_mrs,

        case
            when state = 'merged'
                then 1
            else 0
        end as state_merged_mrs,

        title as title,
        description as description,
        sha as sha,

        labels as labels,
        (
            SELECT string_agg(trim(label_elements::text, '"'), ', ')
            FROM jsonb_array_elements(labels) label_elements
        ) AS labels_str,

        source_project_id as source_project_id,
        source_branch as source_branch,
        target_project_id as target_project_id,
        target_branch as target_branch,

        upvotes as upvotes,
        downvotes as downvotes,
        user_notes_count as user_notes_count,

        allow_collaboration as allow_collaboration,
        allow_maintainer_to_push as allow_maintainer_to_push,

        work_in_progress as work_in_progress,
        merge_status as merge_status,

        merge_when_pipeline_succeeds as merge_when_pipeline_succeeds,
        force_remove_source_branch as force_remove_source_branch,
        should_remove_source_branch as should_remove_source_branch,
        squash as squash,

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

        EXTRACT(DAY FROM merged_at) merged_day,
        EXTRACT(WEEK FROM merged_at) merged_week,
        EXTRACT(MONTH FROM merged_at) merged_month,
        EXTRACT(QUARTER FROM merged_at) merged_quarter,
        EXTRACT(YEAR FROM merged_at) merged_year,

        -- Add time to merge and time to close metrics
        round(abs(extract(epoch from merged_at - created_at))::numeric, 0) as time_to_merge,
        round(abs(extract(epoch from merged_at - created_at)/3600)::numeric, 2) as hours_to_merge,
        round(abs(extract(epoch from merged_at - created_at)/86400)::numeric, 2) as days_to_merge,

        round(abs(extract(epoch from closed_at - created_at))::numeric, 0) as time_to_close,
        round(abs(extract(epoch from closed_at - created_at)/3600)::numeric, 2) as hours_to_close,
        round(abs(extract(epoch from closed_at - created_at)/86400)::numeric, 2) as days_to_close

    from source

)

select * from renamed
