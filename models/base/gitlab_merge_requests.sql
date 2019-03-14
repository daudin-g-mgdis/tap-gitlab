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

        closed_by_id as closed_by_id,
        merged_by_id as merged_by_id,

        state as state,

        title as title,
        description as description,
        sha as sha,

        labels as labels,

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

        created_at as created_at,
        updated_at as updated_at

    from source

)

select * from renamed
