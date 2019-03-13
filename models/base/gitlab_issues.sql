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

        state as state,

        title as title,
        description as description,
        labels as labels,

        weight as weight,
        confidential as confidential,

        upvotes as upvotes,
        downvotes as downvotes,
        user_notes_count as user_notes_count,
        merge_requests_count as merge_requests_count,

        due_date as due_date,

        created_at as created_at,
        updated_at as updated_at

    from source

)

select * from renamed
