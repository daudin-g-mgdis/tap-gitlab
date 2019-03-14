with source as (

    select * from {{var('schema')}}.commits

),

renamed as (

    select

        id as commit_id,
        short_id as short_id,
        project_id as project_id,

        parent_ids as parent_ids,

        title as title,
        message as message,

        author_name as author_name,
        author_email as author_email,
        authored_date as authored_date,

        committer_name as committer_name,
        committer_email as committer_email,
        committed_date as committed_date,

        created_at as created_at

    from source

)

select * from renamed
