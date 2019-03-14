with source as (

    select * from {{var('schema')}}.releases

),

renamed as (

    select

        project_id as project_id,
        commit_id as commit_id,
        tag_name as tag_name,

        author_id as author_id,

        name as release_name,
        description as description,

        created_at as created_at

    from source

)

select * from renamed
