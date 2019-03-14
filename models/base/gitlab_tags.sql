with source as (

    select * from {{var('schema')}}.tags

),

renamed as (

    select

        project_id as project_id,
        commit_id as commit_id,
        name as tag_name,

        target as target,

        message as message

    from source

)

select * from renamed
