with source as (

    select * from {{var('schema')}}.branches

),

renamed as (

    select

        project_id as project_id,
        name as branch_name,

        commit_id as commit_id,

        "default" as default_branch,
        developers_can_merge as developers_can_merge,
        developers_can_push as developers_can_push,
        can_push as can_push,
        merged as merged,
        protected as protected

    from source

)

select * from renamed
