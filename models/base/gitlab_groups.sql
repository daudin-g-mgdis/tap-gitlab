with source as (

    select * from {{var('schema')}}.groups

),

renamed as (

    select

        id as group_id,

        name as group_name,
        full_name as full_name,

        description as description,

        path as path,
        full_path as full_path,

        lfs_enabled as lfs_enabled,
        request_access_enabled as request_access_enabled,
        visibility_level as visibility_level

    from source

)

select * from renamed
