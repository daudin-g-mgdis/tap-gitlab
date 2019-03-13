with source as (

    select * from {{var('schema')}}.project_members

),

renamed as (

    select

        id as member_id,
        project_id as project_id,

        user_id as user_id,
        access_level as access_level,

        case
            when access_level = 10
                then 'Guest access'
            when access_level = 20
                then 'Reporter access'
            when access_level = 30
                then 'Developer access'
            when access_level = 40
                then 'Maintainer access'
            else 'Unknown'
        end as access_level_code

    from source

)

select * from renamed

