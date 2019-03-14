with source as (

    select * from {{var('schema')}}.users

),

renamed as (

    select

        id as user_id,

        name as user_name,
        username as username,

        state as state

    from source

)

select * from renamed
