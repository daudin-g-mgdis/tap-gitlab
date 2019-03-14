with source as (

    select * from {{var('schema')}}.group_milestones

),

renamed as (

    select

        id as milestone_id,
        
        group_id as group_id,
        iid as iid,

        title as title,
        description as description,

        state as state,

        start_date as start_date,
        due_date as due_date,
        created_at as created_at,
        updated_at as updated_at

    from source

)

select * from renamed
