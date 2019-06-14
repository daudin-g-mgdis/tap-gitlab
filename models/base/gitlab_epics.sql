-- Conditionally generate this model only for Gitlab Ultimate accounts
{{
  config(
    enabled=var('ultimate_license')|lower in ["true", "1", "yes", "on"]
  )
}}


with source as (

    select * from {{var('schema')}}.epics

),

renamed as (

    select

        id as epic_id,
        iid as epic_iid,

        parent_id as parent_id,

        group_id as group_id,
        author_id as author_id,

        state as state,

        title as title,
        description as description,
        labels as labels,

        upvotes as upvotes,
        downvotes as downvotes,

        start_date as start_date,
        end_date as end_date,
        due_date as due_date,

        created_at as created_at,
        updated_at as updated_at

    from source

)

select * from renamed
