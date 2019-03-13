with source as (

    select * from {{var('schema')}}.group_labels

),

renamed as (

    select

        id as label_id,
        group_id as group_id,

        name as label_name,
        description as description,

        open_issues_count as open_issues_count,
        open_merge_requests_count as open_merge_requests_count,
        closed_issues_count as closed_issues_count,

        color as color,
        text_color as text_color,
        subscribed as subscribed

    from source

)

select * from renamed
