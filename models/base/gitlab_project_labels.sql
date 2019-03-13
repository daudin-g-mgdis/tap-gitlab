with source as (

    select * from {{var('schema')}}.project_labels

),

renamed as (

    select

        id as label_id,
        project_id as project_id,

        name as label_name,
        description as description,

        open_issues_count as open_issues_count,
        open_merge_requests_count as open_merge_requests_count,
        closed_issues_count as closed_issues_count,

        -- Project Specific attributes
        is_project_label as is_project_label,
        priority as priority,

        color as color,
        text_color as text_color,
        subscribed as subscribed

    from source

)

select * from renamed
