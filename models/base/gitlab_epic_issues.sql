-- Conditionally generate this model only for Gitlab Ultimate accounts
{{
  config(
    enabled=var('ultimate_license')|lower in ["true", "1", "yes", "on"]
  )
}}


with source as (

    select * from {{var('schema')}}.epic_issues

),

renamed as (

    select

        -- Keys

        -- The group the epic is in and the iid of the epic (id inside the group)
        group_id as group_id,
        epic_iid as epic_iid,

        -- The id of the epic_issue (it should be a key by itself, but the API needs all 3)
        epic_issue_id as epic_issues_id,

        -- Data about the issue

        -- The id and iid of the issue
        issue_id as issue_id,
        issue_iid as issue_iid,

        -- Relative position of the issue in the epic
        relative_position as relative_position,

        -- The project_id of the issue
        project_id as project_id

    from source

)

select * from renamed
