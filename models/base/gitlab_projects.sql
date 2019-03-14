with source as (

    select * from {{var('schema')}}.projects

),

renamed as (

    select

        id as project_id,

        creator_id as creator_id,
        owner_id as owner_id,

        case
            when namespace__kind = 'group'
                then namespace__id
            else NULL
        end as group_id,

        name as project_name,
        description as description,

        path as path,
        path_with_namespace as path_with_namespace,

        visibility as visibility,

        forks_count as forks_count,
        open_issues_count as open_issues_count,
        star_count as star_count,

        statistics__commit_count as commit_count,
        statistics__job_artifacts_size as job_artifacts_size,
        statistics__lfs_objects_size as lfs_objects_size,
        statistics__repository_size as repository_size,
        statistics__storage_size as storage_size,

        archived as archived,
        public as public,

        request_access_enabled as request_access_enabled,
        issues_enabled as issues_enabled,
        merge_requests_enabled as merge_requests_enabled,
        snippets_enabled as snippets_enabled,

        builds_enabled as builds_enabled,
        container_registry_enabled as container_registry_enabled,
        lfs_enabled as lfs_enabled,
        public_builds as public_builds,
        shared_runners_enabled as shared_runners_enabled,
        wiki_enabled as wiki_enabled,

        only_allow_merge_if_all_discussions_are_resolved as only_allow_merge_if_all_discussions_are_resolved,
        only_allow_merge_if_build_succeeds as only_allow_merge_if_build_succeeds,
        approvals_before_merge as approvals_before_merge,

        shared_with_groups as shared_with_groups,
        tag_list as tag_list,

        created_at as created_at,
        last_activity_at as last_activity_at

    from source

)

select * from renamed 
  