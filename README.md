# dbt | tap-gitlab

This [dbt](https://github.com/fishtown-analytics/dbt) package contains data models for [tap-gitlab](https://gitlab.com/meltano/tap-gitlab).

Two variables are required:
* schema: the schema where the raw gitlab tables (result of `tap-gitlab`) are located
* ultimate_license: If it is true, then the GitLab account used has access to the Gitlab Ultimate or Gitlab.com Gold features. It will enable models over tables that are only available for Gitlab Ultimate and Gitlab.com Gold accounts. For example, `epics` and `epic_issues`.

**dbt_project.yml**
```
... ...

models:
  tap_gitlab:

    ... ...

    vars:
      schema:             'tap_gitlab'        # the schema where the raw gitlab tables are located
      ultimate_license:   'true'        # whether tables (e.g. epics) only available on Ultimate Accounts are available
```
