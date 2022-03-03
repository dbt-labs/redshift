# redshift 0.6.1
## Fixes
- `redshift_maintenance()` macro now works if a custom `ref()` macro exists in the project ([#52](https://github.com/dbt-labs/redshift/issues/52), [#53](https://github.com/dbt-labs/redshift/pull/53)) ([@jeremyyeo](https://github.com/jeremyyeo))

# redshift 0.6.0

This release supports any version (minor and patch) of v1, which means far less need for compatibility releases in the future.

## Under the hood
- Change `require-dbt-version` to `[">=1.0.0", "<2.0.0"]`
- Bump dbt-utils dependency
- Replace `source-paths` with `model-paths`

# redshift v0.5.1
🚨 This is a compatibility release in preparation for `dbt-core` v1.0.0 (🎉). Projects using this version with `dbt-core` v1.0.x can expect to see a deprecation warning. This will be resolved in the next minor release.
