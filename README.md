# Databricks CI/CD Test

Minimal template to test Databricks deployment pipeline.

## Quick Start

1. Create repository on GitHub
2. Add secrets: DATABRICKS_HOST, DATABRICKS_TOKEN
3. Push code
4. Check GitHub Actions for deployment status

## Test Success Criteria

- ✅ GitHub Actions runs without errors
- ✅ Job deploys to Databricks workspace
- ✅ Test job runs and prints success message
- ✅ Ready to build NASA engine project!