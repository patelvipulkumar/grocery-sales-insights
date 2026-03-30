# GitHub Secrets Setup Guide

This guide walks you through configuring GitHub repository secrets needed for CI/CD pipelines.

## Prerequisites

- GitHub repository access with admin permissions
- GCP service account key (JSON file)
- GCP project ID

## Steps

### 1. Prepare Your GCP Service Account Key

1. Go to [Google Cloud Console](https://console.cloud.google.com)
2. Navigate to **Service Accounts** → Select your service account
3. Go to **Keys** tab → **Create new key** → **JSON**
4. Save the JSON file securely
5. Open the file in a text editor — you'll need its full contents

### 2. Add Secrets to GitHub Repository

1. Navigate to your GitHub repository
2. Go to **Settings** → **Secrets and variables** → **Actions**
3. Click **New repository secret**

#### Add Secret #1: `GCP_PROJECT_ID`
- **Name**: `GCP_PROJECT_ID`
- **Value**: Your GCP project ID (e.g., `Your Project ID`)
- Click **Add secret**

#### Add Secret #2: `GCP_SA_KEY`
- **Name**: `GCP_SA_KEY`
- **Value**: Full contents of your GCP service account JSON key file
  - Open the file and copy **entire contents** (including `{` and `}`)
  - Paste into the secret value field
- Click **Add secret**

#### Add Secret #3: `DOCKERHUB_USERNAME`
- **Name**: `DOCKERHUB_USERNAME`
- **Value**: Your Docker Hub username (for this project, typically `patelvipulkumar`)
- Click **Add secret**

#### Add Secret #4: `DOCKERHUB_TOKEN`
- **Name**: `DOCKERHUB_TOKEN`
- **Value**: Docker Hub access token with push permission
  - Create from Docker Hub: **Account Settings** → **Personal access tokens**
  - Use token value (not your account password)
- Click **Add secret**

#### Add Secret #5: `GCP_REGION`
- **Name**: `GCP_REGION`
- **Value**: GCP region/zone for your infrastructure (e.g., `us-west1-a`)
- Click **Add secret**

#### Add Secret #6: `KAGGLE_API_TOKEN`
- **Name**: `KAGGLE_API_TOKEN`
- **Value**: Your Kaggle API token (find it at kaggle.com → Account → API → Create New Token, copy the `key` value)
- Click **Add secret**

#### Add Secret #7: `LOOKER_STUDIO_REPORT_ID`
- **Name**: `LOOKER_STUDIO_REPORT_ID`
- **Value**: Your Looker Studio report UUID (from the report URL)
- Click **Add secret**

#### Add Secret #8: `AIRFLOW_DB_PASSWORD`
- **Name**: `AIRFLOW_DB_PASSWORD`
- **Value**: Password for the Airflow PostgreSQL metadata database
- Click **Add secret**

#### Optional Secret #9: `DOCKER_IMAGE_REPOSITORY`
- **Name**: `DOCKER_IMAGE_REPOSITORY`
- **Value**: Docker image repository to push from CD (for example `yourdockerhubuser/your-image-name`)
- If omitted, CD defaults to `patelvipulkumar/grocerysalesendtoend`
- Use this if you are forking this project or want to publish to your own repo

#### Optional Secret #10: `GCE_INSTANCE_NAME`
- **Name**: `GCE_INSTANCE_NAME`
- **Value**: Name of the GCE VM instance where Airflow runs (e.g. `airflow-vm`)
- If set alongside `GCE_ZONE`, CD will SSH into the VM and run `docker-compose pull && docker-compose up -d` automatically
- If omitted, the deploy step prints manual instructions and exits successfully

#### Optional Secret #11: `GCE_ZONE`
- **Name**: `GCE_ZONE`
- **Value**: GCP zone of the GCE VM (e.g. `us-west1-a`)
- Required together with `GCE_INSTANCE_NAME` to enable automated GCE deployment

### 3. Verify Secrets Are Set

Go back to **Settings** → **Secrets and variables** → **Actions** and confirm:
- ✅ `GCP_PROJECT_ID` listed
- ✅ `GCP_SA_KEY` listed
- ✅ `GCP_REGION` listed
- ✅ `DOCKERHUB_USERNAME` listed
- ✅ `DOCKERHUB_TOKEN` listed
- ✅ `KAGGLE_API_TOKEN` listed
- ✅ `LOOKER_STUDIO_REPORT_ID` listed
- ✅ `AIRFLOW_DB_PASSWORD` listed
- ✅ `DOCKER_IMAGE_REPOSITORY` listed (optional)
- ✅ `GCE_INSTANCE_NAME` listed (optional — enables automated GCE deployment)
- ✅ `GCE_ZONE` listed (optional — required with GCE_INSTANCE_NAME)

Both should show "Last used: Never" (until workflows run).

## Testing Secrets

Once you uncomment CI/CD triggers in `.github/workflows/`, the pipelines will:
1. **CI**: Verify Python lint, dbt compile, Terraform fmt on every push/PR
2. **CD**: Build and push image to your configured Docker repository (`DOCKER_IMAGE_REPOSITORY`) or default to `patelvipulkumar/grocerysalesendtoend`, then deploy on merge to `main`

## ⚠️ Security Notes

- Never commit `.json` files or paste keys into code
- Secrets are masked in workflow logs
- Rotate service account keys periodically
- Use [least-privilege IAM roles](https://cloud.google.com/iam/docs/understanding-roles) for service accounts

## When Ready to Enable Workflows

Uncomment the `on:` triggers in:
- `.github/workflows/ci.yml` — Runs on push and PR to `main`
- `.github/workflows/cd.yml` — Runs on push to `main` (auto-deploy)

Then commit and push — workflows will activate automatically.
