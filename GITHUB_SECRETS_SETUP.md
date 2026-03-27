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
- **Value**: Your GCP project ID (e.g., `project-ba995651-8202-4620-8c2`)
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

#### Optional Secret #5: `DOCKER_IMAGE_REPOSITORY`
- **Name**: `DOCKER_IMAGE_REPOSITORY`
- **Value**: Docker image repository to push from CD (for example `yourdockerhubuser/your-image-name`)
- If omitted, CD defaults to `patelvipulkumar/grocerysalesendtoend`
- Use this if you are forking this project or want to publish to your own repo

### 3. Verify Secrets Are Set

Go back to **Settings** → **Secrets and variables** → **Actions** and confirm:
- ✅ `GCP_PROJECT_ID` listed
- ✅ `GCP_SA_KEY` listed
- ✅ `DOCKERHUB_USERNAME` listed
- ✅ `DOCKERHUB_TOKEN` listed
- ✅ `DOCKER_IMAGE_REPOSITORY` listed (optional)

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
