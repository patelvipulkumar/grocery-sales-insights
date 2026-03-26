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

### 3. Verify Secrets Are Set

Go back to **Settings** → **Secrets and variables** → **Actions** and confirm:
- ✅ `GCP_PROJECT_ID` listed
- ✅ `GCP_SA_KEY` listed

Both should show "Last used: Never" (until workflows run).

## Testing Secrets

Once you uncomment CI/CD triggers in `.github/workflows/`, the pipelines will:
1. **CI**: Verify Python lint, dbt compile, Terraform fmt on every push/PR
2. **CD**: Deploy to GCP on merge to `main` branch

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
