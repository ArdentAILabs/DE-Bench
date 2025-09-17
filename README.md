# DE-Bench
DE Bench: Can Agents Solve Real-World Data Engineering Problems?

This is repository of real world problems for Data Engineering Agents to solve. It was designed to test Ardent's Agents

There is a README within each test folder to explain the problem and the tests

To Run this testing yourself:

1. Clone the repo into wherever you want. Ideally a tests folder

2. Set Environment variables

  You will have to set a ton of environment variables for the tests to work. This provides the neccesary information for the tests to set up the right environments as well as provide the agent enough information to make solving the problem possible.


## Environment Variables Template:

Below is a template of all environment variables needed for the tests. Copy this to your `.env` file and replace the placeholder values with your own credentials. If there is an actual value there already do not change it:

<pre><code>
# AWS Credentials
ACCESS_KEY_ID_AWS="YOUR_AWS_ACCESS_KEY_ID"
SECRET_ACCESS_KEY_AWS="YOUR_AWS_SECRET_ACCESS_KEY"

# AWS S3 Credentials (for Snowflake S3 integration)
AWS_ACCESS_KEY="YOUR_AWS_ACCESS_KEY"
AWS_SECRET_KEY="YOUR_AWS_SECRET_KEY"

# MongoDB
MONGODB_URI="YOUR_MONGODB_CONNECTION_STRING"

# MySQL
MYSQL_HOST="YOUR_MYSQL_HOST"
MYSQL_PORT=3306
MYSQL_USERNAME="YOUR_MYSQL_USERNAME"
MYSQL_PASSWORD="YOUR_MYSQL_PASSWORD"

# Supabase
SUPABASE_PROJECT_URL="YOUR_SUPABASE_PROJECT_URL"
SUPABASE_API_KEY="YOUR_SUPABASE_API_KEY"
SUPABASE_URL="YOUR_SUPABASE_URL"
SUPABASE_SERVICE_ROLE_KEY="YOUR_SUPABASE_SERVICE_ROLE_KEY"
SUPABASE_JWT_SECRET="YOUR_SUPABASE_JWT_SECRET"

# PostgreSQL
POSTGRES_HOSTNAME="YOUR_POSTGRES_HOSTNAME"
POSTGRES_PORT=5432
POSTGRES_USERNAME="YOUR_POSTGRES_USERNAME"
POSTGRES_PASSWORD="YOUR_POSTGRES_PASSWORD"

# Snowflake
SNOWFLAKE_ACCOUNT="YOUR_SNOWFLAKE_ACCOUNT"
SNOWFLAKE_USER="YOUR_SNOWFLAKE_USER"
SNOWFLAKE_PASSWORD="YOUR_SNOWFLAKE_PASSWORD"
SNOWFLAKE_WAREHOUSE="YOUR_SNOWFLAKE_WAREHOUSE"
SNOWFLAKE_ROLE="SYSADMIN"

# Azure SQL
AZURE_SQL_SERVER="YOUR_AZURE_SQL_SERVER"
AZURE_SQL_USERNAME="YOUR_AZURE_SQL_USERNAME"
AZURE_SQL_PASSWORD="YOUR_AZURE_SQL_PASSWORD"
AZURE_SQL_VERSION=18

# Airflow Configuration
AIRFLOW_GITHUB_TOKEN="YOUR_GITHUB_TOKEN"
AIRFLOW_REPO="YOUR_AIRFLOW_REPO_URL"
AIRFLOW_DAG_PATH="dags/"
AIRFLOW_REQUIREMENTS_PATH="Requirements/"
AIRFLOW_HOST="http://localhost:8888"
AIRFLOW_USERNAME="airflow"
AIRFLOW_PASSWORD="airflow"
AIRFLOW_UID=501
AIRFLOW_GID=0
AIRFLOW_IMAGE_NAME="apache/airflow:2.10.5"
_AIRFLOW_WWW_USER_USERNAME="airflow"
_AIRFLOW_WWW_USER_PASSWORD="airflow"
AIRFLOW__CORE__LOAD_EXAMPLES=false

# Databricks Configuration
DATABRICKS_HOST="YOUR_DATABRICKS_HOST"
DATABRICKS_TOKEN="YOUR_DATABRICKS_TOKEN"
DATABRICKS_CLUSTER_ID="YOUR_DATABRICKS_CLUSTER_ID"
DATABRICKS_HTTP_PATH="YOUR_DATABRICKS_HTTP_PATH"
DATABRICKS_JOBS_WORKSPACE_URL="YOUR_DATABRICKS_WORKSPACE_URL"
DATABRICKS_JOBS_ACCESS_TOKEN="YOUR_DATABRICKS_ACCESS_TOKEN"
DATABRICKS_JOBS_GITHUB_TOKEN="YOUR_DATABRICKS_GITHUB_TOKEN"
DATABRICKS_JOBS_REPO="YOUR_DATABRICKS_REPO_URL"
DATABRICKS_JOBS_REPO_PATH="YOUR_DATABRICKS_REPO_PATH"

# Finch API
FINCH_ACCESS_TOKEN="YOUR_FINCH_ACCESS_TOKEN"

# Astronomer Cloud Configuration
ASTRO_WORKSPACE_ID="YOUR_ASTRO_WORKSPACE_ID"
ASTRO_ACCESS_TOKEN="YOUR_ASTRO_ACCESS_TOKEN"
ASTRO_API_TOKEN="YOUR_ASTRO_API_TOKEN"   # This can be used instead of the ASTRO_ACCESS_TOKEN
ASTRO_CLOUD_PROVIDER="aws"
ASTRO_REGION="us-east-1"

# Azure Configuration (for Claude_Code mode and container services)
# Azure Service Principal (for AKS access)
AZURE_CLIENT_ID="YOUR_AZURE_SERVICE_PRINCIPAL_CLIENT_ID"
AZURE_CLIENT_SECRET="YOUR_AZURE_SERVICE_PRINCIPAL_CLIENT_SECRET"
AZURE_TENANT_ID="YOUR_AZURE_TENANT_ID"
AZURE_SUBSCRIPTION_ID="YOUR_AZURE_SUBSCRIPTION_ID"

# Azure Container Services
ACI_RESOURCE_GROUP="YOUR_AKS_RESOURCE_GROUP_NAME"
ACI_CONTAINER_GROUP_NAME="YOUR_CONTAINER_GROUP_NAME"
ACR_REGISTRY_SERVER="YOUR_ACR_REGISTRY_SERVER"
ACR_REGISTRY_USERNAME="YOUR_ACR_USERNAME"
ACR_REGISTRY_PASSWORD="YOUR_ACR_PASSWORD"

# Azure Kubernetes Service
AKS_CLUSTER_NAME="YOUR_AKS_CLUSTER_NAME"
AKS_IMAGE_NAME="YOUR_AKS_IMAGE_NAME"

# Azure Storage Account
AZURE_STORAGE_ACCOUNT_NAME="YOUR_STORAGE_ACCOUNT_NAME"
AZURE_STORAGE_ACCOUNT_KEY="YOUR_STORAGE_ACCOUNT_KEY"

# Azure Key Vault
AZURE_KEY_VAULT_NAME="YOUR_KEY_VAULT_NAME"

# AWS Credentials for Claude Code (Bedrock access)
AWS_ACCESS_KEY_ID_CLAUDE="YOUR_AWS_ACCESS_KEY_FOR_CLAUDE_BEDROCK"
AWS_SECRET_ACCESS_KEY_CLAUDE="YOUR_AWS_SECRET_KEY_FOR_CLAUDE_BEDROCK"
AWS_DEFAULT_REGION_CLAUDE="us-east-1"

# Claude Code Configuration
IS_SANDBOX=1

# OpenAI Configuration (for OpenAI_Codex mode - Coming Soon)
OPENAI_API_KEY="YOUR_OPENAI_API_KEY"
AZURE_OPENAI_API_KEY="YOUR_AZURE_OPENAI_API_KEY"
AZURE_OPENAI_ENDPOINT="YOUR_AZURE_OPENAI_ENDPOINT"
AZURE_OPENAI_API_VERSION="2023-12-01-preview"
AZURE_OPENAI_CHAT_DEPLOYMENT_NAME="YOUR_DEPLOYMENT_NAME"
</code></pre>

### Custom Variables for Your Setup

Below are custom variables for your specific setup. We set up the Ardent configs as an example:

<pre><code>
# Ardent AI Configuration (Example Custom Setup)
ARDENT_PUBLIC_KEY="YOUR_ARDENT_PUBLIC_KEY"
ARDENT_SECRET_KEY="YOUR_ARDENT_SECRET_KEY"
ARDENT_BASE_URL="http://localhost:8000"
</code></pre>

3. Edit the Run_Model.py file to edit the wrapper and import in your model. You must make sure MODEL_PATH is the same path for your model import. Plug in your model to the wrapper function in Run_Model




4. Set up and run the Docker Compose environment:

```bash
# Build and start the containers
docker-compose up --build

# Or run in detached mode
docker-compose up -d --build
```

5. Run tests from inside the container:

```bash
# Enter the container
docker-compose exec de-bench bash

# Run tests with various options
pytest -n auto -sv                    # Run with default settings (parallel)
pytest -sv -k "keyword"               # Run tests by keyword
pytest -m "postgres"                  # Run tests by marker
pytest                                # Run all tests without parallelization (not recommended)
```

## Execution Modes

The test suite supports multiple AI model execution modes via the `--mode` flag:

### Available Modes:

1. **Ardent** (Default) - Uses Ardent AI's backend service
2. **Claude_Code** - Uses Anthropic's Claude Code CLI in Kubernetes containers  
3. **OpenAI_Codex** - Uses OpenAI's Codex CLI (coming soon)

### Running Tests with Different Modes:

```bash
# Run with Ardent AI (default mode)
pytest Tests/MongoDB_Agent_Add_Record/ --mode Ardent -v

# Run with Claude Code in Kubernetes
pytest Tests/MongoDB_Agent_Add_Record/ --mode Claude_Code -v

# Run multiple tests in parallel with Claude Code
pytest -n auto --mode Claude_Code -v

# Run specific test categories with different modes
pytest -m "postgres" --mode Claude_Code -v
pytest -m "airflow" --mode Ardent -v
```

### Mode-Specific Requirements:

**Ardent Mode:**
- Requires `ARDENT_PUBLIC_KEY` and `ARDENT_SECRET_KEY` environment variables
- Uses Supabase for account management

**Claude_Code Mode:**
- Requires Azure Kubernetes Service (AKS) access
- Uses Kubernetes pods with Claude Code CLI installed
- Automatically handles container lifecycle and cleanup
- **Required Environment Variables:**
  - `AZURE_CLIENT_ID` - Azure service principal client ID
  - `AZURE_CLIENT_SECRET` - Azure service principal client secret  
  - `AZURE_TENANT_ID` - Azure tenant ID
  - `AZURE_SUBSCRIPTION_ID` - Azure subscription ID
  - `ACI_RESOURCE_GROUP` - Azure resource group containing AKS cluster
  - `AKS_CLUSTER_NAME` - Name of the AKS cluster
  - `AWS_ACCESS_KEY_ID_CLAUDE` - AWS access key for Claude Code (Bedrock access)
  - `AWS_SECRET_ACCESS_KEY_CLAUDE` - AWS secret key for Claude Code
  - `AWS_DEFAULT_REGION_CLAUDE` - AWS region for Claude Code (defaults to us-east-1)
  - `IS_SANDBOX=1` - Required for Claude Code non-interactive execution

**OpenAI_Codex Mode:** (Coming Soon)
- Will require OpenAI API credentials
- Will support Azure OpenAI deployments

### Performance & Usage Tips:

**Parallel Execution:**
```bash
# Run 4 tests in parallel with Claude Code (recommended)
pytest -n auto --mode Claude_Code Tests/MongoDB_Agent_Add_Record/ Tests/PostgreSQL_Agent_Add_Record/ Tests/MySQL_Agent_Update_Records/ Tests/Snowflake_Agent_Add_Record/ -v

# Compare performance between modes
pytest Tests/MongoDB_Agent_Add_Record/ --mode Ardent -v
pytest Tests/MongoDB_Agent_Add_Record/ --mode Claude_Code -v
```

**Resource Management:**
- **Claude_Code mode** automatically creates and destroys Kubernetes pods for each test
- **Ardent mode** uses persistent backend connections
- Both modes support parallel execution with `pytest-xdist` (`-n auto`)
- Cleanup is handled automatically even if tests are interrupted with Ctrl+C

**Benchmarking:**
```bash
# Benchmark all modes on the same test
pytest Tests/MongoDB_Agent_Add_Record/ --mode Ardent -v --tb=short
pytest Tests/MongoDB_Agent_Add_Record/ --mode Claude_Code -v --tb=short
# pytest Tests/MongoDB_Agent_Add_Record/ --mode OpenAI_Codex -v --tb=short  # Coming soon
```

Pytest supports `and` & `or` operators too. Something like `pytest -m "one and two"` will work.

**⚠️ Important: Graceful Test Interruption**

The test suite now handles **Ctrl+C** gracefully! When you interrupt tests with Ctrl+C:
- ✅ **All resources are cleaned up** (databases, containers, cloud resources)
- ✅ **No orphaned resources** are left behind
- ✅ **Fixture teardown runs** automatically
- ✅ **Temp files are removed**

You can safely interrupt long-running tests without worrying about cleanup! DO NOT SPAM CONTROL C though

6. Configure your tools and permissions:

MongoDB:
- Required Role: dbAdmin
- Permissions needed:
  - Create/Delete Collections
  - Create/Delete Databases
  - Read/Write to Collections

Snowflake:
- Required Role: SYSADMIN (or custom role with database creation permissions)
- Required Permissions:
  - CREATE DATABASE
  - CREATE SCHEMA
  - CREATE TABLE
  - COPY INTO (for S3 loading)
- AWS S3 Access: Ensure AWS credentials have S3 read permissions for parquet files

7. A lot of the tests run on tools or frameworks. We've set up a clean .env file with all the necessary variables needed. We've tried to optimize the setup of all the tests but it will likely charge some credits through the tools. Keep that in mind.



Notes:

-Tigerbeetle must be set up with VOPR for testing.
-Mongo must have permisions to create and drop collections and databases
-Airflow must be set up with git sync enabled to the repo you provide
-make sure your mySQL password and username are up to date. AWS sets defaults to rotate once a week...
-Postgres must have the postgres db in it to function (i mean u shouldn't have deleted this anyway)

## Common Errors

### **Astronomer Token Expired**
```
subprocess.CalledProcessError: Command '['astro', 'login', '--token-login', 'eyJhbGciOiJSUzI1NiIs...']' returned non-zero exit status 1.
```
**Solution:** Your `ASTRO_ACCESS_TOKEN` has expired. Generate a new token from your Astronomer account and update your `.env` file.


### **Database Connection Errors**
```
psycopg2.OperationalError: could not connect to server
mysql.connector.errors.DatabaseError: Can't connect to MySQL server
```
**Solution:** Check your database credentials in the `.env` file. For AWS RDS, credentials may rotate weekly - update them as needed.