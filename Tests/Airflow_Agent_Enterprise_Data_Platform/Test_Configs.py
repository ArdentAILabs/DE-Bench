import os
from dotenv import load_dotenv

load_dotenv()

User_Input = """
Create an Airflow DAG that:
1. Integrates CRM, E-commerce, and Marketing data into unified customer profiles with deduplication and real-time CDC.
2. Maps customer journeys, segments with ML, and models customer lifetime value using predictive analytics.
3. Harmonizes ERP and E-commerce transactions with validation, unified views, currency/tax handling, reconciliation, fraud monitoring, and GAAP reporting.
4. Integrates warehouse and 3PL inventory with real-time tracking, demand forecasting, multi-location optimization, automated reorder points, and KPI dashboards.
5. Tracks multichannel campaigns like email, social, and digital ads with multi-touch attribution, CAC/LTV, ROI cohorts, A/B testing, and predictive performance models.
6. Builds product analytics for performance scoring, recommendations, lifecycle analysis, competitive pricing, quality monitoring, and demand forecasting.
7. Delivers operational intelligence with real-time dashboards, supply-chain and fulfillment optimization, support analytics, quality control, and efficiency metrics.
8. Implements advanced ML for customer churn, sales forecasting, cross-sell recommendations, anomaly detection, sentiment analysis, and predictive maintenance.
9. Enforces governance with lineage, automated data-quality monitoring, GDPR anonymization, audit logging, retention policies, and access controls.
10. Ensures observability with comprehensive logging, quality and failure alerts, SLA dashboards, distributed tracing, health checks, and automated recovery.
11. Name the main DAG 'enterprise_data_platform_dag'.
12. Create it in branch 'BRANCH_NAME'.
13. Name the PR 'PR_NAME'.
"""

# Configuration will be generated dynamically by create_model_inputs function
