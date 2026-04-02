import pandas as pd
import numpy as np
import psycopg2
from scipy.stats import ttest_ind

# -------------------------------
# STEP 1: Simulated Mongo Data
# -------------------------------
np.random.seed(42)

engagement_data = pd.DataFrame({
    "customer_id": np.random.randint(1, 1200, 1000),
    "sessions": np.random.randint(1, 50, 1000),
    "session_duration_sec": np.random.randint(100, 5000, 1000),
    "feature": np.random.choice(["Feature_A", "Feature_B", "Feature_C"], 1000),
    "date": pd.date_range(start="2024-01-01", periods=1000, freq="D")
})

# -------------------------------
# STEP 2: PostgreSQL Connection
# -------------------------------
conn = psycopg2.connect(
    dbname="nimbus_core",
    user="postgres",
    password="hariss1522",   # your password
    host="localhost",
    port="5432"
)

# -------------------------------
# STEP 3: SQL Query (FIXED schema)
# -------------------------------
sql_query = """
SELECT 
    c.customer_id,
    c.company_name,
    c.is_active,
    c.churned_at,
    p.plan_tier,
    s.start_date,
    s.end_date
FROM nimbus.customers c
JOIN nimbus.subscriptions s ON c.customer_id = s.customer_id
JOIN nimbus.plans p ON s.plan_id = p.plan_id
"""

sql_df = pd.read_sql(sql_query, conn)
print("SQL rows:", len(sql_df))

# -------------------------------
# STEP 4: Merge Data
# -------------------------------
sql_df['customer_id'] = sql_df['customer_id'].astype(int)

merged_df = pd.merge(sql_df, engagement_data, on='customer_id', how='inner')
print("Merged rows:", len(merged_df))

# -------------------------------
# STEP 5: Engagement Score
# -------------------------------
engagement = merged_df.groupby('customer_id').agg({
    'session_duration_sec': 'sum',
    'sessions': 'sum'
}).reset_index()

engagement.columns = ['customer_id', 'total_duration', 'total_sessions']

engagement['engagement_score'] = (
    engagement['total_sessions'] +
    (engagement['total_duration'] / 100)
)

# -------------------------------
# STEP 6: Hypothesis Test
# -------------------------------
merged_df['churn_flag'] = merged_df['churned_at'].notnull().astype(int)

data = pd.merge(merged_df, engagement, on='customer_id')

churned = data[data['churn_flag'] == 1]['engagement_score']
not_churned = data[data['churn_flag'] == 0]['engagement_score']

t_stat, p_value = ttest_ind(churned, not_churned, nan_policy='omit')

print("T-test p-value:", p_value)

# -------------------------------
# STEP 7: Segmentation
# -------------------------------
engagement['segment'] = pd.qcut(
    engagement['engagement_score'],
    3,
    labels=['Low', 'Medium', 'High']
)

print(engagement.groupby('segment').size())

# -------------------------------
# STEP 8: Final Merge + Export
# -------------------------------
final_df = pd.merge(data, engagement[['customer_id', 'segment']], on='customer_id')

final_df.to_csv("final_data.csv", index=False)

print("CSV file created successfully!")