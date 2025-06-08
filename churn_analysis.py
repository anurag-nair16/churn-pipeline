import mysql.connector
import pandas as pd
from datetime import datetime, timedelta
import os
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

try:
    # Connect to TiDB
    conn = mysql.connector.connect(
        host=os.getenv('TIDB_HOST'),
        user=os.getenv('TIDB_USER'),
        password=os.getenv('TIDB_PASSWORD'),
        database=os.getenv('TIDB_DATABASE'),
        port=4000,
        ssl_ca='tidb-ca.pem',
        ssl_verify_cert=True
    )
    cursor = conn.cursor()

    # Get current date
    analysis_date = datetime.now().date()

    # Fetch current data from churn_pipeline
    cursor.execute("""
        SELECT 
            COUNT(*) as total_customers,
            SUM(CASE WHEN Churn = 'Yes' THEN 1 ELSE 0 END) as churned_customers,
            SUM(CASE WHEN Churn = 'Yes' AND Contract = 'Month-to-month' THEN 1 ELSE 0 END) as churn_month_to_month,
            SUM(CASE WHEN Churn = 'Yes' AND Contract = 'One year' THEN 1 ELSE 0 END) as churn_one_year,
            SUM(CASE WHEN Churn = 'Yes' AND Contract = 'Two year' THEN 1 ELSE 0 END) as churn_two_year,
            SUM(CASE WHEN Churn = 'Yes' AND SeniorCitizen = 1 THEN 1 ELSE 0 END) as churn_senior_citizen,
            SUM(CASE WHEN Churn = 'Yes' AND OnlineSecurity = 'No' THEN 1 ELSE 0 END) as churn_no_online_security,
            SUM(CASE WHEN Churn = 'Yes' AND TechSupport = 'No' THEN 1 ELSE 0 END) as churn_no_tech_support,
            AVG(CASE WHEN Churn = 'Yes' THEN tenure END) as avg_tenure_churned,
            AVG(CASE WHEN Churn = 'No' THEN tenure END) as avg_tenure_retained,
            AVG(CASE WHEN Churn = 'Yes' THEN MonthlyCharges END) as avg_monthly_charges_churned,
            AVG(CASE WHEN Churn = 'No' THEN MonthlyCharges END) as avg_monthly_charges_retained,
            SUM(CASE WHEN Churn = 'Yes' THEN MonthlyCharges ELSE 0 END) as revenue_loss_churn,
            SUM(CASE WHEN Contract = 'Month-to-month' AND TechSupport = 'No' AND tenure < 12 THEN 1 ELSE 0 END) as high_risk_customers,
            SUM(CASE WHEN Churn = 'Yes' AND Contract = 'Month-to-month' AND TechSupport = 'No' AND tenure < 12 THEN 1 ELSE 0 END) as high_risk_churned
        FROM churn_pipeline
    """)
    result = cursor.fetchone()

    total_customers = result[0]
    churned_customers = result[1]
    churn_month_to_month = result[2]
    churn_one_year = result[3]
    churn_two_year = result[4]
    churn_senior_citizen = result[5]
    churn_no_online_security = result[6]
    churn_no_tech_support = result[7]
    avg_tenure_churned = result[8] or 0
    avg_tenure_retained = result[9] or 0
    avg_monthly_charges_churned = result[10] or 0
    avg_monthly_charges_retained = result[11] or 0
    revenue_loss_churn = result[12] or 0
    high_risk_customers = result[13] or 0
    high_risk_churned = result[14] or 0

    # Calculate churn rates
    churn_rate = (churned_customers / total_customers * 100) if total_customers > 0 else 0
    high_risk_churn_rate = (high_risk_churned / high_risk_customers * 100) if high_risk_customers > 0 else 0

    # Fetch previous day's churn rate
    cursor.execute("""
        SELECT churn_rate
        FROM churn_analysis_history
        WHERE analysis_date = %s
    """, (analysis_date - timedelta(days=1),))
    prev_result = cursor.fetchone()
    churn_rate_change = (churn_rate - prev_result[0]) if prev_result else 0

    # Fetch last 7 days' churn rates for moving average
    cursor.execute("""
        SELECT churn_rate
        FROM churn_analysis_history
        WHERE analysis_date >= %s AND analysis_date < %s
        ORDER BY analysis_date DESC
    """, (analysis_date - timedelta(days=6), analysis_date))
    recent_churn_rates = [row[0] for row in cursor.fetchall()]
    churn_rate_7day_avg = sum(recent_churn_rates) / len(recent_churn_rates) if recent_churn_rates else churn_rate

    # Insert or update analysis results
    cursor.execute("""
        INSERT INTO churn_analysis_history (
            analysis_date, total_customers, churned_customers, churn_rate,
            churn_rate_change, churn_rate_7day_avg, churn_month_to_month,
            churn_one_year, churn_two_year, churn_senior_citizen,
            churn_no_online_security, churn_no_tech_support,
            avg_tenure_churned, avg_tenure_retained,
            avg_monthly_charges_churned, avg_monthly_charges_retained,
            revenue_loss_churn, high_risk_customers, high_risk_churn_rate
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            total_customers = %s,
            churned_customers = %s,
            churn_rate = %s,
            churn_rate_change = %s,
            churn_rate_7day_avg = %s,
            churn_month_to_month = %s,
            churn_one_year = %s,
            churn_two_year = %s,
            churn_senior_citizen = %s,
            churn_no_online_security = %s,
            churn_no_tech_support = %s,
            avg_tenure_churned = %s,
            avg_tenure_retained = %s,
            avg_monthly_charges_churned = %s,
            avg_monthly_charges_retained = %s,
            revenue_loss_churn = %s,
            high_risk_customers = %s,
            high_risk_churn_rate = %s
    """, (
        analysis_date, total_customers, churned_customers, round(churn_rate, 2),
        round(churn_rate_change, 2), round(churn_rate_7day_avg, 2),
        churn_month_to_month, churn_one_year, churn_two_year, churn_senior_citizen,
        churn_no_online_security, churn_no_tech_support,
        round(avg_tenure_churned, 2), round(avg_tenure_retained, 2),
        round(avg_monthly_charges_churned, 2), round(avg_monthly_charges_retained, 2),
        round(revenue_loss_churn, 2), high_risk_customers, round(high_risk_churn_rate, 2),
        # Repeated for ON DUPLICATE KEY UPDATE
        total_customers, churned_customers, round(churn_rate, 2),
        round(churn_rate_change, 2), round(churn_rate_7day_avg, 2),
        churn_month_to_month, churn_one_year, churn_two_year, churn_senior_citizen,
        churn_no_online_security, churn_no_tech_support,
        round(avg_tenure_churned, 2), round(avg_tenure_retained, 2),
        round(avg_monthly_charges_churned, 2), round(avg_monthly_charges_retained, 2),
        round(revenue_loss_churn, 2), high_risk_customers, round(high_risk_churn_rate, 2)
    ))

    # Check if the query affected an existing row (update) or created a new one (insert)
    if cursor.rowcount == 1:
        logging.info("✅ Inserted new churn analysis for %s", analysis_date)
    else:
        logging.info("✅ Updated existing churn analysis for %s", analysis_date)

    # Log key insights
    logging.info("Key Insights: Churn Rate: %.2f%%, 7-Day Avg: %.2f%%, Revenue Loss: $%.2f, High-Risk Churn Rate: %.2f%%",
                 churn_rate, churn_rate_7day_avg, revenue_loss_churn, high_risk_churn_rate)

    # Commit changes
    conn.commit()

except mysql.connector.Error as e:
    logging.error("Database error: %s", e)
    raise
except Exception as e:
    logging.error("Unexpected error: %s", e)
    raise
finally:
    cursor.close()
    conn.close()
