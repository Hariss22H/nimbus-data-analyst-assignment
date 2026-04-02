-- Q1
SELECT
    p.plan_name,
    p.plan_tier,
    COUNT(DISTINCT s.customer_id) AS active_customers,
    ROUND(AVG(s.mrr_usd), 2) AS avg_mrr_usd,
    ROUND(
        COUNT(st.ticket_id)::NUMERIC
        / NULLIF(COUNT(DISTINCT s.customer_id), 0)
        / 6.0,2
    ) AS ticket_rate_per_customer_per_month
FROM subscriptions s
JOIN plans p
    ON s.plan_id = p.plan_id
LEFT JOIN support_tickets st
    ON st.customer_id = s.customer_id
    AND st.created_at >= NOW() - INTERVAL '6 months'
WHERE s.start_date <= NOW()
AND (s.end_date IS NULL OR s.end_date >= NOW() - INTERVAL '6 months')
GROUP BY
    p.plan_id,
    p.plan_name,
    p.plan_tier
ORDER BY
    avg_mrr_usd DESC;


-- Q2
WITH customer_ltv AS (
    SELECT
        c.customer_id,
        c.company_name,
        p.plan_tier,
        COALESCE(SUM(bi.total_usd), 0) AS total_ltv
    FROM customers c
    JOIN subscriptions s
        ON s.customer_id = c.customer_id
        AND s.subscription_id = (
            SELECT subscription_id
            FROM subscriptions s2
            WHERE s2.customer_id = c.customer_id
            ORDER BY s2.start_date DESC
            LIMIT 1
        )
    JOIN plans p
        ON s.plan_id = p.plan_id
    LEFT JOIN billing_invoices bi
        ON bi.customer_id = c.customer_id
        AND bi.status = 'paid'
    GROUP BY
        c.customer_id,
        c.company_name,
        p.plan_tier
),
tier_stats AS (
    SELECT
        plan_tier,
        AVG(total_ltv) AS avg_tier_ltv
    FROM customer_ltv
    GROUP BY plan_tier
)
SELECT
    cl.customer_id,
    cl.company_name,
    cl.plan_tier,
    ROUND(cl.total_ltv, 2) AS total_ltv_usd,
    RANK() OVER (
        PARTITION BY cl.plan_tier
        ORDER BY cl.total_ltv DESC
    ) AS rank_in_tier,
    ROUND(ts.avg_tier_ltv, 2) AS tier_avg_ltv_usd,
    ROUND(
        (cl.total_ltv - ts.avg_tier_ltv)
        / NULLIF(ts.avg_tier_ltv, 0) * 100,
        2
    ) AS pct_diff_from_tier_avg
FROM customer_ltv cl
JOIN tier_stats ts
    ON cl.plan_tier = ts.plan_tier
ORDER BY
    cl.plan_tier,
    rank_in_tier;

--Q3
WITH downgrades AS (
    SELECT
        s_old.customer_id,
        s_new.start_date AS downgrade_date,
        p_old.plan_id AS prev_plan_id,
        p_old.plan_name AS prev_plan_name,
        p_old.plan_tier AS prev_plan_tier,
        p_old.monthly_price_usd AS prev_plan_price,
        p_new.plan_id AS curr_plan_id,
        p_new.plan_name AS curr_plan_name,
        p_new.plan_tier AS curr_plan_tier,
        p_new.monthly_price_usd AS curr_plan_price
    FROM subscriptions s_old
    JOIN subscriptions s_new
        ON s_new.customer_id = s_old.customer_id
        AND s_new.start_date >= s_old.end_date
    JOIN plans p_old ON s_old.plan_id = p_old.plan_id
    JOIN plans p_new ON s_new.plan_id = p_new.plan_id
    WHERE
        s_new.start_date >= CURRENT_DATE - INTERVAL '90 days'
        AND p_new.monthly_price_usd < p_old.monthly_price_usd
),
ticket_counts AS (
    SELECT
        st.customer_id,
        d.downgrade_date,
        COUNT(st.ticket_id) AS tickets_before_downgrade
    FROM support_tickets st
    JOIN downgrades d
        ON st.customer_id = d.customer_id
        AND st.created_at >= (d.downgrade_date::TIMESTAMP - INTERVAL '30 days')
        AND st.created_at < d.downgrade_date::TIMESTAMP
    GROUP BY
        st.customer_id,
        d.downgrade_date
)
SELECT
    d.customer_id,
    c.company_name,
    c.contact_email,
    c.industry,
    c.company_size,
    TO_CHAR(d.downgrade_date, 'YYYY-MM-DD') AS downgrade_date,
    d.prev_plan_name,
    d.prev_plan_tier,
    ROUND(d.prev_plan_price, 2) AS prev_monthly_price_usd,
    d.curr_plan_name,
    d.curr_plan_tier,
    ROUND(d.curr_plan_price, 2) AS curr_monthly_price_usd,
    ROUND(d.prev_plan_price - d.curr_plan_price, 2) AS monthly_revenue_lost_usd,
    tc.tickets_before_downgrade
FROM downgrades d
JOIN ticket_counts tc
    ON d.customer_id = tc.customer_id
    AND d.downgrade_date = tc.downgrade_date
JOIN customers c
    ON d.customer_id = c.customer_id
WHERE tc.tickets_before_downgrade > 3
ORDER BY
    tc.tickets_before_downgrade DESC,
    monthly_revenue_lost_usd DESC;

--Q4
WITH monthly_new AS (
    SELECT
        p.plan_tier,
        DATE_TRUNC('month', s.start_date) AS month,
        COUNT(*) AS new_subscriptions
    FROM subscriptions s
    JOIN plans p ON s.plan_id = p.plan_id
    GROUP BY
        p.plan_tier,
        DATE_TRUNC('month', s.start_date)
),
monthly_churn AS (
    SELECT
        p.plan_tier,
        DATE_TRUNC('month', s.end_date) AS month,
        COUNT(*) AS churned_subscriptions
    FROM subscriptions s
    JOIN plans p ON s.plan_id = p.plan_id
    WHERE
        s.status = 'cancelled'
        AND s.end_date IS NOT NULL
    GROUP BY
        p.plan_tier,
        DATE_TRUNC('month', s.end_date)
),
combined AS (
    SELECT
        COALESCE(mn.plan_tier, mc.plan_tier) AS plan_tier,
        COALESCE(mn.month, mc.month) AS month,
        COALESCE(mn.new_subscriptions, 0) AS new_subscriptions,
        COALESCE(mc.churned_subscriptions, 0) AS churned_subscriptions
    FROM monthly_new mn
    FULL OUTER JOIN monthly_churn mc
        ON mn.plan_tier = mc.plan_tier
        AND mn.month = mc.month
),
metrics AS (
    SELECT
        plan_tier,
        month,
        new_subscriptions,
        churned_subscriptions,
        ROUND(
            (
                new_subscriptions
                - LAG(new_subscriptions, 1, 0) OVER w
            )::NUMERIC
            / NULLIF(LAG(new_subscriptions, 1, 0) OVER w, 0) * 100,
            2
        ) AS mom_growth_rate_pct,
        ROUND(
            churned_subscriptions::NUMERIC
            / NULLIF(new_subscriptions + churned_subscriptions, 0) * 100,
            2
        ) AS churn_rate_pct,
        ROUND(
            AVG(
                churned_subscriptions::NUMERIC
                / NULLIF(new_subscriptions + churned_subscriptions, 0) * 100
            ) OVER (
                PARTITION BY plan_tier
                ORDER BY month
                ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
            ),
            2
        ) AS rolling_3m_avg_churn_pct
    FROM combined
    WINDOW w AS (PARTITION BY plan_tier ORDER BY month)
)
SELECT
    plan_tier,
    TO_CHAR(month, 'YYYY-MM') AS month,
    new_subscriptions,
    churned_subscriptions,
    mom_growth_rate_pct,
    churn_rate_pct,
    rolling_3m_avg_churn_pct,
    CASE
        WHEN churn_rate_pct > 2.0 * rolling_3m_avg_churn_pct
        THEN 'CHURN SPIKE'
        ELSE 'Normal'
    END AS churn_spike_flag
FROM metrics
WHERE new_subscriptions > 0
   OR churned_subscriptions > 0
ORDER BY
    plan_tier,
    month;

--Q5
WITH
name_pairs AS (
    SELECT
        c1.customer_id AS cust_id_1,
        c2.customer_id AS cust_id_2,
        SIMILARITY(
            LOWER(TRIM(c1.company_name)),
            LOWER(TRIM(c2.company_name))
        ) AS name_score
    FROM customers c1
    JOIN customers c2
        ON c1.customer_id < c2.customer_id
    WHERE
        SIMILARITY(
            LOWER(TRIM(c1.company_name)),
            LOWER(TRIM(c2.company_name))
        ) >= 0.55
),
domain_pairs AS (
    SELECT
        c1.customer_id AS cust_id_1,
        c2.customer_id AS cust_id_2,
        SPLIT_PART(LOWER(c1.contact_email), '@', 2) AS shared_domain
    FROM customers c1
    JOIN customers c2
        ON c1.customer_id < c2.customer_id
        AND LOWER(SPLIT_PART(c1.contact_email, '@', 2))
         = LOWER(SPLIT_PART(c2.contact_email, '@', 2))
    WHERE
        SPLIT_PART(LOWER(c1.contact_email), '@', 2) NOT IN (
            'gmail.com', 'yahoo.com', 'hotmail.com',
            'outlook.com', 'protonmail.com', 'icloud.com',
            'startup.co', 'corp.net', 'company.io',
            'enterprise.com', 'tech.dev'
        )
),
shared_members AS (
    SELECT
        tm1.customer_id AS cust_id_1,
        tm2.customer_id AS cust_id_2,
        COUNT(*) AS shared_member_count
    FROM team_members tm1
    JOIN team_members tm2
        ON tm1.customer_id < tm2.customer_id
        AND LOWER(
                REGEXP_REPLACE(TRIM(tm1.email), '\+[^@]+', '')
            )
         = LOWER(
                REGEXP_REPLACE(TRIM(tm2.email), '\+[^@]+', '')
            )
    GROUP BY
        tm1.customer_id,
        tm2.customer_id
),
all_pairs AS (
    SELECT
        COALESCE(np.cust_id_1, dp.cust_id_1, sm.cust_id_1) AS cust_id_1,
        COALESCE(np.cust_id_2, dp.cust_id_2, sm.cust_id_2) AS cust_id_2,
        np.name_score,
        dp.shared_domain,
        sm.shared_member_count,
        (
            CASE WHEN np.name_score IS NOT NULL THEN 1 ELSE 0 END
          + CASE WHEN dp.shared_domain IS NOT NULL THEN 1 ELSE 0 END
          + CASE WHEN sm.shared_member_count IS NOT NULL THEN 1 ELSE 0 END
        ) AS signals_matched
    FROM name_pairs np
    FULL OUTER JOIN domain_pairs dp
        ON np.cust_id_1 = dp.cust_id_1
        AND np.cust_id_2 = dp.cust_id_2
    FULL OUTER JOIN shared_members sm
        ON COALESCE(np.cust_id_1, dp.cust_id_1) = sm.cust_id_1
        AND COALESCE(np.cust_id_2, dp.cust_id_2) = sm.cust_id_2
)
SELECT
    ap.cust_id_1 AS customer_id_1,
    c1.company_name AS company_1,
    c1.contact_email AS email_1,
    ap.cust_id_2 AS customer_id_2,
    c2.company_name AS company_2,
    c2.contact_email AS email_2,
    CASE WHEN ap.name_score IS NOT NULL THEN 'YES' ELSE 'NO' END AS name_similar,
    CASE WHEN ap.shared_domain IS NOT NULL THEN 'YES' ELSE 'NO' END AS same_domain,
    CASE WHEN ap.shared_member_count IS NOT NULL THEN 'YES' ELSE 'NO' END AS shared_team_member,
    ROUND(COALESCE(ap.name_score, 0)::NUMERIC, 2) AS name_similarity_score,
    COALESCE(ap.shared_domain, 'N/A') AS matched_domain,
    COALESCE(ap.shared_member_count, 0) AS shared_member_count,
    ap.signals_matched,
    CASE ap.signals_matched
        WHEN 3 THEN 'HIGH — likely same company'
        WHEN 2 THEN 'MEDIUM — investigate'
        ELSE 'LOW'
    END AS duplicate_confidence
FROM all_pairs ap
JOIN customers c1 ON c1.customer_id = ap.cust_id_1
JOIN customers c2 ON c2.customer_id = ap.cust_id_2
WHERE ap.signals_matched >= 2
ORDER BY
    ap.signals_matched DESC,
    ap.name_score DESC NULLS LAST;