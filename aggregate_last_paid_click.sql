WITH last_payment_rn AS (
    SELECT
        s.visitor_id,
        s.visit_date,
        s.source AS utm_source,
        s.medium AS utm_medium,
        s.campaign AS utm_campaign,
        l.lead_id,
        l.created_at,
        l.amount,
        l.closing_reason,
        l.status_id,
        ROW_NUMBER()
            OVER (
                PARTITION BY s.visitor_id
                ORDER BY s.visit_date DESC
            )
        AS rn
    FROM sessions AS s
    LEFT JOIN
        leads AS l
        ON s.visitor_id = l.visitor_id AND s.visit_date <= l.created_at
    WHERE medium != 'organic'
),

last_payment_filtred AS (
    SELECT
        visitor_id,
        visit_date::date,
        utm_source,
        utm_medium,
        utm_campaign,
        lead_id,
        created_at,
        amount,
        closing_reason,
        status_id
    FROM last_payment_rn
    WHERE rn = 1
),

ads AS (
    SELECT
        campaign_date::DATE,
        utm_source,
        utm_medium,
        utm_campaign,
        SUM(daily_spent) AS total_cost
    FROM vk_ads
    GROUP BY campaign_date::DATE, utm_source, utm_medium, utm_campaign

    UNION ALL

    SELECT
        campaign_date::date,
        utm_source,
        utm_medium,
        utm_campaign,
        SUM(daily_spent) AS total_cost
    FROM ya_ads
    GROUP BY campaign_date::DATE, utm_source, utm_medium, utm_campaign
)

SELECT
    lp.visit_date,
    lp.utm_source,
    lp.utm_medium,
    lp.utm_campaign,
    COUNT(DISTINCT lp.visitor_id) AS visitors_count,
    COALESCE(a.total_cost, 0) AS total_cost,
    COUNT(lp.lead_id) AS leads_count,
    COUNT(DISTINCT lp.visitor_id) FILTER (
        WHERE lp.closing_reason = 'Успешная продажа' OR lp.status_id = '142'
    ) AS purchases_count,
    SUM(lp.amount) FILTER (
        WHERE lp.closing_reason = 'Успешная продажа' OR lp.status_id = '142'
    ) AS revenue
FROM last_payment_filtred AS lp
LEFT JOIN ads AS a
    ON
        lp.utm_source = a.utm_source
        AND lp.utm_medium = a.utm_medium
        AND lp.utm_campaign = a.utm_campaign
        AND lp.visit_date = a.campaign_date
GROUP BY
    lp.visit_date,
    lp.utm_source,
    lp.utm_medium,
    lp.utm_campaign,
    a.total_cost
ORDER BY
    revenue DESC NULLS LAST, visit_date, visitors_count DESC, utm_source, utm_medium, utm_campaign;
