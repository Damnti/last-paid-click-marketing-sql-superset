-- Визиты на сайт
SELECT COUNT(DISTINCT visitor_id) AS visitors_count
FROM sessions;


-- Общие расходы
SELECT SUM(daily_spent) AS total_spent
FROM (
    SELECT daily_spent
    FROM vk_ads

    UNION ALL

    SELECT daily_spent
    FROM ya_ads
) AS total;


-- Лиды
SELECT COUNT(lead_id) AS leads_count
FROM leads;


-- Успешные лиды
SELECT COUNT(lead_id) AS leads_count
FROM leads
WHERE closing_reason = 'Успешная продажа' OR status_id = '142';


-- Расчет общих метрик
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
    WHERE s.medium != 'organic'
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
        campaign_date::date,
        utm_source,
        utm_medium,
        utm_campaign,
        SUM(daily_spent) AS total_cost
    FROM vk_ads
    GROUP BY campaign_date::date, utm_source, utm_medium, utm_campaign

    UNION ALL

    SELECT
        campaign_date::date,
        utm_source,
        utm_medium,
        utm_campaign,
        SUM(daily_spent) AS total_cost
    FROM ya_ads
    GROUP BY campaign_date::date, utm_source, utm_medium, utm_campaign
),

agg_lpc AS (
    SELECT
        visit_date,
        utm_source,
        utm_medium,
        utm_campaign,
        COUNT(DISTINCT visitor_id) AS visitors_count,
        COUNT(lead_id) AS leads_count,
        COUNT(DISTINCT visitor_id) FILTER (
            WHERE closing_reason = 'Успешная продажа' OR status_id = '142'
        ) AS purchases_count,
        SUM(amount) FILTER (
            WHERE closing_reason = 'Успешная продажа' OR status_id = '142'
        ) AS revenue
    FROM last_payment_filtred
    GROUP BY
        visit_date,
        utm_source,
        utm_medium,
        utm_campaign
)

SELECT
    ROUND((SUM(a.total_cost) / NULLIF(SUM(l.visitors_count), 0)), 2) AS cpu,
    ROUND(SUM(a.total_cost) / NULLIF(SUM(l.leads_count), 0), 2) AS cpl,
    ROUND((SUM(a.total_cost) / NULLIF(SUM(l.purchases_count), 0)), 2) AS cppu,
    ROUND(
        (
            (
                (SUM(l.revenue) - SUM(a.total_cost))
                / NULLIF(SUM(a.total_cost), 0)
            )
        ),
        2
    ) AS roi
FROM agg_lpc AS l
LEFT JOIN ads AS a
    ON
        l.utm_source = a.utm_source
        AND l.utm_medium = a.utm_medium
        AND l.utm_campaign = a.utm_campaign
        AND l.visit_date = a.campaign_date
WHERE a.utm_source IS NOT NULL;


-- Каналы переходов
SELECT
    source AS utm_source,
    medium AS utm_medium,
    campaign AS utm_campaign,
    visit_date::date AS date,
    COUNT(DISTINCT visitor_id) AS unique_visitors
FROM sessions
GROUP BY source, medium, campaign, visit_date::date;


-- Конверсия из клика в лид и из лида в оплату
WITH visits AS (
    SELECT COUNT(DISTINCT visitor_id) AS visitors
    FROM sessions
),

succ_lead AS (
    SELECT COUNT(lead_id) AS s_lead
    FROM leads
    WHERE closing_reason = 'Успешная продажа' OR status_id = '142'
),

lead AS (
    SELECT COUNT(lead_id) AS leads
    FROM leads
)

SELECT
    'Клик → Лид' AS conversion_type,
    l.leads::float / v.visitors AS conversion
FROM visits AS v
CROSS JOIN lead AS l

UNION ALL

SELECT
    'Лид → Продажа' AS conversion_type,
    sl.s_lead::float / ld.leads AS conversion
FROM lead AS ld
CROSS JOIN succ_lead AS sl;


-- Расходы
SELECT
    campaign_date::date AS date,
    utm_source,
    utm_medium,
    utm_campaign,
    SUM(daily_spent) AS daily_spent
FROM vk_ads
GROUP BY
    campaign_date,
    utm_source,
    utm_medium,
    utm_campaign

UNION ALL

SELECT
    campaign_date::date,
    utm_source,
    utm_medium,
    utm_campaign,
    SUM(daily_spent) AS daily_spent
FROM ya_ads
GROUP BY
    campaign_date,
    utm_source,
    utm_medium,
    utm_campaign;


-- Доходы
SELECT
    l.created_at::date AS date,
    s.source AS utm_source,
    s.medium AS utm_medium,
    SUM(l.amount)
FROM leads AS l
LEFT JOIN sessions AS s
    ON
        l.visitor_id = s.visitor_id
WHERE s.source IN ('vk', 'yandex')
GROUP BY l.created_at::date, s.source, s.medium;


-- Расчет метрик
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
    WHERE s.medium != 'organic'
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
        campaign_date::date,
        utm_source,
        utm_medium,
        utm_campaign,
        SUM(daily_spent) AS total_cost
    FROM vk_ads
    GROUP BY campaign_date::date, utm_source, utm_medium, utm_campaign

    UNION ALL

    SELECT
        campaign_date::date,
        utm_source,
        utm_medium,
        utm_campaign,
        SUM(daily_spent) AS total_cost
    FROM ya_ads
    GROUP BY campaign_date::date, utm_source, utm_medium, utm_campaign
),

agg_lpc AS (
    SELECT
        visit_date,
        utm_source,
        utm_medium,
        utm_campaign,
        COUNT(DISTINCT visitor_id) AS visitors_count,
        COUNT(lead_id) AS leads_count,
        COUNT(DISTINCT visitor_id) FILTER (
            WHERE closing_reason = 'Успешная продажа' OR status_id = '142'
        ) AS purchases_count,
        SUM(amount) FILTER (
            WHERE closing_reason = 'Успешная продажа' OR status_id = '142'
        ) AS revenue
    FROM last_payment_filtred
    GROUP BY
        visit_date,
        utm_source,
        utm_medium,
        utm_campaign
)

SELECT
    a.utm_source,
    ROUND((SUM(a.total_cost) / NULLIF(SUM(l.visitors_count), 0)), 2) AS cpu,
    ROUND(SUM(a.total_cost) / NULLIF(SUM(l.leads_count), 0), 2) AS cpl,
    ROUND((SUM(a.total_cost) / NULLIF(SUM(l.purchases_count), 0)), 2) AS cppu,
    ROUND(
        (
            (
                (SUM(l.revenue) - SUM(a.total_cost))
                / NULLIF(SUM(a.total_cost), 0)
            )
            * 100
        ),
        2
    ) AS roi
FROM agg_lpc AS l
LEFT JOIN ads AS a
    ON
        l.utm_source = a.utm_source
        AND l.utm_medium = a.utm_medium
        AND l.utm_campaign = a.utm_campaign
        AND l.visit_date = a.campaign_date
WHERE a.utm_source IS NOT NULL
GROUP BY a.utm_source;


-- За сколько дней с момента перехода по рекламе закрывается 90% лидов
SELECT
    PERCENTILE_CONT(0.9) WITHIN GROUP (
        ORDER BY l.created_at - s.visit_date
    ) AS prcntile_9
FROM sessions AS s
LEFT JOIN leads AS l ON
    s.visitor_id = l.visitor_id
    AND s.visit_date <= l.created_at;
