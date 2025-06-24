WITH last_payment_rn AS (
    SELECT
        s.visitor_id,
        s.visit_date,
        s.source,
        s.medium,
        s.campaign,
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
    INNER JOIN
        leads AS l
        ON s.visitor_id = l.visitor_id AND s.visit_date <= l.created_at
    WHERE medium != 'organic'
)

SELECT
    visitor_id,
    visit_date,
    source AS utm_source,
    medium AS utm_medium,
    campaign AS utm_campaign,
    lead_id,
    created_at,
    amount,
    closing_reason,
    status_id
FROM last_payment_rn
WHERE rn = 1
ORDER BY
    amount DESC NULLS LAST, visit_date ASC, utm_source ASC, utm_medium ASC, utm_campaign ASC;
