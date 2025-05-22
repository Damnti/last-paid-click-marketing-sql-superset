with last_payment_rn as (
    select
        s.visitor_id,
        s.visit_date,
        s.source as utm_source,
        s.medium as utm_medium,
        s.campaign as utm_campaign,
        l.lead_id,
        l.created_at,
        l.amount,
        l.closing_reason,
        l.status_id,
        row_number()
            over (
                partition by s.visitor_id
                order by s.visit_date desc
            )
        as rn
    from sessions as s
    left join
        leads as l
        on s.visitor_id = l.visitor_id and s.visit_date <= l.created_at
    where medium != 'organic'
),

last_payment_filtred as (
    select
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
    from last_payment_rn
    where rn = 1
),

ads as (
    select
        campaign_date::date,
        utm_source,
        utm_medium,
        utm_campaign,
        sum(daily_spent) as total_cost
    from vk_ads
    group by campaign_date::date, utm_source, utm_medium, utm_campaign

    union all

    select
        campaign_date::date,
        utm_source,
        utm_medium,
        utm_campaign,
        sum(daily_spent) as total_cost
    from ya_ads
    group by campaign_date::date, utm_source, utm_medium, utm_campaign
)

select
    lp.visit_date,
    lp.utm_source,
    lp.utm_medium,
    lp.utm_campaign,
    coalesce(a.total_cost, 0),
    count(distinct lp.visitor_id) as visitors_count,
    count(lp.lead_id) as leads_count,
    count(distinct lp.visitor_id) filter (
        where lp.closing_reason = 'Успешная продажа' or lp.status_id = '142'
    ) as purchases_count,
    sum(lp.amount) filter (
        where lp.closing_reason = 'Успешная продажа' or lp.status_id = '142'
    ) as revenue
from last_payment_filtred as lp
left join ads as a
    on
        lp.utm_source = a.utm_source
        and lp.utm_medium = a.utm_medium
        and lp.utm_campaign = a.utm_campaign
        and lp.visit_date = a.campaign_date
group by
    lp.visit_date,
    lp.utm_source,
    lp.utm_medium,
    lp.utm_campaign,
    a.total_cost
order by
    revenue desc nulls last, visit_date asc, visitors_count desc, utm_source asc, utm_medium asc, utm_campaign asc;