{{
    config(
        enabled=True
    )
}}

-- {% set dPIT = "'02-19-2022 23:59:59.999 +0500'" %}

WITH bi AS (
	select 
		c.id as cost_id,				-- ИД
		null as lead_id,
		c.dates_id as created_id,		-- дата создания
		c.dates_id as dates_id, 		-- дата закрытия
		null as status,
		null as pipeline,
		c.source as source,   			-- соурс
		c.medium as medium,				-- медиум
		c.cid as cid, 					-- сид 
		c.impressions as impressions, 	-- просмотры
		c.clicks as clicks,		 		-- клики
		c.cost_rub as cost_rub, 		-- стоимость
		null as amount
	from v_stg_costs_facts_rub c
	union all
	select 
		null as cost_id,
		l.id as lead_id,			-- ИД лида
		l.created_id as created_id,	-- дата создания
		l.closed_id as dates_id,	-- дата закрытия
		l.status as ststus,			-- статус лида
		l.pipeline as pipeline,		-- пайплайн лида
		l.source as source,			-- соурс
		l.medium as medium,			-- медиум
		l.cid as cid,				-- сид
		null as impressions,	
		null as clicks, 		
		null as cost_rub, 		
		l.amount as amount			-- сумма лида
	from v_stg_amocrm_leads_attr l
)
select 
	bi.cost_id as cost_id,			-- кост ИД
	bi.lead_id as lead_id,			-- лид ИД
	bi.status as status,			-- статус
	bi.pipeline as pipeline,		-- пйплайн
	bi.source as source,   			-- соурс
	bi.medium as medium,			-- медиум
	bi.cid as cid, 					-- сид 
	COALESCE(bi.impressions,0) as impressions, 	-- просмотры
	COALESCE(bi.clicks,0) as clicks,		 	-- клики
	COALESCE(bi.cost_rub,0) as cost_rub, 		-- расход
	COALESCE(bi.amount,0) as amount,			-- приход
	gd1.dt as date1, 				-- дата создания
	gd2.dt as date2  				-- дата закрытия
from bi 		
left join {{ source('src','general_dates') }} gd1 on bi.created_id=gd1.id
left join {{ source('src','general_dates') }} gd2 on bi.dates_id=gd2.id
