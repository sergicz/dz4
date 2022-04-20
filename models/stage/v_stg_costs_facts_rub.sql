{{
    config(
        enabled=True
    )
}}

WITH staging AS (
	select gcf.id as id, 						-- ИД расхода
		
		-- измерения
		gcf.account_id as account_id, 			-- аккаунт?
		gcf.dates_id as dates_id, 				-- даты
		gcf.sites_id as sites_id, 				-- сайт?
		gcf.traffic_id as traffic_id, 			-- траффик?
		multiIf(gcf.account_id in (33206,33207,30596,30352),'yandexdirect',gcf.account_id in (30246,30610,32296,33162,33203,35748),'fb',gcf.account_id in (30595),'google',gcf.account_id in (30351,30925),'vk','other') as source,   -- соурс
		gt.medium as medium,					-- медиум
		left(substr(gt.content,position(gt.content,'|cid|')+5),position(substr(gt.content,position(gt.content,'|cid|')+5),'|')-1) as cid, -- сид 
		
		-- метрики
		gcf.impressions as impressions, 		-- просмотры
		gcf.clicks as clicks, 					-- клики
		gcf.vat_included as vat_included, 		-- налог?
		gcf.cost as cost, 						-- стоимость?
		if(gcf.account_id={{var('fb_account_id')}},round(gcf.cost * COALESCE(cif.rate,1),2),gcf.cost) as cost_rub -- стоимость в рублях
	from {{ source('src','general_costs_facts') }} gcf
	left join {{ source('src','currency_items_facts') }} cif 
		on gcf.dates_id=cif.dates_id and cif.items_id={{var('usd_id')}}
	left join {{ source('src','general_traffic') }} gt 
		on gcf.traffic_id=gt.id
)

SELECT * FROM staging
