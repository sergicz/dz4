{{
    config(
        enabled=True
    )
}}

--utm_source
with u_source as (
select
	ala.leads_id,
	max(ala.value) as u_source
from
	{{ source('src','amocrm_leads_attributes') }} ala
where ala.name = 'utm_source'
group by
	ala.leads_id),
--utm_medium
u_medium as (
select
	ala.leads_id,
	max(ala.value) as u_medium
from
	{{ source('src','amocrm_leads_attributes') }} ala
where ala.name = 'utm_medium'
group by
	ala.leads_id),
--utm_content
u_content as (
select
	ala.leads_id,
	max(ala.value) as u_content
from
	{{ source('src','amocrm_leads_attributes') }} ala
where ala.name = 'utm_content'
group by
	ala.leads_id),
--уникальные лиды из аттрибутов
u_main as (
select distinct ala.leads_id 
from {{ source('src','amocrm_leads_attributes') }} ala),
--лиды с атрибутами
la as (
select u_main.leads_id as leads_id, u_source, u_medium, u_content from u_main 
left join u_source on u_main.leads_id = u_source.leads_id
left join u_medium on u_main.leads_id = u_medium.leads_id 
left join u_content on u_main.leads_id = u_content.leads_id
where COALESCE(u_source, u_medium, u_content) is not null),
--лиды с атрибутами, статусом, датой завершения, стоимостью, утм-метками
ltot as (
select 
	al.id as id, 				-- ИД лида
	al.status as status, 		-- статус лида
	al.pipeline as pipeline,	-- пайплайн лида
	la.u_source as u_source,	-- исходный соурс?
	multiIf(la.u_source in ('yandexdirect'),'yandexdirect',la.u_source in ('Facebook ADS','fb','facebook+ads'),'fb',la.u_source in ('google'),'google',la.u_source in ('vk','vk_ads','vkontakte','wggvk'),'vk','other') as source,	-- соурс лида
	la.u_medium as medium, 		-- медиум лида
	la.u_content as content, 	-- контент лида
	alf.created_id as created_id,-- дата создания лида?
	alf.closed_id as closed_id,	-- дата завершения лида
	alf.price as amount, 	 	-- сумма лида
	left(substr(la.u_content,position(la.u_content,'|cid|')+5),position(substr(la.u_content,position(la.u_content,'|cid|')+5),'|')-1) as cid -- сид лида
from {{source('src','amocrm_leads')}} al 
left join la on al.id = la.leads_id
left join {{ source('src','amocrm_leads_facts') }} alf on alf.leads_id = al.id)

select * from ltot
 