{{
    config(
        enabled=True,
		materialized='incremental',
		incremental_strategy='insert_overwrite',
		unique_key='id'
    )
}}

select 
	bi.skey as skey,				-- суррогатный ключ
	bi.id as id,					-- ид
	bi.type_phone as type_phone,	-- тип абонента
	bi.phone_code as phone_code,	-- код региона
	bi.duration as duration,		-- длительность
	bi.date_yekat as call_date, 	-- дата звонка
	bi.res as res,					-- результат
	bi.type_call as type_call,		-- тип
	r.REGION as region,				-- регион
	r.COORDINATES as coordinates,	-- геополигон региона
	u.FIO as fio,					-- ФИО
	u.DEPARTMENT as department,		-- отдел
	u.INT_PHONE as int_phone,  		-- вн.телефон юзера
	d.GROUP as group,				-- группа подраздалений
	bi.load_date as load_date
from {{ ref('v_stg_b24') }} bi		
left join {{ source('src','regions') }} r on bi.phone_code == r.PHONE_CODE
left join {{ source('src','b24users') }} u on bi.puser_id == u.PORTAL_USER_ID
left join {{ source('src','b24departments') }} d on u.DEPARTMENT == d.DEPARTMENT
{% if is_incremental() %}
  -- this filter will only be applied on an incremental run
  where id >= (select max(id) from {{ this }})
{% endif %}