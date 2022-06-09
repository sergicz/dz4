{{
    config(
        enabled=True
    )
}}

WITH staging AS (
	select -- приводим типы из строк к правильным + заполняем доп.поля
		MD5(coalesce(b24.ID, '')) as skey,												-- Surrogate key
		toUInt64OrZero(b24.ID) as id, 													-- ИД
		toUInt64OrZero(b24.PORTAL_USER_ID) as puser_id, 								-- юзер ИД
		replaceOne(b24.PHONE_NUMBER,'+7','7') as phoneplus7,							-- избавились от +7
		if(left(phoneplus7,1)=='8',concat('7',substring(phoneplus7,2,3)),phoneplus7) as phone7, -- избавились от 8 
		multiIf(
			length(phone7)<=3,'Внутренний',
			length(phone7)==7,'Городской',
			left(phone7,1)=='9','Сотовый',
			left(phone7,2)=='79','Сотовый', 
			left(phone7,4)=='7343','Городской',			
			left(phone7,1)=='7','Междугородный',			
			'Международный'
			) as type_phone, 															-- тип абонента (внутренний, сотовый, городской, междугородный, международный)
		toUInt64OrZero(multiIf(
			length(phone7)<=3,'0',
			length(phone7)==7,'7343',
			left(phone7,2)=='9','0',
			left(phone7,2)=='79','0', 
			left(phone7,1)=='7',left(phone7,4),			
			'0'
			)) as phone_code, 															-- телефонный код региона
		toUInt64OrZero(b24.CALL_DURATION) as duration, 									-- длительность звонка
		toDateTime(left(b24.CALL_START_DATE,19),'Asia/Yekaterinburg') as date_yekat, 	-- дата-время звонка
		If(b24.CALL_FAILED_CODE=='200','Успешный','Неуспешный') as res,   				-- результат
		If(b24.CALL_TYPE=='2','Входящий','Исходящий') as type_call						-- тип звонка вх/исх
	from {{source('src','b24')}} b24 
	where id > {{var('beg_id')}} -- для отладки
)

SELECT skey, id, puser_id, type_phone, phone_code, duration, date_yekat, res, type_call, {{ var('load_date') }} AS load_date FROM staging
