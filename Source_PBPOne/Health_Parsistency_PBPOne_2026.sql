
with t1 as(
select
PartnerCode,
CalendarYear as Year,
case 
	 when Quarter = 'Q1' then 'Q1-JFM'
	 when Quarter = 'Q2' then 'Q2-AMJ'
	 when Quarter = 'Q3' then 'Q3-JAS'
	 when Quarter = 'Q4' then 'Q4-OND'
END as Quarter,
Qtr_Renewal_Due, Qtr_Renewal_Done,
Annual_Renewal_Due as Yearly_Renewal_Due, Annual_Renewal_Done as Yearly_Renewal_Done,
case when Qtr_Health_Persistency >1 then Qtr_Renewal_Done/Qtr_Renewal_Due else Qtr_Health_Persistency end as Qtr_Health_Persistency, 
case when Annual_Health_Persistency >1 then Annual_Renewal_Done/Annual_Renewal_Due else  Annual_Health_Persistency end as Annual_Health_Persistency,
CreatedOn, UpdatedOn

from PBPOneDB.dbo.PbpPartnerHealthPersistency nolock
where CalendarYear =2026
and IsActive =1

),
t2 as
(
select *,
case when Qtr_Health_Persistency >= .9 then 
		  case when Quarter = 'Q4-OND' then .40 else .75 end
	 when Qtr_Health_Persistency >= .8 then
		  case when Quarter = 'Q4-OND' then .35 else .65 end
	 when Qtr_Health_Persistency >= .7 then 
		  case when Quarter = 'Q4-OND' then .30 else .60 end
	 when Qtr_Health_Persistency >= .6 then 
		  case when Quarter = 'Q4-OND' then .25 else .55 end
	 else 0
	 end as Qtr_Health_Persistency_Multiplier,

case 
	 when Annual_Renewal_due < 5 then 1
	 when Annual_Health_Persistency >= .9 then
		  case when Quarter = 'Q4-OND' then .40 else .75 end
	 when Annual_Health_Persistency >= .8 then
		  case when Quarter = 'Q4-OND' then .35 else .65 end
	 when Annual_Health_Persistency >= .7 then
		  case when Quarter = 'Q4-OND' then .30 else .60 end
	 when Annual_Health_Persistency >= .6 then
		  case when Quarter = 'Q4-OND' then .25 else .55 end
	 else 0
	 end as Annual_Health_Persistency_Multiplier,
	 'Health' as product_name, 'Renewal' as Health_bt,
row_number() over(Partition by Year, PartnerCode order by Year,PartnerCode, Quarter) as rnk
from t1
)
select *
--case when Quarter <> 'Q4-OND' THEN Annual_Health_Persistency_Multiplier END AS Annual_Health_Persistency_Multiplier_upto_JAS,
--case when Quarter = 'Q4-OND' THEN Annual_Health_Persistency_Multiplier END AS Annual_Health_Persistency_Multiplier_OND
from t2
WHERE 1=1
-- CONDITION_PLACEHOLDER
