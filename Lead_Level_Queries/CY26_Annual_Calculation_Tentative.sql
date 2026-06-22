with cte as
(
select
PartnerCode, 
case when Quarter = 'Q1-JFM' then Tier end as JFM_Club,
case when Quarter = 'Q1-JFM' then WeightedNet else 0 end as JFM_NWP,
case when Quarter = 'Q2-AMJ' then Tier end as AMJ_Club,
case when Quarter = 'Q2-AMJ' then WeightedNet else 0 end as AMJ_NWP,
case when Quarter = 'Q3-JAS' then Tier end as JAS_Club,
case when Quarter = 'Q3-JAS' then WeightedNet else 0 end as JAS_NWP,
case when Quarter = 'Q4-OND' then Tier end as OND_Club,
case when Quarter = 'Q4-OND' then WeightedNet else 0 end as OND_NWP
from PBPOneDB.dbo.PartnerQuarterlyCalculation nolock
where CalendarYear = 2026 --AND TierLevel > 0
),
cte_1 as
(
select 
PartnerCode,
max(JFM_Club) as JFM_Club,
sum(JFM_NWP) as JFM_NWP,
max(AMJ_Club) as AMJ_Club,
sum(AMJ_NWP) as AMJ_NWP,
max(JAS_Club) as JAS_Club,
sum(JAS_NWP) as JAS_NWP,
max(OND_Club) as OND_Club,
sum(OND_NWP) as OND_NWP,
sum(case when JFM_Club = 'Crown' then 1 else 0 end+
	case when AMJ_Club = 'Crown' then 1 else 0 end+
	case when JAS_Club = 'Crown' then 1 else 0 end+
	case when OND_Club = 'Crown' then 1 else 0 end)
as 'Crown_Count'
from cte
group by PartnerCode
),
cte_2 as
(
select *,
(JFM_NWP + AMJ_NWP + JAS_NWP + OND_NWP ) as Total_NWP,
case when (JFM_NWP + AMJ_NWP + JAS_NWP + OND_NWP ) >= 25000000 then 'One Club'
	 when (JFM_NWP + AMJ_NWP + JAS_NWP + OND_NWP ) >= 15000000 then 'CBO'
	 when (JFM_NWP + AMJ_NWP + JAS_NWP + OND_NWP ) >= 7500000 then 'Masters'
	 end as 'Annual_Club',

case when (JFM_NWP + AMJ_NWP + JAS_NWP + OND_NWP ) >= 25000000 and Crown_Count >=3 then 'One Club'
	 when (JFM_NWP + AMJ_NWP + JAS_NWP + OND_NWP ) >= 15000000 and Crown_Count >=2 then 'CBO'
	 when (JFM_NWP + AMJ_NWP + JAS_NWP + OND_NWP ) >= 7500000 and Crown_Count >=1 then 'Masters'
	 end as 'Annual_Club_with_Crown_Logic',

case when JFM_NWP >= 25000000/2 then 'Paris Pass'
	 when JFM_NWP >= 15000000/2 then 'Armenia Pass'
	 when JFM_NWP >= 7500000/2 then 'Dubai Pass'
	 end as 'JFM_Pass'

from cte_1
)
select * from cte_2
--where [Annual_Club] is not null or
--[JFM_Pass] is not null
order by Total_NWP desc 