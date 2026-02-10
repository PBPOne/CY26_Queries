with dates as 
			(select '2026-01-01' as min_date, 
					'2027-01-01' as max_date),
spl_deals as
(
	select 
		[MatrixLeadId], product from [TestDB].[dbo].[tbl_ContestDB] t
	cross join dates d
		where 
			t.ContestMonth >= d.min_date
),

all_bookings_2 as --sumis
(
select 
	LEADID, SumInsured,ProductID 
from [PospDB].[dbo].BookingDetails_v1 (nolock)
		where ProductID in (3,106,118,130,138,144,189,190,147,224)
),

Health_Verification_cte as --pt
(
select 
	 LeadId, VerificationStatus
from [PospDB].dbo.HealthAdditionalDetails (nolock)
     where VerificationStatus like '%Verif%'
),

all_bookings as --vw
(
select 
	vw.Utm_term,vw.leadid,vw.TotalPremium,vw.APE,vw.[Net Premium],vw.ProductId,IsComplianceN,
	vw.[Insurer Name],vw.BookingMode,vw.BookingDate,
	DATEFROMPARTS(YEAR(vw2.Prev_end_date), MONTH(vw2.Prev_end_date), 1) as MON,
	vw.BusinessType,vw.Status,vw.StatusId,cast(vw.PolicyStartDate as date) as PolicyStartDate,cast(vw2.Prev_end_date as date) as Prev_end_date,
	vw.ProductId as Product_updated,
	'Health' as product_name,

	case when Month(Prev_end_date) in (1,2,3) and BookingDate <= DATEFROMPARTS(YEAR(Prev_end_date),5, 15)  then DATEFROMPARTS(YEAR(Prev_end_date),5, 15)
		when Month(Prev_end_date) in (4,5,6) and BookingDate <= DATEFROMPARTS(YEAR(Prev_end_date),8, 15) then DATEFROMPARTS(YEAR(Prev_end_date),8, 15)
		when Month(Prev_end_date) in (7,8,9) and BookingDate <= DATEFROMPARTS(YEAR(Prev_end_date),11,15) then DATEFROMPARTS(YEAR(Prev_end_date),11, 15)
		when Month(Prev_end_date) in (10,11,12) and BookingDate <= DATEFROMPARTS(YEAR(Prev_end_date)+1,2, 15) then DATEFROMPARTS(YEAR(Prev_end_date)+1,2, 15)
		else null
	 end as Qtr_Locking_Date
from [PospDB].[dbo].vwAllBookingDetails vw (nolock)
	cross join dates d
	inner join (select referralid, leadid from [PospDB].[dbo].LeadDetails_v1  (nolock)) abc
	on vw.leadid = abc.LeadID
	left join (select leadid, policyEndDate as Prev_end_date from [PospDB].[dbo].vwAllBookingDetails) vw2
	on abc.ReferralID = vw2.leadid
	where
	vw.ProductId in (130,190, 224) and vw.BusinessType in ('Renewal') ---Only Health Renewal Cases
	and vw.BookingDate >= DATEADD(MONTH, -2, d.min_date)
	and vw.BookingDate <  DATEADD(DAY, 15, DATEADD(MONTH, 1, d.max_date))
	and StatusId in (41,42,43,44) 
	and vw2.Prev_end_date >=  d.min_date 
	and vw2.Prev_end_date < d.max_date
),
p_motor as (
select 
	PartnerCode,SellNowEnabled,ComplianceCertified
	from [PospDB].[dbo].vwAllPartnerDetails_v1  (nolock)
	where 
		PartnerCode like 'IP%'
		and SellNowEnabled = 'Yes' 
		and ComplianceCertified = 'Yes'
		and Markettype in ('Central_Vishal Khede','Dealership','East 1_Avishek Bhowmick','East 2_Ritesh Luktuke','North 1_Pawan Sehrawat','North 2_Rajesh Singh','South 1_Aritra Dasgupta','South 2_Amit Bhadoria','VRM _Gautam Ranjan','West_Anuj Aggarwal', 'Others')
		and SalesCat not in  ('prime','Strategic Motor', 'Fleets')
),
p_other as (
select 
	PartnerCode,SellNowEnabled,ComplianceCertified
	from [PospDB].[dbo].vwAllPartnerDetails_v1  (nolock)
	where 
		PartnerCode like 'IP%'
		and SellNowEnabled = 'Yes' 
		and Markettype not in ('Central_Vishal Khede','Dealership','East 1_Avishek Bhowmick','East 2_Ritesh Luktuke','North 1_Pawan Sehrawat','North 2_Rajesh Singh','South 1_Aritra Dasgupta','South 2_Amit Bhadoria','VRM _Gautam Ranjan','West_Anuj Aggarwal',
		'Fleets_Anuj Aggarwal', 'Strategic Motor', 'Others')
		and SalesCat not in  ('prime','Strategic Motor','Fleets','Others')
),
p1 as 
(select * from p_motor
union all
select * from p_other
),

t1 as
(
select vw.*, 
		sumis.SumInsured, hv.VerificationStatus,sd.MatrixLeadId
from all_bookings vw
	left join all_bookings_2 sumis on vw.leadid = sumis.LEADID and vw.ProductId = sumis.ProductID
	left join Health_Verification_cte hv on vw.leadid = hv.LeadId
	left join spl_deals sd on vw.leadid = sd.MatrixLeadId and vw.product_name= sd.product
	where StatusId in (41, 42, 43, 44)
),
t2 as
(
select  
	 upper(p1.PartnerCode) as PartnerCode,p1.SellNowEnabled,p1.ComplianceCertified, t1.*,
	 'Renewal' as Health_bt
from t1
inner join p1 on t1.Utm_term = p1.PartnerCode
),
t3 as
(
select  t2.*, APE as netpr,
--1 as policy_booked_flag,
case when StatusId in (41,42,44) then 1 else 0 end as policy_issued_flag,
--1 as policy_verified_flag,

case when MatrixLeadId is null then 1 else 0 end as special_deal_flag,---0 means special deal

case when  [Insurer Name] like '%National Insurance%' 
			 or [Insurer Name] like '%Oriental%' 
			 or [Insurer Name] like '%United%' 
			 or [Insurer Name] like '%New India%' then 'PSU'
		  else 'Others_pvt' end as health_insurers
	 
from t2
),
t4 as
(
select *,
case when Product_updated in (3,118,130,189,147,224) then netpr*1.25
	 when Product_updated in (190) and Health_bt in ('New') and SumInsured >= 1000000 then netpr*1.25
	 when Product_updated in (190) and Health_bt in ('New') and SumInsured >= 500000 and SumInsured < 1000000 then netpr*.75
	 when Product_updated in (190) and SumInsured < 500000  then netpr*0
	 when Product_updated in (190) and Health_bt in ('Port') and SumInsured >= 1000000 then netpr*.5
	 when Product_updated in (190) and Health_bt in ('Port') and SumInsured < 1000000 then 0   --added
	 when Product_updated in (106,138,144) then netpr   --verified with Lalit on 17 Jan 25
else netpr
	 end as 'Accrual_Net_Pr'
from t3
),
t5 as
(
select *,
case when health_insurers in ('PSU') then 0 else netpr end as 'Accrual_Net_Ins',
case when ComplianceCertified = 'Yes' and IsComplianceN = 'Yes' then 1 else 0 end as compliance_flag
from t4
)
select --top 5 
PartnerCode,SellNowEnabled,ComplianceCertified,IsComplianceN,compliance_flag,leadid,TotalPremium,APE,netpr,[Insurer Name], BookingDate,
MON,Status,StatusId,Product_updated,product_name,Qtr_Locking_Date,Prev_end_date,Health_bt,policy_issued_flag,special_deal_flag, Accrual_Net_Pr, Accrual_Net_Ins,
(Accrual_Net_Ins * special_deal_flag) as Accrual_Net_Booked,
(Accrual_Net_Ins * policy_issued_flag * special_deal_flag) as Accrual_Net,
(Accrual_Net_Ins * policy_issued_flag * special_deal_flag * compliance_flag) as Accrual_Net_C,
(Accrual_Net_Ins * policy_issued_flag * special_deal_flag)*4 as W_Net,
(Accrual_Net_Ins * policy_issued_flag * special_deal_flag * compliance_flag)*4 as W_Net_C
from t5

