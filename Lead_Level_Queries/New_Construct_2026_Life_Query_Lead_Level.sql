with dates as 
			(select '2026-01-01' as min_date, 
					'2026-04-01' as max_date),
spl_deals as
(
	select 
		[MatrixLeadId], product from [TestDB].[dbo].[tbl_ContestDB] t
	cross join dates d
		where 
			t.ContestMonth >= d.min_date
),

all_bookings_1 as --bp
(
select LEADID, PlanId, SupplierId, ProductID, BasicPremium,PaymentPeriodicity,cast(issuanceDate as date) as issuanceDate--SupplierName,
from [PospDB].[dbo].BookingDetails_v1 b1 (nolock)
),

life_plans as --pl
(
select 
	distinct PlanID,ProductID,SupplierID,PayoutProdCat 
from [PospDB].insurers.life_plan_details (nolock)
),

Payterm_cte as --pt
(
select 
	 LeadId, PayTerm
from [PospDB].dbo.vehicleDetails (nolock)
),

all_bookings as --vw
(
select 
	vw.Utm_term,vw.leadid,vw.TotalPremium,vw.APE,vw.[Net Premium],vw.ProductId,IsComplianceN,
	vw.[Insurer Name],vw.BookingMode,vw.BookingDate,
	DATEFROMPARTS(YEAR(vw.BookingDate), MONTH(vw.BookingDate), 1) as MON,
	vw.BusinessType,vw.Status,cast(vw.PolicyStartDate as date) as PolicyStartDate,vw.StatusId,
	'Life' as product_name,
	vw.ProductId as Product_updated,
	case 
		when Month(BookingDate) in (1,2,3) then DATEFROMPARTS(YEAR(BookingDate),5, 15)
		when Month(BookingDate) in (4,5,6) then DATEFROMPARTS(YEAR(BookingDate),8, 15)
		when Month(BookingDate) in (7,8,9) then DATEFROMPARTS(YEAR(BookingDate),11, 15)
		else DATEFROMPARTS(YEAR(BookingDate)+1,2, 15)
		end as Qtr_Locking_Date
from [PospDB].[dbo].vwAllBookingDetails vw (nolock)
	cross join dates d
	where
		vw.BookingDate >= d.min_date
		and vw.BookingDate < d.max_date
		and vw.ProductId in (7,115,200) 
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
select vw.*, bp.BasicPremium,bp.issuanceDate,bp.PaymentPeriodicity,--bp.SupplierName,
		pl.PayoutProdCat,pt.PayTerm,sd.MatrixLeadId
from all_bookings vw
    left join all_bookings_1 bp on vw.leadid = bp.LEADID
	left join life_plans pl on bp.PlanId = pl.PlanID and bp.ProductID = pl.ProductID and bp.SupplierId = pl.SupplierID
	left join Payterm_cte pt on vw.leadid = pt.LeadId
	left join spl_deals sd on vw.leadid = sd.MatrixLeadId and vw.product_name= sd.product
	where Status in ('Booked','Policy Issued','Sale Complete','Soft Copy Received')
),
t2 as
(
select  
	 upper(p1.PartnerCode) as PartnerCode,p1.SellNowEnabled,p1.ComplianceCertified, t1.*
     
from t1
inner join p1 on t1.Utm_term = p1.PartnerCode
),
t3 as
(
select  t2.*, BasicPremium as netpr,
      case when Status in ('Policy Issued', 'Sale Complete','Soft Copy Received') then 1
	  else 0 end as policy_issued_flag,
	  case when MatrixLeadId is null then 1 else 0 end as special_deal_flag, ---0 means special deal

	  1 as policy_verified_flag ,  --tentative

case when [Insurer Name] in ('LIC India') or [Insurer Name] like 'SBI%' then 'PSU'
	when 
	([Insurer Name] like 'HDFC%' 
	or [Insurer Name] like 'ICICI%' 
	or [Insurer Name] like 'Tata%' 
	or [Insurer Name] like 'bajaj%' 
	or [Insurer Name] like 'axis%' 
	or [Insurer Name] like 'Birla%') then 'Pvt'
	else 'Others_pvt' end as life_insurers
from t2
),
	
t4 as
(
select *,
case when  PaymentPeriodicity in ('Single','Single pay','Single Premium')  then 0
	 when  PayoutProdCat = 'ULIP' then 0
	 when PayTerm in (2,3,4) then netpr*.5
else netpr
	 end as 'Accrual_Net_Pr'
from t3
),

t5 as
(
select *,
case 
	 when life_insurers in ('PSU') then 0
	 when life_insurers in ('Pvt') then Accrual_Net_Pr
	 when life_insurers in ('Others_pvt') then Accrual_Net_Pr*0.75
	 else Accrual_Net_Pr
end as 'Accrual_Net_Ins',
case when ComplianceCertified = 'Yes' and IsComplianceN = 'Yes' then 1 else 0 end as compliance_flag
from t4
)
select
PartnerCode,SellNowEnabled,ComplianceCertified,IsComplianceN,compliance_flag,leadid,TotalPremium,APE,netpr,PaymentPeriodicity,PayoutProdCat,PayTerm,[Insurer Name], BookingDate,
MON,Status,StatusId,Product_updated,product_name,Qtr_Locking_Date,policy_issued_flag,policy_verified_flag,special_deal_flag,Accrual_Net_Pr, Accrual_Net_Ins,
(Accrual_Net_Ins * special_deal_flag) as Accrual_Net_Booked,
(Accrual_Net_Ins * policy_issued_flag * policy_verified_flag * special_deal_flag) as Accrual_Net,
(Accrual_Net_Ins * policy_issued_flag * policy_verified_flag * special_deal_flag * compliance_flag) as Accrual_Net_C,
(Accrual_Net_Ins * policy_issued_flag * policy_verified_flag * special_deal_flag)*1.5 as W_Net,
(Accrual_Net_Ins * policy_issued_flag * policy_verified_flag * special_deal_flag * compliance_flag)*1.5 as W_Net_C
from t5



