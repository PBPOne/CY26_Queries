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
	DATEFROMPARTS(YEAR(vw.BookingDate), MONTH(vw.BookingDate), 1) as MON,
	vw.BusinessType,vw.Status,cast(vw.PolicyStartDate as date) as PolicyStartDate,vw.StatusId,
	vw.ProductId as Product_updated,
	'Health' as product_name,
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
		and vw.ProductId in (3,106,118,130,138,144,189,190,147,224)
		and vw.BusinessType not in ('Renewal')
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
	where StatusId in (13, 39, 41, 42, 43, 44, 77)
),
t2 as
(
select  
	 upper(p1.PartnerCode) as PartnerCode,p1.SellNowEnabled,p1.ComplianceCertified, t1.*,
    

	 case when  BusinessType in ('Fresh Booking','New','NEW BUSINESS','Fresh') then 'New'
	      when  BusinessType in ('Fresh Port','New Port','Rollover Port','Rollover') then 'Port'
		  --when  BusinessType in ('Renewal') then 'Renewal'
		  end as Health_bt
from t1
inner join p1 on t1.Utm_term = p1.PartnerCode
),
t3 as
(
select  t2.*, APE as netpr,

1 as policy_booked_flag,

case when StatusId in (41, 42, 44) then 1
	else 0 end as policy_issued_flag,

case when MatrixLeadId is null then 1 else 0 end as special_deal_flag, ---0 means special deal

case when VerificationStatus is not null  then 1 
	when VerificationStatus is  null and Product_updated in (190,130,224) and Health_bt in ('New', 'Port')  then 0 
	else 1 end as policy_verified_flag,

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
case 
    when health_insurers = 'PSU' and Product_updated > 3 then Accrual_Net_Pr * 0
    else Accrual_Net_Pr
end as Accrual_Net_Ins,
case when ComplianceCertified = 'Yes' and IsComplianceN = 'Yes' then 1 else 0 end as compliance_flag,
'Health_Fresh' as pd
from t4
)
select --top 5 
PartnerCode,SellNowEnabled,ComplianceCertified,IsComplianceN,compliance_flag,leadid,TotalPremium,APE,netpr,SumInsured,[Insurer Name], BookingDate,
MON,Status,StatusId,Product_updated,product_name,Qtr_Locking_Date,Health_bt,policy_booked_flag,policy_issued_flag,policy_verified_flag,special_deal_flag,Accrual_Net_Pr, Accrual_Net_Ins,
(Accrual_Net_Ins * special_deal_flag) as Accrual_Net_Booked,
(Accrual_Net_Ins * policy_issued_flag * policy_verified_flag * special_deal_flag) as Accrual_Net,
(Accrual_Net_Ins * policy_issued_flag * policy_verified_flag * special_deal_flag * compliance_flag) as Accrual_Net_C,
(Accrual_Net_Ins * policy_issued_flag * policy_verified_flag * special_deal_flag)*4 as W_Net,
(Accrual_Net_Ins * policy_issued_flag * policy_verified_flag * special_deal_flag * compliance_flag)*4 as W_Net_C
from t5

