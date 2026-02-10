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
motor_business_type as
	(select Leadid,
	 case when PBPBusinessType in ('New','Rollover') then 'New' else 'Renewal' end as Motor_bt
	 from PospDB.dbo.tbl_BookingBusinessType
),
all_bookings as --vw
(
select 
	vw.Utm_term,vw.leadid,vw.TotalPremium,vw.APE,vw.[Net Premium],vw.ProductId,IsComplianceN,
	vw.[Insurer Name],vw.BookingMode,vw.BookingDate,
	DATEFROMPARTS(YEAR(vw.BookingDate), MONTH(vw.BookingDate), 1) as MON,
	vw.BusinessType, vw.SubProduct,vw.VehicleSubClass,
	vw.ODPremium,vw.TPPremium,vw.ODTerm,vw.TPTerm,vw.Status,cast(vw.PolicyStartDate as date) as PolicyStartDate,vw.StatusId,
	'Motor' as product_name,
	case 
		when vw.ProductId in (186) and vw.SubProduct = 'Taxi' then 188 
		when vw.ProductId in (186) and vw.SubProduct is null and vw.VehicleSubClass = 'Taxi' then 188 --CV
		else vw.ProductId end as Product_updated,
    
	case 
		when Month(BookingDate) in (1,2,3) then DATEFROMPARTS(YEAR(BookingDate),5, 15)
		when Month(BookingDate) in (4,5,6) then DATEFROMPARTS(YEAR(BookingDate),8, 15)
		when Month(BookingDate) in (7,8,9) then DATEFROMPARTS(YEAR(BookingDate),11, 15)
		else DATEFROMPARTS(YEAR(BookingDate)+1,2, 15)
		end as Qtr_Locking_Date
from [PospDB].[dbo].vwAllBookingDetails vw (nolock)
	cross join dates d
	where
		vw.ProductId in (186,187,188) --Motor BU
		and vw.BookingDate >= d.min_date
		and vw.BookingDate < d.max_date

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
select vw.*, mb.Motor_bt,sd.MatrixLeadId
from all_bookings vw
	left join motor_business_type mb on vw.leadid = mb.Leadid
	left join spl_deals sd on vw.leadid = sd.MatrixLeadId and vw.product_name= sd.product
),
t2 as
(
select  
	 upper(p1.PartnerCode) as PartnerCode,p1.SellNowEnabled,p1.ComplianceCertified, t1.*,
     case when ODTerm >0 then cast(ODPremium/ODTerm as float) else 0 end as od_netpr,
	 case when TPTerm >0 then cast(TPPremium/TPTerm as float) else 0 end as tp_netpr
from t1
inner join p1 on t1.Utm_term = p1.PartnerCode
),
t3 as
(
select  t2.*,
     (od_netpr+tp_netpr) as netpr,
	 1  as motor_booked_flag,
	 case when StatusId in (select StatusId  from [PospDB].[dbo].StatusMaster (nolock) where StatusName like 're%') 
		  then  1 else 0 end as motor_cancelled_flag,
	 case when StatusId in (select StatusId  from [PospDB].[dbo].StatusMaster (nolock) where StatusName not like 're%') 
		  then  1 else 0 end as policy_booked_flag,

	 case when MatrixLeadId is null then 1 else 0 end as special_deal_flag, ---0 means special deal

	 case when [Insurer Name] like '%National Insurance%' 
			or [Insurer Name] like '%Oriental%' 
			or [Insurer Name] like '%United%' 
			or [Insurer Name] like '%New India%' then 'PSU'

		  when [Insurer Name] like '%Sompo%' or [Insurer Name] like '%Iffco%' or 
			   [Insurer Name] like '%Shriram%' or [Insurer Name] like '%Raheja%' then 'Pvt'
		  else 'Others_pvt' end as motor_insurers	 
from t2
),
t4 as
(
select *,
--CV
case when Product_updated in (188) and BookingMode in ('Online') and Motor_bt in ('New','Renewal') then netpr
	 when Product_updated in (188) and BookingMode in ('Offline') and Motor_bt in ('New','Renewal') then netpr*.9
--Private Car Brand New
	 when Product_updated in (186) and ODTerm >=1 and TPTerm >=3 and BookingMode in ('Online') and Motor_bt in ('New','Renewal') then netpr
	 when Product_updated in (186) and ODTerm >=1 and TPTerm >=3 and ODTerm >0 and BookingMode in ('Offline') and Motor_bt in ('New') then netpr*.9
	 when Product_updated in (186) and ODTerm >=1 and TPTerm >=3 and ODTerm >0 and BookingMode in ('Offline') and Motor_bt in ('Renewal') then netpr*.8
--CarComp/ SAOD
	 when Product_updated in (186) and ODTerm >0 and BookingMode in ('Online') and Motor_bt in ('New') then netpr*1.2
	 when Product_updated in (186) and ODTerm >0 and BookingMode in ('Online') and Motor_bt in ('Renewal') then netpr
	 when Product_updated in (186) and ODTerm >0 and BookingMode in ('Offline') and Motor_bt in ('New') then netpr*.9
	 when Product_updated in (186) and ODTerm >0 and BookingMode in ('Offline') and Motor_bt in ('Renewal') then netpr*.8 
--Private Car TP
	 when Product_updated in (186) and ODTerm =0 and TPTerm >0 and BookingMode in ('Online') and Motor_bt in ('New','Renewal') then netpr
	 when Product_updated in (186) and ODTerm =0 and TPTerm >0 and BookingMode in ('Offline') and Motor_bt in ('New','Renewal') then netpr*.5
--Two Wheeler Brand New
	 when Product_updated in (187) and ODTerm >=1 and TPTerm >=5 and BookingMode in ('Online') and Motor_bt in ('New','Renewal') then netpr
	 when Product_updated in (187) and ODTerm >=1 and TPTerm >=5 and BookingMode in ('Offline') and Motor_bt in ('New','Renewal') then netpr*.8
--TW comp/SAOD
	 when Product_updated in (187) and ODTerm >0 and BookingMode in ('Online') and Motor_bt in ('New') then netpr*1.1
	 when Product_updated in (187) and ODTerm >0 and BookingMode in ('Online') and Motor_bt in ('Renewal') then netpr
	 when Product_updated in (187) and ODTerm >0 and BookingMode in ('Offline') and Motor_bt in ('New','Renewal') then netpr*0
--Two Wheeler TP
	 when Product_updated in (187) and ODTerm =0 and TPTerm >0 and BookingMode in ('Online') and Motor_bt in ('New','Renewal') then netpr
	 when Product_updated in (187) and ODTerm =0 and TPTerm >0 and BookingMode in ('Offline') and Motor_bt in ('New','Renewal') then netpr

else netpr
	 end as 'Accrual_Net_Pr'
	
from t3
),

t5 as
(
select *, 
case when motor_insurers in ('PSU') then Accrual_Net_Pr * .9
	 when motor_insurers in ('Pvt') then Accrual_Net_Pr * .75
	 else Accrual_Net_Pr 
	 end as 'Accrual_Net_Ins',
case when ComplianceCertified = 'Yes' and IsComplianceN = 'Yes' then 1 else 0 end as compliance_flag
	
from t4
)
select 
PartnerCode,
product_name, MON,
sum(Accrual_Net_Ins * policy_booked_flag) as Accrual_Net,
sum(case when compliance_flag=1 then Accrual_Net_Ins * policy_booked_flag else 0 end) as Accrual_Net_C,
--sum(case when compliance_flag=0 then Accrual_Net_Ins * policy_booked_flag else 0 end) as Accrual_Net_NC,
sum(motor_booked_flag) as motor_booked,
sum(motor_cancelled_flag) as motor_cancelled
from t5
group by 
PartnerCode,
product_name, MON
--having sum(Accrual_Net_Ins * policy_booked_flag) >0








