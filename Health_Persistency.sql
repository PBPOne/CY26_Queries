WITH base_data AS (
    SELECT 
        t1.leadid, 
        t2.ReferralID, 
        t1.Utm_term,

        CASE 
            WHEN MONTH(t1.PolicyEndDate) IN (1,2,3) THEN 'Q1-JFM'
            WHEN MONTH(t1.PolicyEndDate) IN (4,5,6) THEN 'Q2-AMJ'
            WHEN MONTH(t1.PolicyEndDate) IN (7,8,9) THEN 'Q3-JAS'
			ELSE 'Q4-OND' END AS Qtr,
            YEAR(t1.PolicyEndDate) as Year                         
    FROM  PospDB.dbo.vwAllBookingDetails as t1
    LEFT JOIN pospdb.dbo.LeadDetails_v1 as t2 
        ON t1.leadid = t2.ReferralID
    WHERE 
        t1.PolicyEndDate >= '2026-01-01' AND t1.PolicyEndDate < '2027-01-01'
        AND t1.productid IN (190, 130,224) AND t1.StatusId IN (13,41,42,43,44)       
),
Qtr_data as
(SELECT 
    upper(Utm_term) as PartnerCode,Qtr,Year,
	CAST(NULLIF(COUNT(leadid), 0) AS FLOAT) AS Qtr_Renewal_Due,
	CAST(COUNT(ReferralID) * 1.0 AS FLOAT) AS Qtr_Renewal_Done,
    ROUND(
        CAST(COUNT(ReferralID) * 1.0 AS FLOAT) / 
        CAST(NULLIF(COUNT(leadid),0) AS FLOAT), 2
    ) AS Qtr_Health_Persistency
FROM base_data
GROUP BY Utm_term, Qtr, Year
),
Annual_data as
(SELECT 
    PartnerCode,Year,
	sum(Qtr_Renewal_Due) as Yearly_Renewal_Due,
	sum(Qtr_Renewal_Done) as Yearly_Renewal_Done,
	round((sum(Qtr_Renewal_Done)*1.0/ sum(Qtr_Renewal_Due)),2)  AS Annual_Health_Persistency 
    
FROM Qtr_data
GROUP BY PartnerCode, Year
),
t1 as(
select 
	q.PartnerCode, q.Qtr as Quarter, q.Year,Qtr_Renewal_Due,Qtr_Renewal_Done,Yearly_Renewal_Due,Yearly_Renewal_Done, 
	Qtr_Health_Persistency, Annual_Health_Persistency
	
from Qtr_data q
join Annual_data a 
on q.PartnerCode= a.PartnerCode and q.Year= a.Year
),
t2 as
(
select *,
case when Qtr_Health_Persistency >= .9 then .75
	 when Qtr_Health_Persistency >= .8 then .65
	 when Qtr_Health_Persistency >= .7 then .60
	 when Qtr_Health_Persistency >= .6 then .55
	 when Qtr_Health_Persistency < .6 then 0.0
	 end as Qtr_Health_Persistency_Multiplier,

case when Yearly_Renewal_due < 5 then 1
	 when Annual_Health_Persistency >= .9 then .75
	 when Annual_Health_Persistency >= .8 then .65
	 when Annual_Health_Persistency >= .7 then .60
	 when Annual_Health_Persistency >= .6 then .55
	 when Annual_Health_Persistency < .6 then 0.0
	 end as Annual_Health_Persistency_Multiplier,
	 'Health' as product_name, 'Renewal' as Health_bt,
row_number() over(Partition by Year, PartnerCode order by Year,PartnerCode, Quarter) as rnk
from t1
)
select * from t2







