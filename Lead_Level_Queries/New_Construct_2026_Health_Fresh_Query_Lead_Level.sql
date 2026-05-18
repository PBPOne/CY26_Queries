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
	DATEFROMPARTS(YEAR(vw.BookingDate), MONTH(vw.BookingDate), 1) as MON,
	vw.BusinessType,vw.Status,cast(vw.PolicyStartDate as date) as PolicyStartDate,vw.StatusId,
	vw.ProductId as Product_updated,
	'Health' as product_name,
	'Health_Fresh' as bt,
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
--All Partners Base
p_base AS (
    SELECT 
        PartnerCode,
        SellNowEnabled,
        ComplianceCertified,
        Markettype,
        SalesCat
    FROM [PospDB].[dbo].vwAllPartnerDetails_v1 (NOLOCK)
    WHERE 
        PartnerCode LIKE 'IP%'
        AND SellNowEnabled = 'Yes'
),

-- SME Inclusion List
p_sme_inclusion AS (
    SELECT 
        PartnerCode,
        SellNowEnabled,
        ComplianceCertified
    FROM p_base
    WHERE PartnerCode IN (
        'IP100107','IP100314','IP100315','IP100465','IP10067','IP100703','IP101070','IP101665','IP101686','IP101746','IP101750','IP102327','IP102451','IP102463','IP10304','IP103090','IP103222','IP10346','IP103925','IP104167','IP104570','IP104618','IP104703','IP104787','IP105210','IP105231','IP105233','IP105535','IP105788','IP106536','IP10664','IP107243','IP107491','IP107566','IP107671','IP107940','IP107981','IP108012','IP108218','IP108360','IP108629','IP109150','IP109348','IP109920','IP110671','IP111035','IP111137','IP112216','IP112523','IP112771','IP112772','IP11303','IP113072','IP113556','IP114461','IP114931','IP115536','IP115545','IP115797','IP115993','IP11650','IP116516','IP116538','IP11660','IP116697','IP118119','IP118168','IP11820','IP118391','IP118429','IP119468','IP119469','IP119786','IP119916','IP120995','IP121160','IP121301','IP121450','IP12177','IP122232','IP122381','IP122490','IP122598','IP123675','IP123930','IP124046','IP124538','IP124665','IP125366','IP125420','IP125614','IP126030','IP126162','IP126810','IP128775','IP128791','IP129017','IP130291','IP130385','IP130656','IP130975','IP131175','IP131308','IP131343','IP131378','IP133021','IP133121','IP133298','IP134793','IP134852','IP135295','IP135477','IP135649','IP136126','IP136352','IP136708','IP137556','IP138316','IP138367','IP138479','IP138829','IP13924','IP13944','IP13952','IP140075','IP140257','IP140432','IP140558','IP140718','IP140829','IP142642','IP143038','IP143170','IP143281','IP143880','IP143908','IP144194','IP144344','IP144366','IP144475','IP144542','IP144601','IP145396','IP145595','IP145868','IP146086','IP14670','IP147150','IP147480','IP147670','IP147769','IP148563','IP148879','IP149353','IP149764','IP150622','IP150732','IP151222','IP151354','IP15152','IP151528','IP151654','IP152181','IP15222','IP153461','IP153678','IP154218','IP154334','IP154715','IP154959','IP155228','IP156246','IP156365','IP156367','IP156385','IP157085','IP15741','IP158392','IP158410','IP158482','IP158526','IP158815','IP158968','IP159060','IP159283','IP159382','IP160181','IP160303','IP160559','IP160953','IP161165','IP161304','IP161817','IP161981','IP162016','IP162891','IP163699','IP164383','IP164390','IP164716','IP164949','IP165297','IP165480','IP165496','IP165714','IP166653','IP167383','IP168472','IP169893','IP170139','IP170160','IP170492','IP171057','IP171202','IP172160','IP174049','IP175132','IP176696','IP177093','IP177127','IP177214','IP177404','IP178428','IP178626','IP178643','IP178795','IP178816','IP179058','IP179294','IP180247','IP180575','IP180863','IP182273','IP182445','IP182550','IP183382','IP183392','IP183922','IP184792','IP18492','IP185031','IP185325','IP187068','IP187316','IP187397','IP187425','IP188174','IP18838','IP188391','IP189018','IP18936','IP1896','IP189968','IP190987','IP191151','IP191234','IP191691','IP191866','IP192042','IP192281','IP192389','IP192398','IP192753','IP193055','IP193233','IP19339','IP193513','IP194695','IP195783','IP196199','IP196436','IP196473','IP196781','IP198038','IP198475','IP198547','IP198964','IP199033','IP199228','IP199893','IP200789','IP200980','IP201074','IP201756','IP201777','IP202058','IP202688','IP202725','IP202971','IP203098','IP203335','IP20335','IP203564','IP203745','IP204191','IP204316','IP205081','IP205298','IP20588','IP206225','IP206530','IP206650','IP207567','IP208423','IP208864','IP209709','IP209811','IP209994','IP210071','IP210578','IP210632','IP211272','IP211909','IP212481','IP212846','IP212948','IP212951','IP21335','IP213907','IP213932','IP214241','IP214421','IP21473','IP216201','IP216651','IP21736','IP217776','IP2179','IP218443','IP218534','IP219125','IP219280','IP21936','IP219748','IP219872','IP220539','IP220697','IP220886','IP221126','IP22125','IP221295','IP221577','IP221585','IP221783','IP221825','IP221942','IP222413','IP222748','IP222894','IP223181','IP223301','IP223412','IP223986','IP224400','IP224686','IP225137','IP225793','IP226113','IP227050','IP227107','IP227598','IP229143','IP229307','IP229394','IP229657','IP229705','IP230191','IP230440','IP230550','IP230553','IP230970','IP231205','IP231317','IP231742','IP231801','IP232547','IP232983','IP233044','IP234329','IP234625','IP234793','IP236433','IP236548','IP237166','IP237209','IP237653','IP237690','IP237760','IP238418','IP239026','IP239094','IP239640','IP241172','IP241505','IP24352','IP244757','IP245283','IP245436','IP246203','IP246609','IP246751','IP24682','IP247609','IP24851','IP249653','IP250300','IP250393','IP25134','IP251961','IP252653','IP252750','IP253841','IP254972','IP255555','IP256189','IP256672','IP25671','IP256859','IP257002','IP258108','IP25923','IP259627','IP259766','IP259981','IP26011','IP260650','IP261274','IP261438','IP26194','IP261974','IP262629','IP263138','IP263636','IP264513','IP264753','IP265306','IP265403','IP265692','IP266617','IP266720','IP266782','IP266822','IP266910','IP267304','IP268393','IP26864','IP268868','IP269078','IP270621','IP27093','IP271273','IP271657','IP27189','IP272566','IP273008','IP27312','IP273555','IP274042','IP274102','IP274514','IP274779','IP275419','IP27579','IP276037','IP276061','IP276364','IP277222','IP277999','IP278043','IP278048','IP278110','IP278192','IP27922','IP279447','IP279518','IP279867','IP280344','IP281443','IP282088','IP282797','IP283084','IP283497','IP283831','IP284257','IP284357','IP28437','IP286667','IP287238','IP287297','IP287305','IP287335','IP287410','IP287628','IP28791','IP28812','IP288612','IP28904','IP289060','IP289627','IP289765','IP290136','IP290374','IP290652','IP290747','IP293121','IP293914','IP295234','IP29540','IP295636','IP295712','IP29648','IP296480','IP297297','IP298840','IP298914','IP299018','IP299043','IP299462','IP299480','IP299758','IP300581','IP301460','IP301576','IP30251','IP302735','IP303312','IP303384','IP303540','IP303624','IP303785','IP303787','IP303966','IP304245','IP304288','IP305306','IP305961','IP306860','IP307025','IP308008','IP308366','IP309519','IP30954','IP310362','IP310433','IP310558','IP310661','IP311914','IP312384','IP312435','IP31245','IP313523','IP313783','IP313844','IP314162','IP315155','IP315339','IP31596','IP316642','IP316778','IP317137','IP31751','IP31788','IP319989','IP320313','IP32054','IP320856','IP321274','IP321317','IP323105','IP323475','IP323892','IP323923','IP324047','IP324189','IP324568','IP325244','IP32551','IP32661','IP327350','IP327629','IP327652','IP329165','IP329424','IP330255','IP331193','IP331836','IP332377','IP332609','IP332816','IP332843','IP332844','IP33296','IP332984','IP333022','IP333129','IP33329','IP333377','IP333533','IP333664','IP333687','IP335350','IP335839','IP336154','IP33644','IP336873','IP33705','IP337168','IP337251','IP337461','IP337525','IP337540','IP338830','IP340029','IP340143','IP340425','IP341191','IP341619','IP341632','IP341647','IP342430','IP342656','IP342842','IP343076','IP343441','IP343507','IP343592','IP343603','IP343609','IP343915','IP344302','IP344686','IP346682','IP346714','IP348224','IP348291','IP34845','IP348460','IP34950','IP349655','IP350113','IP350715','IP350899','IP352189','IP352359','IP352626','IP35292','IP353175','IP35344','IP354018','IP354120','IP354589','IP35572','IP355839','IP356038','IP356125','IP356293','IP356737','IP357283','IP357733','IP358618','IP35936','IP359506','IP36042','IP36076','IP36087','IP361045','IP361701','IP36224','IP362427','IP36315','IP364766','IP364772','IP365045','IP36559','IP36572','IP366098','IP366201','IP366245','IP366505','IP367494','IP367644','IP368632','IP368886','IP368915','IP369279','IP369546','IP369684','IP369920','IP370101','IP370792','IP370943','IP372416','IP372579','IP374712','IP37498','IP375445','IP376299','IP377896','IP378278','IP378395','IP378453','IP37895','IP380027','IP380177','IP38115','IP381534','IP381836','IP382600','IP383990','IP384260','IP384395','IP385024','IP386556','IP387475','IP388005','IP388483','IP388682','IP38885','IP390048','IP390406','IP390502','IP390635','IP39084','IP39109','IP391284','IP392835','IP393417','IP394325','IP394517','IP394614','IP39477','IP39590','IP39600','IP396000','IP397415','IP398294','IP39853','IP398615','IP399029','IP399089','IP399188','IP399278','IP399557','IP39959','IP401224','IP40176','IP40181','IP401963','IP403089','IP403466','IP403809','IP404429','IP404752','IP40512','IP405807','IP405823','IP405829','IP40588','IP406152','IP406423','IP40643','IP406726','IP406989','IP408251','IP408517','IP410122','IP41034','IP410751','IP411662','IP41215','IP41230','IP412422','IP414262','IP415469','IP41547','IP415559','IP415992','IP416009','IP416406','IP41669','IP416712','IP41675','IP417649','IP417759','IP419350','IP420843','IP42096','IP421766','IP422052','IP42257','IP42283','IP422876','IP422947','IP423375','IP423567','IP42642','IP427244','IP427388','IP427787','IP427974','IP427984','IP429120','IP429799','IP429897','IP429911','IP430029','IP430133','IP430510','IP431017','IP431116','IP431150','IP431229','IP431275','IP431589','IP431771','IP431983','IP432039','IP432075','IP432148','IP432280','IP432650','IP432789','IP433009','IP433036','IP433154','IP433418','IP433608','IP433965','IP434187','IP434212','IP434236','IP434267','IP434479','IP434536','IP434548','IP434712','IP434756','IP434919','IP435614','IP435985','IP436172','IP437583','IP43768','IP437855','IP437888','IP437942','IP437993','IP437996','IP43854','IP43857','IP439653','IP439728','IP440381','IP441363','IP442306','IP442307','IP443182','IP443488','IP443619','IP44823','IP44849','IP44866','IP449805','IP44987','IP45053','IP450966','IP45119','IP45141','IP45489','IP46812','IP46926','IP47060','IP47090','IP47322','IP47800','IP47805','IP47953','IP48299','IP48520','IP48611','IP48981','IP49878','IP50057','IP50424','IP50797','IP50843','IP51013','IP51849','IP52291','IP52790','IP5282','IP53198','IP53421','IP53791','IP53879','IP54620','IP54791','IP55354','IP56248','IP56399','IP56739','IP56879','IP56882','IP5689','IP56905','IP56925','IP57303','IP58582','IP59218','IP5991','IP60014','IP60219','IP60393','IP60573','IP60693','IP60972','IP61541','IP62391','IP62458','IP62567','IP62600','IP62919','IP63150','IP63590','IP63935','IP65261','IP65845','IP66037','IP66072','IP66736','IP67109','IP68060','IP68072','IP69324','IP70052','IP70433','IP70487','IP7150','IP71915','IP72181','IP72270','IP73019','IP73178','IP73745','IP74274','IP75118','IP75513','IP75708','IP75967','IP76179','IP77687','IP77836','IP78463','IP78654','IP78786','IP79155','IP79391','IP79600','IP79887','IP79904','IP80106','IP80455','IP80486','IP81175','IP81507','IP81532','IP81994','IP82059','IP82287','IP82369','IP82498','IP82537','IP82705','IP83048','IP84148','IP84923','IP84957','IP85140','IP85167','IP85972','IP85998','IP86098','IP86181','IP86368','IP86489','IP86707','IP86890','IP87239','IP87389','IP87583','IP87866','IP87898','IP88102','IP88952','IP89637','IP89950','IP90704','IP91025','IP91266','IP91378','IP91479','IP92282','IP92302','IP92327','IP92343','IP92439','IP92482','IP93576','IP93788','IP94129','IP94601','IP95025','IP95436','IP95775','IP95967','IP97045','IP9708','IP97133','IP97259','IP98573','IP98728','IP98826','IP99137','IP99715','IP99936','IP9996' -- your full list here
    )
),

-- Motor Partners
p_motor AS (
    SELECT 
        PartnerCode,
        SellNowEnabled,
        ComplianceCertified
    FROM p_base
    WHERE 
        PartnerCode NOT IN (SELECT PartnerCode FROM p_sme_inclusion)
        AND ComplianceCertified = 'Yes'
        AND Markettype IN (
            'Central_&_West_Vishal Khede',
            'East_Avishek Bhowmick',
            'North 1_Pawan Sehrawat',
            'North 2_Rajesh Singh',
            'North_Dealership',
            'South 1_Aritra Dasgupta',
            'South 2_Chirag Mehta',
            'South 3_Amit Bhadoria',
            'South_Dealership',
            'VRM_Gautam Ranjan',
            'VRM_Prashant Gulati',
            'Others'
        )
        AND SalesCat NOT IN ('prime', 'Strategic Motor', 'Fleets')
),

-- Other Partners
p_other AS (
    SELECT 
        PartnerCode,
        SellNowEnabled,
        ComplianceCertified
    FROM p_base
    WHERE 
        PartnerCode NOT IN (SELECT PartnerCode FROM p_sme_inclusion)
        AND Markettype NOT IN (
            'Central_&_West_Vishal Khede',
            'East_Avishek Bhowmick',
            'North 1_Pawan Sehrawat',
            'North 2_Rajesh Singh',
            'North_Dealership',
            'South 1_Aritra Dasgupta',
            'South 2_Chirag Mehta',
            'South 3_Amit Bhadoria',
            'South_Dealership',
            'VRM_Gautam Ranjan',
            'VRM_Prashant Gulati',
            'Others',
            'Fleets',
            'Institutional',
            'PRIME',
            'Strategic Motor'
        )
),
p1 as (
    select * from p_motor
    union all
    select * from p_other
    union all
    select * from p_sme_inclusion
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
    when health_insurers = 'PSU' and Product_updated <> 3 then Accrual_Net_Pr * 0
    else Accrual_Net_Pr
end as Accrual_Net_Ins,
case when ComplianceCertified = 'Yes' and IsComplianceN = 'Yes' then 1 else 0 end as compliance_flag,
'Health_Fresh' as pd
from t4
)
select --top 5 
PartnerCode,SellNowEnabled,ComplianceCertified,IsComplianceN,compliance_flag,leadid,TotalPremium,APE,netpr,SumInsured,[Insurer Name], BookingDate,
MON,Status,StatusId,Product_updated,product_name,bt,Qtr_Locking_Date,Health_bt,policy_booked_flag,policy_issued_flag,policy_verified_flag,special_deal_flag,Accrual_Net_Pr, Accrual_Net_Ins,
(Accrual_Net_Ins * special_deal_flag) as Accrual_Net_Booked,
(Accrual_Net_Ins * policy_issued_flag * policy_verified_flag * special_deal_flag) as Accrual_Net,
(Accrual_Net_Ins * policy_issued_flag * policy_verified_flag * special_deal_flag * compliance_flag) as Accrual_Net_C,
(Accrual_Net_Ins * special_deal_flag)*4 as W_Net_Booked,
(Accrual_Net_Ins * policy_issued_flag * policy_verified_flag * special_deal_flag)*4 as W_Net,
(Accrual_Net_Ins * policy_issued_flag * policy_verified_flag * special_deal_flag * compliance_flag)*4 as W_Net_C
from t5
WHERE 1=1
-- CONDITION_PLACEHOLDER






