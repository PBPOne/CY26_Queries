select
	PartnerCode,PartnerName,RMCode, RMName, MarketType,PartnerPhoneNumber,PartnerEmailId,ComplianceCertified,SellNowEnabled,
    case 
    when MarketType in ('Central_&_West_Vishal Khede','East_Avishek Bhowmick','North 1_Pawan Sehrawat','North 2_Rajesh Singh','North_Dealership','South 1_Aritra Dasgupta','South 2_Chirag Mehta','South 3_Amit Bhadoria','South_Dealership','VRM_Gautam Ranjan','VRM_Prashant Gulati','Others','Fleets','Institutional','PRIME','Strategic Motor') then 'Motor'
    --when SalesCat in ('Retail-Dealership','Retail','Strategic Motor') then 'Motor'
    when SalesCat in ('PRIME') AND NH_NAME in ('VRMs Health','NSMHealth Health') then 'Health'
    --when SalesCat in ('PRIME') AND NH_NAME not in ('VRMs Health','NSMHealth Health') then 'Motor'
    --when SalesCat in ('Default') then 'Other'
    --when SalesCat in ('VRM') and NH_NAME is null and ZH_NAME = 'Gautam Ranjan'  then 'Motor'
    --when SalesCat in ('VRM') and NH_NAME in ('null','Prashant Gulati') then 'Other'
    --when SalesCat in ('VRM') and NH_NAME in ('CC Admin','VRMs Motor') then 'Motor'
    when SalesCat in ('VRM') and NH_NAME in ('VRMs Health','NSMHealth Health') then 'Health'
    when SalesCat in ('VRM') and NH_NAME in ('VRMs Life') then 'Life' else SalesCat
    end as Vertical
	from PospDB.dbo.vw_AllPartnersDetails_v1 (NOLOCK)
	where PartnerCode like 'IP%'


