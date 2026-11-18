select 
PartnerCode,
CalendarYear as 'Year',
WeightedNet as W_Net_2,
Motor_WNet as Motor_W_Net_1,
Health_WNet as Health_NWP,
Health_WNet_Fresh as Health_W_Net_Fresh_1,
Health_WNet_Renewal as Health_W_Net_Renewal_2,
Life_WNet Life_W_Net,
SME_WNet as SME_W_Net,
Motor_Booked,
Motor_Cancelled

from PBPOneDB.dbo.AnnualCalculationsop nolock
where CalendarYear =2026
and IsActive =1
