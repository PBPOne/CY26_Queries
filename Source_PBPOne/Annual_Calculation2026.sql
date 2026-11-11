select 
PartnerCode,
CalendarYear as Year,
WeightedNet as W_Net_2,
Motor_WNet as Motor_W_Net_1,
Health_WNet as Life_NWP,
Life_WNet Life_W_Net,
SME_WNet as SME_W_Net,
Motor_Booked,
Motor_Cancelled

from PBPOneDB.dbo.AnnualCalculationsop nolock
where CalendarYear =2026
and IsActive =1
