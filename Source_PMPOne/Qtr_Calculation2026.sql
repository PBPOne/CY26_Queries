select --top 100
PartnerCode,
CalendarYear as Year,
case 
	 when Quarter = 'Q1' then 'Q1-JFM'
	 when Quarter = 'Q2' then 'Q2-AMJ'
	 when Quarter = 'Q3' then 'Q3-JAS'
	 when Quarter = 'Q4' then 'Q4-OND'
END as Quarter,
TotalPremium as Total_Premium,
NetPremium as Net_Premium,
APE,
WeightedNet as W_Net_2,
Motor_WNet as Motor_W_Net,
Health_WNet as Health_W_Net_2,
Life_WNet as Life_W_Net,
SME_WNet as SME_W_Net,
NoOfMonth as Active_Months,
Tier,
AddonCoins as Adonncoins_on_WNet
--Motor_AddonCoins as Motor_PBP_Coins,
--Health_AddonCoins as Health_PBP_Coins,
--Life_AddonCoins as Life_PBP_Coins,
--SME_AddonCoins as SME_PBP_Coins

	


from PBPOneDB.dbo.PartnerQuarterlyCalculationop nolock
where CalendarYear =2026
and IsActive =1

--and PartnerCode = 'IP100768'
