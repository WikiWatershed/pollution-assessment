
select * from databmpapi.drb_loads_2019_raw  order by huc12, source limit 15;

-- GET THE AMOUNT OF ADDED LOAD REDUCED FROM SUSTAINABLE DEVELOPEMENT PRACTICES IN NEW DEVELOPMENT BETWEEN 2006 AND 2019
-- ASSUME LOAD REDUCTION EFFICIENCY FROM 1 INCH ON SW PERFORMANCE STANDARD CURVE
-- ASSUME THAT ALL DEVELOPEMENT IN THIS TIME PERIOD HAD THESE LOW IMPACT DEVELOPEMENT CONTROLS
-- TN=0.34, TP=0.56, TSS=0.68
drop view if exists datapolassess.padep_devload_reduction_v;
create or replace view datapolassess.padep_devload_reduction_v
as
select comid, 
histo_21_reduction_tn_lbs + histo_22_reduction_tn_lbs + histo_23_reduction_tn_lbs + histo_24_reduction_tn_lbs as tn_dev_reduction_lbs,
histo_21_reduction_tp_lbs + histo_22_reduction_tp_lbs + histo_23_reduction_tp_lbs + histo_24_reduction_tp_lbs as tp_dev_reduction_lbs,
histo_21_reduction_tss_lbs + histo_22_reduction_tss_lbs + histo_23_reduction_tss_lbs + histo_24_reduction_tss_lbs as tss_dev_reduction_lbs
from (

	select t1.comid, t2.huc12,

	case when histo_21_change < 0 then 0.0 else (histo_21_change*900.0/4046.86)::numeric(12,4) end as histo_21_change_acres,
	case when histo_22_change < 0 then 0.0 else (histo_22_change*900.0/4046.86)::numeric(12,4) end as histo_22_change_acres,
	case when histo_23_change < 0 then 0.0 else (histo_23_change*900.0/4046.86)::numeric(12,4) end as histo_23_change_acres,
	case when histo_24_change < 0 then 0.0 else (histo_24_change*900.0/4046.86)::numeric(12,4) end as histo_24_change_acres,

	case when histo_21_change < 0 then 0.0 else (histo_21_change*900.0/4046.86*ldo.totaln_lb_acre*0.34)::numeric(12,4) end as histo_21_reduction_tn_lbs,
	case when histo_21_change < 0 then 0.0 else (histo_21_change*900.0/4046.86*ldo.totalp_lb_acre*0.56)::numeric(12,4) end as histo_21_reduction_tp_lbs,
	case when histo_21_change < 0 then 0.0 else (histo_21_change*900.0/4046.86*ldo.sediment_lb_acre*0.68)::numeric(12,4) end as histo_21_reduction_tss_lbs,

	case when histo_22_change < 0 then 0.0 else (histo_22_change*900.0/4046.86*ldm.totaln_lb_acre*0.34)::numeric(12,4) end as histo_22_reduction_tn_lbs,
	case when histo_22_change < 0 then 0.0 else (histo_22_change*900.0/4046.86*ldm.totalp_lb_acre*0.56)::numeric(12,4) end as histo_22_reduction_tp_lbs,
	case when histo_22_change < 0 then 0.0 else (histo_22_change*900.0/4046.86*ldm.sediment_lb_acre*0.68)::numeric(12,4) end as histo_22_reduction_tss_lbs,

	case when histo_23_change < 0 then 0.0 else (histo_23_change*900.0/4046.86*mdm.totaln_lb_acre*0.34)::numeric(12,4) end as histo_23_reduction_tn_lbs,
	case when histo_23_change < 0 then 0.0 else (histo_23_change*900.0/4046.86*mdm.totalp_lb_acre*0.56)::numeric(12,4) end as histo_23_reduction_tp_lbs,
	case when histo_23_change < 0 then 0.0 else (histo_23_change*900.0/4046.86*mdm.sediment_lb_acre*0.68)::numeric(12,4) end as histo_23_reduction_tss_lbs,

	case when histo_24_change < 0 then 0.0 else (histo_24_change*900.0/4046.86*hdm.totaln_lb_acre*0.34)::numeric(12,4) end as histo_24_reduction_tn_lbs,
	case when histo_24_change < 0 then 0.0 else (histo_24_change*900.0/4046.86*hdm.totalp_lb_acre*0.56)::numeric(12,4) end as histo_24_reduction_tp_lbs,
	case when histo_24_change < 0 then 0.0 else (histo_24_change*900.0/4046.86*hdm.sediment_lb_acre*0.68)::numeric(12,4) end as histo_24_reduction_tss_lbs

	from (
		select a.comid, 
		b.histo_21 - a.histo_21 as histo_21_change,
		b.histo_22 - a.histo_22 as histo_22_change,
		b.histo_23 - a.histo_23 as histo_23_change,
		b.histo_24 - a.histo_24 as histo_24_change
		from datapolassess.nhdplus_2006 as a
		left join datapolassess.nhdplus_2019 as b
		on a.comid = b.comid
	) as t1
	left join spatial.nhdplus_maregion as t2
	on t1.comid = t2.comid
	left join (select source, huc12, sediment_lb_acre, totaln_lb_acre, totalp_lb_acre from databmpapi.drb_loads_2019_raw where source like 'Low-Density Open Space') as ldo
	on t2.huc12 = ldo.huc12
	left join (select source, huc12, sediment_lb_acre, totaln_lb_acre, totalp_lb_acre from databmpapi.drb_loads_2019_raw where source like 'Low-Density Mixed') as ldm
	on t2.huc12 = ldm.huc12
	left join (select source, huc12, sediment_lb_acre, totaln_lb_acre, totalp_lb_acre from databmpapi.drb_loads_2019_raw where source like 'Medium-Density Mixed') as mdm
	on t2.huc12 = mdm.huc12
	left join (select source, huc12, sediment_lb_acre, totaln_lb_acre, totalp_lb_acre from databmpapi.drb_loads_2019_raw where source like 'High-Density Mixed') as hdm
	on t2.huc12 = hdm.huc12
) as fin

order by comid
;

create table datapolassess.padep_devload_reduction as select * from datapolassess.padep_devload_reduction_v;
alter table datapolassess.padep_devload_reduction add constraint pk_padep_devload_reduction primary key (comid);

select * from datapolassess.padep_devload_reduction;

-- GET THE LOAD AVOIDED FROM SUSTAINABLE AGRICULTURE PRACTICES, BASED ON BEST GUESSES FROM PADEP
drop table if exists datapolassess.pa_nj_countyestimates;
create table datapolassess.pa_nj_countyestimates (	
	County	varchar
,	State	varchar
,	Conservation_Tillage	numeric(3,2)
,	Nutrient_Management	numeric(3,2)
,	Cover_Crop	numeric(3,2)
);		

select * from datapolassess.pa_nj_countyestimates;

select a.*, b.name 
from datapolassess.pa_nj_countyestimates as a
left join spatial.censuscounty_more as b
on a.county like b.name and a.statefp like b.statefp

alter table datapolassess.pa_nj_countyestimates add column statefp varchar;
update datapolassess.pa_nj_countyestimates set statefp = '42' where state like 'PA';
update datapolassess.pa_nj_countyestimates set statefp = '34' where state like 'NJ';

select * from datapolassess.pa_nj_countyestimates;

select * from spatial.censuscounty_more limit 5;

--

--'All Other';'PA';0.70;0.35;0.15;'42'
--'All Other';'NJ';0.02;0.02;0.02;'34'
/*
"Conservation Tillage":  {
      "tn": 0.08,
      "tp": 0.22,
      "tss": 0.30
    }
"Nutrient Management":  {
      "tn": 0.29,
      "tp": 0.44,
      "tss": 0.00
    }
"Cover Crop":  {
      "tn": 0.29,
      "tp": 0.50,
      "tss": 0.35
    }
*/

totaln_lb_acre
totalp_lb_acre
sediment_lb_acre

----------------------

alter table datapolassess.nhdplus_2019 drop constraint nhdplus_2019_pkey;
alter table datapolassess.nhdplus_2019 add constraint nhdplus_2019_pkey primary key (comid);
create index nhdplus_2019_comid_idx on datapolassess.nhdplus_2019 using btree(comid);

alter table datapolassess.nhdplus_2019 add column huc12 character(12);
update datapolassess.nhdplus_2019 a set huc12 = b.huc12
from spatial.nhdplus_maregion as b 
where a.comid = b.comid;
create index nhdplus_2019_huc12_idx on datapolassess.nhdplus_2019 using btree (huc12);

create table datapolassess.drb_loads_2019_raw_hp
as
select source, huc12, sediment_lb_acre, totaln_lb_acre, totalp_lb_acre from databmpapi.drb_loads_2019_raw where source like 'Hay/Pasture';
create index drb_loads_2019_raw_hp_comid_idx on datapolassess.drb_loads_2019_raw_hp using btree(huc12);
alter table datapolassess.drb_loads_2019_raw_hp add constraint pk_drb_loads_2019_raw_hp primary key (huc12);

create table datapolassess.drb_loads_2019_raw_cp
as
select source, huc12, sediment_lb_acre, totaln_lb_acre, totalp_lb_acre from databmpapi.drb_loads_2019_raw where source like 'Hay/Pasture';
create index drb_loads_2019_raw_cp_comid_idx on datapolassess.drb_loads_2019_raw_cp using btree(huc12);
alter table datapolassess.drb_loads_2019_raw_cp add constraint pk_drb_loads_2019_raw_cp primary key (huc12);

create table datapolassess.pa_nj_countyestimates_x_comid
			as
			select a.comid, b.name, b.statefp, c.Conservation_Tillage, c.Nutrient_Management, c.Cover_Crop
			from (select * from spatial.nhdplus_maregion where huc12 like '0204%')as a
			left join spatial.censuscounty_more as b
			on st_intersects(st_centroid(a.catchment), b.geom)
			left join datapolassess.pa_nj_countyestimates as c
			on c.county like b.name and c.statefp like b.statefp;
			create index pa_nj_countyestimates_x_comid_comid_idx on datapolassess.pa_nj_countyestimates_x_comid using btree(comid);
			alter table datapolassess.pa_nj_countyestimates_x_comid add constraint pk_pa_nj_countyestimates_x_comid primary key (comid);

create table datapolassess.pa_nj_countyestimates_x_comid_bmprates
		as
		select comid, name, statefp,
		-- add in the values for 'all other' counties
		case 
			when Conservation_Tillage is null and statefp = '42' then 0.70
			when Conservation_Tillage is null and statefp = '34' then 0.02
			else Conservation_Tillage end as Conservation_Tillage,
		case 
			when Nutrient_Management is null and statefp = '42' then 0.35
			when Nutrient_Management is null and statefp = '34' then 0.02
			else Nutrient_Management end as Nutrient_Management,
		case 
			when Cover_Crop is null and statefp = '42' then 0.15
			when Cover_Crop is null and statefp = '34' then 0.02
			else Cover_Crop end as Cover_Crop

		from datapolassess.pa_nj_countyestimates_x_comid as d;
		create index pa_nj_countyestimates_x_comid_bmprates_comid_idx on datapolassess.pa_nj_countyestimates_x_comid_bmprates using btree(comid);
		alter table datapolassess.pa_nj_countyestimates_x_comid_bmprates add constraint pk_pa_nj_countyestimates_x_comid_bmprates primary key (comid);

-- CREATE THE VIEW
SET enable_seqscan = OFF;
drop view if exists datapolassess.padep_agload_reduction_v;
create or replace view datapolassess.padep_agload_reduction_v
as
select comid, 
(histo_81_conservation_tillage_tn_lbs + histo_81_Nutrient_Management_tn_lbs + histo_81_Cover_Crop_tn_lbs
+ histo_82_conservation_tillage_tn_lbs + histo_82_Nutrient_Management_tn_lbs + histo_82_Cover_Crop_tn_lbs)::numeric(12,4)
as tn_ag_reduction_lbs,
(histo_81_conservation_tillage_tp_lbs + histo_81_Nutrient_Management_tp_lbs + histo_81_Cover_Crop_tp_lbs
+ histo_82_conservation_tillage_tp_lbs + histo_82_Nutrient_Management_tp_lbs + histo_82_Cover_Crop_tp_lbs)::numeric(12,4)
as tp_ag_reduction_lbs,
(histo_81_conservation_tillage_tss_lbs + histo_81_Nutrient_Management_tss_lbs + histo_81_Cover_Crop_tss_lbs
+ histo_82_conservation_tillage_tss_lbs + histo_82_Nutrient_Management_tss_lbs + histo_82_Cover_Crop_tss_lbs)::numeric(12,4)
as tss_ag_reduction_lbs

from (
	select t1.comid, 
	(histo_81)*900.0/4046.86 as histo_81_acres, 

	-- Cells converted to acres, multiplied by the fraction that is managed
	((histo_81)*900.0/4046.86)*coalesce(Conservation_Tillage,0) as histo_81_conservation_tillage_acres, 
	((histo_81)*900.0/4046.86)*coalesce(Nutrient_Management,0) as histo_81_Nutrient_Management_acres, 
	((histo_81)*900.0/4046.86)*coalesce(Cover_Crop,0) as histo_81_Cover_Crop_acres,

	-- Cells converted to acres, multiplied by the fraction that is managed, multiplied by the lbs/acre and the efficiency coefficient
	((histo_81)*900.0/4046.86)*coalesce(Conservation_Tillage,0)*hp.totaln_lb_acre*0.08 as histo_81_conservation_tillage_tn_lbs, 
	((histo_81)*900.0/4046.86)*coalesce(Conservation_Tillage,0)*hp.totalp_lb_acre*0.22 as histo_81_conservation_tillage_tp_lbs, 
	((histo_81)*900.0/4046.86)*coalesce(Conservation_Tillage,0)*hp.sediment_lb_acre*0.30 as histo_81_conservation_tillage_tss_lbs, 

	((histo_81)*900.0/4046.86)*coalesce(Nutrient_Management,0)*hp.totaln_lb_acre*0.29 as histo_81_Nutrient_Management_tn_lbs, 
	((histo_81)*900.0/4046.86)*coalesce(Nutrient_Management,0)*hp.totalp_lb_acre*0.44 as histo_81_Nutrient_Management_tp_lbs, 
	((histo_81)*900.0/4046.86)*coalesce(Nutrient_Management,0)*hp.sediment_lb_acre*0.0 as histo_81_Nutrient_Management_tss_lbs, 

	((histo_81)*900.0/4046.86)*coalesce(Cover_Crop,0)*hp.totaln_lb_acre*0.29 as histo_81_Cover_Crop_tn_lbs,
	((histo_81)*900.0/4046.86)*coalesce(Cover_Crop,0)*hp.totalp_lb_acre*0.50 as histo_81_Cover_Crop_tp_lbs,
	((histo_81)*900.0/4046.86)*coalesce(Cover_Crop,0)*hp.sediment_lb_acre*0.35 as histo_81_Cover_Crop_tss_lbs,

	(histo_82)*900.0/4046.86 as histo_82_acres, 

	((histo_82)*900.0/4046.86)*coalesce(Conservation_Tillage,0) as histo_82_conservation_tillage_acres, 
	((histo_82)*900.0/4046.86)*coalesce(Nutrient_Management,0) as histo_82_Nutrient_Management_acres, 
	((histo_82)*900.0/4046.86)*coalesce(Cover_Crop,0) as histo_82_Cover_Crop_acres,

	((histo_82)*900.0/4046.86)*coalesce(Conservation_Tillage,0)*cp.totaln_lb_acre*0.08 as histo_82_conservation_tillage_tn_lbs, 
	((histo_82)*900.0/4046.86)*coalesce(Conservation_Tillage,0)*cp.totalp_lb_acre*0.22 as histo_82_conservation_tillage_tp_lbs, 
	((histo_82)*900.0/4046.86)*coalesce(Conservation_Tillage,0)*cp.sediment_lb_acre*0.30 as histo_82_conservation_tillage_tss_lbs, 

	((histo_82)*900.0/4046.86)*coalesce(Nutrient_Management,0)*cp.totaln_lb_acre*0.29 as histo_82_Nutrient_Management_tn_lbs, 
	((histo_82)*900.0/4046.86)*coalesce(Nutrient_Management,0)*cp.totalp_lb_acre*0.44 as histo_82_Nutrient_Management_tp_lbs, 
	((histo_82)*900.0/4046.86)*coalesce(Nutrient_Management,0)*cp.sediment_lb_acre*0.0 as histo_82_Nutrient_Management_tss_lbs, 

	((histo_82)*900.0/4046.86)*coalesce(Cover_Crop,0)*cp.totaln_lb_acre*0.29 as histo_82_Cover_Crop_tn_lbs,
	((histo_82)*900.0/4046.86)*coalesce(Cover_Crop,0)*cp.totalp_lb_acre*0.50 as histo_82_Cover_Crop_tp_lbs,
	((histo_82)*900.0/4046.86)*coalesce(Cover_Crop,0)*cp.sediment_lb_acre*0.35 as histo_82_Cover_Crop_tss_lbs

	from datapolassess.nhdplus_2019 as t1
	left join datapolassess.pa_nj_countyestimates_x_comid_bmprates as t2
	on t1.comid = t2.comid

	left join
		datapolassess.drb_loads_2019_raw_hp as hp
		on t1.huc12 = hp.huc12
	left join
		datapolassess.drb_loads_2019_raw_cp as cp
		on t1.huc12 = cp.huc12
) as fin

order by comid
;

SET enable_seqscan = OFF;
create table datapolassess.padep_agload_reduction as select * from datapolassess.padep_agload_reduction_v;
alter table datapolassess.padep_agload_reduction add constraint pk_padep_agload_reduction primary key (comid);

select * from datapolassess.padep_agload_reduction;

-- BRING THE DEV AND AG TABLES TOGETHER!

select * from datapolassess.padep_devload_reduction order by comid;
select * from datapolassess.padep_agload_reduction order by comid;

select a.comid, 
a.tn_dev_reduction_lbs + b.tn_ag_reduction_lbs as tn_reduction_lbs, 
a.tp_dev_reduction_lbs + b.tp_ag_reduction_lbs as tp_reduction_lbs, 
a.tss_dev_reduction_lbs + b.tss_ag_reduction_lbs as tss_reduction_lbs
from datapolassess.padep_agload_reduction as a
left join datapolassess.padep_devload_reduction as b
on a.comid = b.comid
;

-- ADD IN THE BMPs FROM FIELDDOC

-- RESTORATION
'Delaware River Operational Fund'
'Delaware Watershed Conservation Fund'
'Delaware River Restoration Fund'

select * from datapolassess.fd_api_restoration_comid;

select * from datapolassess.cache_restoration_lbsreduced_comid;

drop table if exists datapolassess.fd_api_restoration_lbsreduced_comid;
create table datapolassess.fd_api_restoration_lbsreduced_comid
as
select comid as comid_rest
,(tn_reduction_lbs/2.20462)::numeric(12,4) as tn_reduced_kg
,(tp_reduction_lbs)::numeric(12,4) as tp_reduced_kg
,(tss_reduction_lbs)::numeric(12,4) as tss_reduced_kg
,source
from (

	select distinct comid
	,sum(tn) over (partition by comid)::numeric(12,4) as tn_reduction_lbs
	,sum(tp) over (partition by comid)::numeric(12,4) as tp_reduction_lbs
	,sum(tss) over (partition by comid)::numeric(12,4) as tss_reduction_lbs
	,program_name as source
	from datapolassess.fd_api_restoration_comid
	where tn + tp + tss > 0.00

	union all
	select distinct comid, 
	tn_dev_reduction_lbs as tn_reduction_lbs, 
	tp_dev_reduction_lbs as tp_reduction_lbs, 
	tss_dev_reduction_lbs as tss_reduction_lbs,
	'PADEP'::text as source
	from datapolassess.padep_devload_reduction
	where tn_dev_reduction_lbs + tp_dev_reduction_lbs + tss_dev_reduction_lbs > 0.0

	union all 
	select comid, 
	tn_ag_reduction_lbs as tn_reduction_lbs, 
	tp_ag_reduction_lbs as tp_reduction_lbs, 
	tss_ag_reduction_lbs as tss_reduction_lbs,
	'NJDEP'::text as source
	from datapolassess.padep_agload_reduction
	where tn_ag_reduction_lbs + tp_ag_reduction_lbs + tss_ag_reduction_lbs > 0.0
) as t1
order by comid
;

alter table datapolassess.fd_api_restoration_lbsreduced_comid add constraint pk_fd_api_restoration_lbsreduced_comid primary key (comid_rest, source);

select * from datapolassess.fd_api_restoration_lbsreduced_comid 
where source in ('Delaware River Operational Fund', 'Delaware Watershed Conservation Fund', 'Delaware River Restoration Fund', 'PADEP', 'NJDEP');

select * from datapolassess.fd_api_restoration_lbsreduced_comid 
where source in ('Delaware River Operational Fund', 'Delaware Watershed Conservation Fund', 'Delaware River Restoration Fund') order by source;

select * from datapolassess.fd_api_restoration_lbsreduced_comid 
where source in ('Delaware River Restoration Fund') order by source;

select * from datapolassess.fd_api_restoration_lbsreduced_comid
where source like 'PADEP' order by tp_reduced_kg;


