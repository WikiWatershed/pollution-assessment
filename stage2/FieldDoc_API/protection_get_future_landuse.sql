drop table if exists datapolassess.fd_api_protection_comid_buildout_corridors;
create table datapolassess.fd_api_protection_comid_buildout_corridors
(
comid integer NOT NULL
,practice_id bigint NOT NULL
,histo_11 numeric
,histo_21 numeric
,histo_22 numeric
,histo_23 numeric
,histo_24 numeric
,histo_31 numeric
,histo_41 numeric
,histo_42 numeric
,histo_43 numeric
,histo_52 numeric
,histo_71 numeric
,histo_81 numeric
,histo_82 numeric
,histo_90 numeric
,histo_95 numeric
)
;

select * from datapolassess.fd_api_protection_comid_buildout_corridors;
select * from datapolassess.fd_api_protection_comid_nlcd2019;
select * from databmpapi.drb_loads_2019_raw order by huc12, source;


select * from datapolassess.fd_api_protection_comid_buildout_corridors
where comid = 4153500 and practice_id = 55080
select * from datapolassess.fd_api_protection_comid_nlcd2019
where comid = 4153500 and practice_i = 55080

0.0;1654479;0.0;0.0;0.0;0.0;1232001;0.0;164862;33813;2061;0.0;0.0;721863;10521
0;0;0;0;0;3074;0;6;176;0;8;0;980;0;0.0

select distinct source from databmpapi.drb_loads_2019_raw order by source;

totaln_lb_acre
totalp_lb_acre
sediment_lb_acre

-- SEE WHAT OSI SAYS
select * from datapolassess.fd_api_protection_comid_buildout_corridors;
select * from datapolassess.fd_api_protection_comid_nlcd2019;

create table datapolassess.osi_buildout_analysis
(
	project_name	varchar
,	dev_open_acres_add	numeric
,	dev_low_acres_add	numeric
,	dev_med_acres_add	numeric
,	dev_high_acres_add	numeric
,	crop_acres_add	numeric
,	notes	varchar
,	cp_acres_sub	boolean
,	hp_acres_sub	boolean
,	fore_acres_sub	boolean
,	shrub_acres_sub	boolean
,	wet_acres_sub	boolean
,	all_acres_sub	boolean

);

alter table datapolassess.osi_buildout_analysis add column geom geometry(multipolygon, 4326);

select distinct proj_name 
from datapolassess.osilpia_protection_lbsavoided 
order by proj_name;

update datapolassess.osi_buildout_analysis set project_name = 'Zemel Woodland South'
where project_name like 'Zemel Woodland S';

select distinct proj_name from datapolassess.osilpia_protection_lbsavoided where proj_name like '%Zemel%' order by proj_name;
select distinct project_name from datapolassess.osi_buildout_analysis where project_name like '%Zemel%' order by project_name;

update datapolassess.osi_buildout_analysis a set geom = b.geom
from datapolassess.osilpia_protection_lbsavoided as b
where a.project_name like b.proj_name;


select distinct b.proj_name, a.* 
from datapolassess.osi_buildout_analysis as a
left join datapolassess.osilpia_protection_lbsavoided as b
on lower(a.project_name) like lower(b.proj_name)
 --or lower(left(a.project_name,11)) like lower(left(b.proj_name,11))
order by a.project_name;

alter table datapolassess.osi_buildout_analysis add column cp_acres_sub_int int;
alter table datapolassess.osi_buildout_analysis add column hp_acres_sub_int int;
alter table datapolassess.osi_buildout_analysis add column fore_acres_sub_int int;
alter table datapolassess.osi_buildout_analysis add column shrub_acres_sub_int int;
alter table datapolassess.osi_buildout_analysis add column wet_acres_sub_int int;
alter table datapolassess.osi_buildout_analysis add column all_acres_sub_int int;

update datapolassess.osi_buildout_analysis set cp_acres_sub_int = 1
where cp_acres_sub = 't';
update datapolassess.osi_buildout_analysis set hp_acres_sub_int = 1
where hp_acres_sub = 't';
update datapolassess.osi_buildout_analysis set fore_acres_sub_int = 1
where fore_acres_sub = 't';
update datapolassess.osi_buildout_analysis set shrub_acres_sub_int = 1
where shrub_acres_sub = 't';
update datapolassess.osi_buildout_analysis set wet_acres_sub_int = 1
where wet_acres_sub = 't';
update datapolassess.osi_buildout_analysis set all_acres_sub_int = 1
where all_acres_sub = 't';



update datapolassess.osi_buildout_analysis set cp_acres_sub = 1
where cp_acres_sub = 't';

select distinct b.practice_n, b.comid,
a.project_name, 
dev_open_acres_add,dev_low_acres_add,dev_med_acres_add,dev_high_acres_add,crop_acres_add,
cp_acres_sub_int, hp_acres_sub_int, fore_acres_sub_int, shrub_acres_sub_int, wet_acres_sub_int, all_acres_sub_int,
count(project_name) over (partition by project_name) as comid_count, 
b.histo_11, 
case when dev_open_acres_add > 0.0 then b.histo_21 + (dev_open_acres_add*4046.86/900.0)/comid_count::numeric, 
case when dev_low_acres_add > 0.0 then b.histo_22 + (dev_low_acres_add*4046.86/900.0)/comid_count::numeric, 
case when dev_med_acres_add > 0.0 then b.histo_23 + (dev_med_acres_add*4046.86/900.0)/comid_count::numeric, 
case when dev_high_acres_add > 0.0 then b.histo_24 + (dev_high_acres_add*4046.86/900.0)/comid_count::numeric,  

case 
	when all_acres_sub_int = 1 then b.histo_31 - ((dev_open_acres_add + dev_low_acres_add + dev_med_acres_add + dev_high_acres_add + crop_acres_add)*4046.86/900.0)/10.0/comid_count::numeric, 
case  
	when all_acres_sub_int = 1 then b.histo_41 - ((dev_open_acres_add + dev_low_acres_add + dev_med_acres_add + dev_high_acres_add + crop_acres_add)*4046.86/900.0)/10.0/comid_count::numeric, 
case  
	when all_acres_sub_int = 1 then b.histo_42 - ((dev_open_acres_add + dev_low_acres_add + dev_med_acres_add + dev_high_acres_add + crop_acres_add)*4046.86/900.0)/10.0/comid_count::numeric, 
case  
	when all_acres_sub_int = 1 then b.histo_43 - ((dev_open_acres_add + dev_low_acres_add + dev_med_acres_add + dev_high_acres_add + crop_acres_add)*4046.86/900.0)/10.0/comid_count::numeric, 
case  
	when all_acres_sub_int = 1 then b.histo_52 - ((dev_open_acres_add + dev_low_acres_add + dev_med_acres_add + dev_high_acres_add + crop_acres_add)*4046.86/900.0)/10.0/comid_count::numeric, 
case  
	when all_acres_sub_int = 1 then b.histo_71 - ((dev_open_acres_add + dev_low_acres_add + dev_med_acres_add + dev_high_acres_add + crop_acres_add)*4046.86/900.0)/10.0/comid_count::numeric, 
case  
	when all_acres_sub_int = 1 then b.histo_81 - ((dev_open_acres_add + dev_low_acres_add + dev_med_acres_add + dev_high_acres_add + crop_acres_add)*4046.86/900.0)/10.0/comid_count::numeric, 
case  
	when all_acres_sub_int = 1 then b.histo_82 - ((dev_open_acres_add + dev_low_acres_add + dev_med_acres_add + dev_high_acres_add + crop_acres_add)*4046.86/900.0)/10.0/comid_count::numeric, 
case  
	when all_acres_sub_int = 1 then b.histo_90 - ((dev_open_acres_add + dev_low_acres_add + dev_med_acres_add + dev_high_acres_add + crop_acres_add)*4046.86/900.0)/10.0/comid_count::numeric, 
case  
	when all_acres_sub_int = 1 then b.histo_95 - ((dev_open_acres_add + dev_low_acres_add + dev_med_acres_add + dev_high_acres_add + crop_acres_add)*4046.86/900.0)/10.0/comid_count::numeric, 

b.histo_41, 
b.histo_42, 
b.histo_43, 
b.histo_52, 
b.histo_71, 
b.histo_81, 
b.histo_82, 
b.histo_90, 
b.histo_95 
from datapolassess.osi_buildout_analysis as a
left join datapolassess.fd_api_protection_comid_nlcd2019 as b
on a.project_name like b.practice_n
	or st_intersects (st_buffer(st_centroid(st_transform(a.geom,32618)),100), st_transform(b.geom,32618))
order by a.project_name;


-- WHERE ARE THESE PROJECTS?
-- 13 projects not found
'18 Years'
'Bear Creek Properties LLC'
	'Bear Creek Ten Mile Run'
	'Bear Creek Ten Mile Run - Subject'
	'Bearhill Downs'
	'Bear Creek: Pinchot Forest Addition'
	'Bear Creek Addition'
	'Bear Swamp'

'Brodhead Flyfishers'
	'Brodhead Watershed: SJC Builders'
'Burnt Meadow'
'Fisher'
'Graham'
'Graystone'
'Hay Creek Riparian Buffer'
'Holly Ridge Forest'
'Meister'
'S Little Bushkill'
	'Little Bushkill Forest Reserve'
	'Bushkill Watershed: Lein'
'South Branch Rancocas Creek'
'Spence'




select * from datapolassess.fd_api_protection where lower(practice_name) like '%sixteen%';

select distinct practice_name from datapolassess.fd_api_protection order by practice_name;


create table datapolassess.fd_api_protection_comid_buildout_osi;


alter table datapolassess.fd_api_protection_comid_nlcd2019 add column histo_24 numeric default 0.0;

drop table if exists datapolassess.fd_api_protection_lbsavoided_comid;
create table datapolassess.fd_api_protection_lbsavoided_comid
as
select distinct comid as comid_prot
,sum(tnlb_change/2.20462) over (partition by comid) as tn_avoided_kg
,sum(tplb_change/2.20462) over (partition by comid) as tp_avoided_kg
,sum(tsslb_change/2.20462) over (partition by comid) as tss_avoided_kg
,program_na as source
	from (
	select id
	,geom
	,comid
	,huc12
	,practice_n
	,practice_i
	,program_na
	,program_id
	,organizati
	,descriptio
	,practice_t
	,created_at
	,modified_a
	,bmp_size
	,bmp_size_u

	,histo_21_tnlb_change+histo_22_tnlb_change+histo_23_tnlb_change+histo_24_tnlb_change--+histo_31_tnlb_change+histo_41_tnlb_change+histo_42_tnlb_change
	--+histo_43_tnlb_change+histo_52_tnlb_change+histo_71_tnlb_change+histo_81_tnlb_change+histo_82_tnlb_change+histo_90_tnlb_change+histo_95_tnlb_change
	as tnlb_change

	,histo_21_tplb_change+histo_22_tplb_change+histo_23_tplb_change+histo_24_tplb_change--+histo_31_tplb_change+histo_41_tplb_change+histo_42_tplb_change
	--+histo_43_tplb_change+histo_52_tplb_change+histo_71_tplb_change+histo_81_tplb_change+histo_82_tplb_change+histo_90_tplb_change+histo_95_tplb_change
	as tplb_change

	,histo_21_tsslb_change+histo_22_tsslb_change+histo_23_tsslb_change+histo_24_tsslb_change--+histo_31_tsslb_change+histo_41_tsslb_change+histo_42_tsslb_change
	--+histo_43_tsslb_change+histo_52_tsslb_change+histo_71_tsslb_change+histo_81_tsslb_change+histo_82_tsslb_change+histo_90_tsslb_change+histo_95_tsslb_change
	as tsslb_change

	from (
		select t1.id
		,t1.geom
		,comid
		,t1.huc12
		,practice_n
		,practice_i
		,program_na
		,program_id
		,organizati
		,descriptio
		,practice_t
		,created_at
		,modified_a
		,bmp_size
		,bmp_size_u


		,histo_21_acres_change * ldo.totaln_lb_acre as histo_21_tnlb_change
		,histo_22_acres_change * ldm.totaln_lb_acre as histo_22_tnlb_change
		,histo_23_acres_change * mdm.totaln_lb_acre as histo_23_tnlb_change
		,histo_24_acres_change * hdm.totaln_lb_acre as histo_24_tnlb_change
		,histo_31_acres_change * ba.totaln_lb_acre as histo_31_tnlb_change
		,histo_41_acres_change * fore.totaln_lb_acre as histo_41_tnlb_change
		,histo_42_acres_change * fore.totaln_lb_acre as histo_42_tnlb_change
		,histo_43_acres_change * fore.totaln_lb_acre as histo_43_tnlb_change
		,histo_52_acres_change * fore.totaln_lb_acre as histo_52_tnlb_change
		,histo_71_acres_change * fore.totaln_lb_acre as histo_71_tnlb_change
		,histo_81_acres_change * hp.totaln_lb_acre as histo_81_tnlb_change
		,histo_82_acres_change * cp.totaln_lb_acre as histo_82_tnlb_change
		,histo_90_acres_change * wet.totaln_lb_acre as histo_90_tnlb_change
		,histo_95_acres_change * wet.totaln_lb_acre as histo_95_tnlb_change

		,histo_21_acres_change * ldo.totalp_lb_acre as histo_21_tplb_change
		,histo_22_acres_change * ldm.totalp_lb_acre as histo_22_tplb_change
		,histo_23_acres_change * mdm.totalp_lb_acre as histo_23_tplb_change
		,histo_24_acres_change * hdm.totalp_lb_acre as histo_24_tplb_change
		,histo_31_acres_change * ba.totalp_lb_acre as histo_31_tplb_change
		,histo_41_acres_change * fore.totalp_lb_acre as histo_41_tplb_change
		,histo_42_acres_change * fore.totalp_lb_acre as histo_42_tplb_change
		,histo_43_acres_change * fore.totalp_lb_acre as histo_43_tplb_change
		,histo_52_acres_change * fore.totalp_lb_acre as histo_52_tplb_change
		,histo_71_acres_change * fore.totalp_lb_acre as histo_71_tplb_change
		,histo_81_acres_change * hp.totalp_lb_acre as histo_81_tplb_change
		,histo_82_acres_change * cp.totalp_lb_acre as histo_82_tplb_change
		,histo_90_acres_change * wet.totalp_lb_acre as histo_90_tplb_change
		,histo_95_acres_change * wet.totalp_lb_acre as histo_95_tplb_change

		,histo_21_acres_change * ldo.sediment_lb_acre as histo_21_tsslb_change
		,histo_22_acres_change * ldm.sediment_lb_acre as histo_22_tsslb_change
		,histo_23_acres_change * mdm.sediment_lb_acre as histo_23_tsslb_change
		,histo_24_acres_change * hdm.sediment_lb_acre as histo_24_tsslb_change
		,histo_31_acres_change * ba.sediment_lb_acre as histo_31_tsslb_change
		,histo_41_acres_change * fore.sediment_lb_acre as histo_41_tsslb_change
		,histo_42_acres_change * fore.sediment_lb_acre as histo_42_tsslb_change
		,histo_43_acres_change * fore.sediment_lb_acre as histo_43_tsslb_change
		,histo_52_acres_change * fore.sediment_lb_acre as histo_52_tsslb_change
		,histo_71_acres_change * fore.sediment_lb_acre as histo_71_tsslb_change
		,histo_81_acres_change * hp.sediment_lb_acre as histo_81_tsslb_change
		,histo_82_acres_change * cp.sediment_lb_acre as histo_82_tsslb_change
		,histo_90_acres_change * wet.sediment_lb_acre as histo_90_tsslb_change
		,histo_95_acres_change * wet.sediment_lb_acre as histo_95_tsslb_change

		from (
			select a.id
			,a.geom
			,a.comid
			,a.huc12
			,a.practice_n
			,a.practice_i
			,a.program_na
			,a.program_id
			,a.organizati
			,a.descriptio
			,a.practice_t
			,a.created_at
			,a.modified_a
			,a.bmp_size
			,a.bmp_size_u
			,coalesce((b.histo_11/4046.86) - (a.histo_11*900.0/4046.86),0) as histo_11_acres_change
			,coalesce((b.histo_21/4046.86) - (a.histo_21*900.0/4046.86),0) as histo_21_acres_change
			,coalesce((b.histo_22/4046.86) - (a.histo_22*900.0/4046.86),0) as histo_22_acres_change
			,coalesce((b.histo_23/4046.86) - (a.histo_23*900.0/4046.86),0) as histo_23_acres_change
			,coalesce((b.histo_24/4046.86) - (a.histo_24*900.0/4046.86),0) as histo_24_acres_change
			,coalesce((b.histo_31/4046.86) - (a.histo_31*900.0/4046.86),0) as histo_31_acres_change
			,coalesce((b.histo_41/4046.86) - (a.histo_41*900.0/4046.86),0) as histo_41_acres_change
			,coalesce((b.histo_42/4046.86) - (a.histo_42*900.0/4046.86),0) as histo_42_acres_change
			,coalesce((b.histo_43/4046.86) - (a.histo_43*900.0/4046.86),0) as histo_43_acres_change
			,coalesce((b.histo_52/4046.86) - (a.histo_52*900.0/4046.86),0) as histo_52_acres_change
			,coalesce((b.histo_71/4046.86) - (a.histo_71*900.0/4046.86),0) as histo_71_acres_change
			,coalesce((b.histo_81/4046.86) - (a.histo_81*900.0/4046.86),0) as histo_81_acres_change
			,coalesce((b.histo_82/4046.86) - (a.histo_82*900.0/4046.86),0) as histo_82_acres_change
			,coalesce((b.histo_90/4046.86) - (a.histo_90*900.0/4046.86),0) as histo_90_acres_change
			,coalesce((b.histo_95/4046.86) - (a.histo_95*900.0/4046.86),0) as histo_95_acres_change

			from datapolassess.fd_api_protection_comid_nlcd2019 as a
			left join datapolassess.fd_api_protection_comid_buildout_corridors as b
			on a.comid = b.comid and a.practice_i = b.practice_id
		) as t1
		left join (select * from databmpapi.drb_loads_2019_raw where source like 'Low-Density Open Space') as ldo
		on t1.huc12 = ldo.huc12

		left join (select * from databmpapi.drb_loads_2019_raw where source like 'Low-Density Mixed') as ldm
		on t1.huc12 = ldm.huc12

		left join (select * from databmpapi.drb_loads_2019_raw where source like 'Medium-Density Mixed') as mdm
		on t1.huc12 = mdm.huc12

		left join (select * from databmpapi.drb_loads_2019_raw where source like 'High-Density Mixed') as hdm
		on t1.huc12 = hdm.huc12

		left join (select * from databmpapi.drb_loads_2019_raw where source like 'Barren Areas') as ba
		on t1.huc12 = ba.huc12

		left join (select * from databmpapi.drb_loads_2019_raw where source like 'Wooded Areas') as fore
		on t1.huc12 = fore.huc12

		left join (select * from databmpapi.drb_loads_2019_raw where source like 'Cropland') as cp
		on t1.huc12 = cp.huc12

		left join (select * from databmpapi.drb_loads_2019_raw where source like 'Hay/Pasture') as hp
		on t1.huc12 = hp.huc12

		left join (select * from databmpapi.drb_loads_2019_raw where source like 'Wetlands') as wet
		on t1.huc12 = wet.huc12
	) as t2

	--where comid = 4153500 and practice_i = 55080
) as t3
;

alter table datapolassess.fd_api_protection_lbsavoided_comid add constraint pk_fd_api_protection_lbsavoided_comid primary key (comid_prot, source);
GRANT SELECT ON TABLE datapolassess.fd_api_protection_lbsavoided_comid TO ms_select;
GRANT SELECT ON TABLE datapolassess.fd_api_protection_lbsavoided_comid TO srat_select;
GRANT SELECT ON TABLE datapolassess.fd_api_protection_lbsavoided_comid TO public;
GRANT SELECT ON TABLE datapolassess.fd_api_protection_lbsavoided_comid TO keisang;

select distinct source from datapolassess.fd_api_protection_lbsavoided_comid order by source;

['Delaware River Watershed Protection Fund - Forestland Capital Grants', 'Delaware River Watershed Protection Fund - Transaction Grants']

(
'Delaware River Operational Fund', -- some have reductions and some do not, not the primary focus of this program
'Delaware Watershed Conservation Fund', -- should not be in restoration
'Delaware River Restoration Fund', -- all reductions valid
'PADEP', 
'NJDEP', 
'Delaware River Watershed Protection Fund - Forestland Capital Grants', 
'Delaware River Watershed Protection Fund - Transaction Grants' 
)














