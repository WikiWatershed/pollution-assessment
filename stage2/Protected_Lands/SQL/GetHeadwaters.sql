
-- GET THE HEADWATERS BASED ON THE NORD (ANS DU)
select t1.comid, t1.nord, t1.nordstop, t1.streamorde, t2.nord, t2.streamorde, t1.huc12
from spatial.nhdplus_maregion as t1
left join spatial.nhdplus_maregion as t2
on (t1.nord - 1) = t2.nord
where t1.streamorde <= 3 
	and (t2.streamorde >= 1 and t2.streamorde <= 3)
	and t1.huc12 like '0204%'
order by comid
;

-- GET THE HEADWATERS BASED ON THE FROM/TO NODES (NHDPLUS V2 VAA)
drop table if exists datapolassess.nhdplus_maregion_headwaters;
create table datapolassess.nhdplus_maregion_headwaters
as
select distinct t1.comid, t1.nord, t1.nordstop, t1.streamorde as from_streamorde, t1.fromnode, t1.tonode, t2.streamorde as to_streamorde, t1.huc12, t1.catchment as geom
from spatial.nhdplus_maregion as t1
left join spatial.nhdplus_maregion as t2
on t1.tonode = t2.fromnode
where t1.streamorde between 0 and 2
	and t2.streamorde between 1 and 3
	and t1.huc12 like '0204%'
order by comid
;

alter table datapolassess.nhdplus_maregion_headwaters add constraint pk_nhdplus_maregion_headwaters primary key (comid);
create index nhdplus_maregion_headwaters_geom_idx
on datapolassess.nhdplus_maregion_headwaters
using gist(geom);

select * from datapolassess.nhdplus_maregion_headwaters;

select sum(st_area(geom)/4046.86) as tot_area_acres from datapolassess.nhdplus_maregion_headwaters;
7,206,829

select sum(st_area(catchment)/4046.86) as tot_area_acres from spatial.nhdplus_maregion where huc12 like '0204%';
10,030,067

-- GET THE COUNTS OF CATCHMENTS IN EACH STREAM ORDER
select distinct streamorde, count(*) over (partition by streamorde) as count
from spatial.nhdplus_maregion
where huc12 like '0204%'
order by streamorde;

---------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------

-- GET ALL OF THE PARCELS
select * from datapolassess.headwaters_parcels;
select * from datapolassess.parcels_all;

-- THESE ARE THE SELECTION CRITERIA FOR THE HEADWATERS
select * 
from datapolassess.parcels_all
where (p_nat_acre >= 90 and p_for_acre >= 75)
and p_prot <= 10
and p_hw_pa2 >= 90
;

-- INTERSECT WITH COMID
drop table if exists datapolassess.headwaters_parcels_comid;
create table datapolassess.headwaters_parcels_comid
as
select * from (
	select distinct t1.id
	,t1.gid
	,t2.comid
	,t1.gis_acre as gis_acre_parcel
	,t1.tax_pin
	,t1.county
	,t1.muni
	,t1.owner1
	,t1.owner2
	,t1.owner3
	,t1.address1
	,t1.address2
	,t1.address3
	,t1.address4
	,t1.land_use
	,t1.propdesc
	,t1.propclass
	,t1.zoning
	,t1.protected
	,t1.prot_nlt_o
	,t1.prot_nlt_1
	,t1.rn
	,t1.nat_acres
	,t1.for_acres
	,t1.tot_acres
	,t1.p_nat_acre
	,t1.p_for_acre
	,t1.histo_11
	,t1.histo_21
	,t1.histo_22
	,t1.histo_23
	,t1.histo_24
	,t1.histo_31
	,t1.histo_41
	,t1.histo_42
	,t1.histo_43
	,t1.histo_52
	,t1.histo_71
	,t1.histo_81
	,t1.histo_82
	,t1.histo_90
	,t1.histo_95
	,t1.p_prot
	,t1.p_hw_osi
	,t1.p_hw_pa2
	,st_area(t2.catchment)/4046.86 as gis_acre_catchment
	,st_area(st_intersection(t1.geom, t2.catchment))/4046.86 as gis_acre_intersection
	,st_makevalid(st_multi(st_intersection(t1.geom, t2.catchment)))::geometry(multipolygon,32618) as geom
	from datapolassess.headwaters_parcels as t1
	left join spatial.nhdplus_maregion as t2
	on st_intersects(t1.geom, t2.catchment)
) as t3
-- WHERE AT LEAST 5% OF THE PARCEL IS WITHIN THE CATCHMENT
where (gis_acre_intersection/gis_acre_parcel) * 100.0 >= 5.0
order by comid, gid
;

alter table datapolassess.headwaters_parcels_comid add constraint pk_headwaters_parcels_comid primary key (comid, gid);
create index headwaters_parcels_comid_geom_idx
on datapolassess.headwaters_parcels_comid
using gist(geom);

select id			
,gid			
,comid			
,gis_acre_parcel			
,tax_pin			
,county			
,muni			
,owner1			
,owner2			
,owner3			
,address1			
,address2			
,address3			
,address4			
,land_use			
,propdesc			
,propclass			
,zoning			
,protected			
,prot_nlt_o			
,prot_nlt_1			
,rn			
,nat_acres			
,for_acres			
,tot_acres			
,p_nat_acre			
,p_for_acre			
,histo_11			
,histo_21			
,histo_22			
,histo_23			
,histo_24			
,histo_31			
,histo_41			
,histo_42			
,histo_43			
,histo_52			
,histo_71			
,histo_81			
,histo_82			
,histo_90			
,histo_95			
,p_prot			
,p_hw_osi			
,p_hw_pa2			
,gis_acre_catchment			
,gis_acre_intersection			
--,geom
from datapolassess.headwaters_parcels_comid order by gid, comid;

select distinct comid, gis_acre_catchment, sum(gis_acre_intersection) over (partition by comid) as parcel_acres
from datapolassess.headwaters_parcels_comid
;

---------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------

-- This catchment has the wrong HUC12 - a known USGS error
update spatial.nhdplus_maregion set huc12 = '020401040901'
where comid = 4152798;

-- Get the outlet catchment for huc10s
drop taBLE if exists datapolassess.huc10_outlet_comid;
create table datapolassess.huc10_outlet_comid
as
select t2.*, nhd.catchment
From (
	select distinct min(nord) over (partition by huc10) as outlet_nord, comid, nord, nordstop, huc10
	from (
		select distinct comid, nord, nordstop, tonode, fromnode, huc12, left(huc12, 10) as huc10, left(huc12, 8) as huc08
		from spatial.nhdplus_maregion
		where huc12 like '0204%'
	) as t1
) as t2
left join spatial.nhdplus_maregion as nhd
on t2.nord = nhd.nord
where t2.nord = t2.outlet_nord
order by t2.huc10, t2.comid
;

-- Get the outlet catchment for huc08s
drop taBLE if exists datapolassess.huc08_outlet_comid;
create table datapolassess.huc08_outlet_comid
as
select t2.*--, nhd.catchment
From (
	select distinct min(nord) over (partition by huc08) as outlet_nord, comid, nord, nordstop, huc08
	from (
		select distinct comid, nord, nordstop, tonode, fromnode, huc12, left(huc12, 10) as huc10, left(huc12, 8) as huc08
		from spatial.nhdplus_maregion
		where huc12 like '0204%'
	) as t1
) as t2
left join spatial.nhdplus_maregion as nhd
on t2.nord = nhd.nord
where t2.nord = t2.outlet_nord
order by t2.huc08, t2.comid
;

-- Get the outlet catchment for HUC12s
drop taBLE if exists datapolassess.huc12_outlet_comid;
create table datapolassess.huc12_outlet_comid
as
select t2.*, nhd.catchment
From (
	select distinct min(nord) over (partition by huc12) as outlet_nord, comid, nord, nordstop, huc12
	from (
		select distinct comid, nord, nordstop, tonode, fromnode, huc12, left(huc12, 10) as huc10, left(huc12, 8) as huc08
		from spatial.nhdplus_maregion
		where huc12 like '0204%'
	) as t1
) as t2
left join spatial.nhdplus_maregion as nhd
on t2.nord = nhd.nord
where t2.nord = t2.outlet_nord
order by t2.huc12, t2.comid
;

select comid, nord, huc12 from datapolassess.huc12_outlet_comid;
select comid, nord, huc10 from datapolassess.huc10_outlet_comid;
select comid, nord, huc08 from datapolassess.huc08_outlet_comid;


