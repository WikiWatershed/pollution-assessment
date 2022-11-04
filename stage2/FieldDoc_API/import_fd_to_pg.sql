
----------------
-- 1) FIRST DROP THE TABLES IN THE DATABSE THEN RUN THE PYTHON FILE TO IMPORT THE DATA
-- FIRST THE RESTORATION STUFF
-- python get_fd_bmps.py restoration
drop table if exists datapolassess.fd_api_restoration;
create table datapolassess.fd_api_restoration
(
practice_name TEXT, 
practice_id bigint primary key, 
program_name text, 
program_id bigint, 
organization text, 
description text, 
practice_type text, 
created_at timestamp with time zone, 
modified_at timestamp with time zone, 
tn numeric(20,2), 
tp numeric(20,2), 
tss numeric(20,2), 
--geom geometry(polygon, 4326),
--drainage_geom geometry(polygon, 4326)
geom geometry(geometrycollection, 4326),
drainage_geom geometry(multipolygon, 4326)
)
;

select * from datapolassess.fd_api_restoration;
select count(*) from datapolassess.fd_api_restoration;

-- 17,790
select sum(tp)/2.20462 as tp
from datapolassess.fd_api_restoration
where program_name in ('Delaware River Restoration Fund', 'Delaware River Operational Fund', 'Delaware Watershed Conservation Fund')
;

select *
from datapolassess.fd_api_restoration
where program_name in ('Delaware River Restoration Fund', 'Delaware River Operational Fund', 'Delaware Watershed Conservation Fund')
;

cur.execute("insert into datapolassess.fd_api_restoration (practice_name ,practice_id ,program_name ,program_id ,organization ,description ,practice_type ,created_at ,modified_at ,tn ,tp ,tss ,geom ,drainage_geom) values ({}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {});".format())

----------------
-- NOW DO THE PROTECTION STUFF
-- python get_fd_bmps.py protection
drop table if exists datapolassess.fd_api_protection;
create table datapolassess.fd_api_protection
(
practice_name TEXT, 
practice_id bigint primary key, 
program_name text, 
program_id bigint, 
organization text, 
description text, 
practice_type text, 
created_at timestamp with time zone, 
modified_at timestamp with time zone, 
tot_pwr numeric(20,2), 
head_pwr numeric(20,2), 
ara_pwr numeric(20,2), 
wet_pwr numeric(20,2), 
str_bank numeric(20,2), 
nat_land numeric(20,2), 
dev_land numeric(20,2), 
ag_land numeric(20,2), 
geom geometry(multipolygon, 4326)
--drainage_geom geometry(polygon, 4326)
--geom geometry(geometrycollection, 4326),
--drainage_geom geometry(multipolygon, 4326)
)
;

-- MAKE SURE THE GEOMs ARE VALID, IF NOT UPDATE THEM WITH THE BELOW UPDATE STATEMENT
select practice_id, st_isvalid(geom) as is_valid
from datapolassess.fd_api_protection
where st_isvalid(geom) = 'f'
order by practice_id
;

update datapolassess.fd_api_protection set geom = st_makevalid(st_multi(st_buffer(st_buffer(geom,0.000001),-0.000001)))::geometry(multipolygon,4326)
where st_isvalid(geom) = false;

-- THE TRANSFORMATION TO 32618 ALSO CAUSES A SELF INTERSECTION. RUN THE BELOW TO MAKE THE NEW GEOMETRY FOR FASTER INTERSECTIONS LATER
alter table datapolassess.fd_api_protection add column geom_32618 geometry(multipolygon,32618);
update datapolassess.fd_api_protection set geom_32618 = st_transform(geom,32618);

select practice_id, st_isvalid(geom_32618) as is_valid
from datapolassess.fd_api_protection
where st_isvalid(geom_32618) = 'f'
order by practice_id
;

update datapolassess.fd_api_protection set geom_32618 = st_makevalid(st_multi(st_buffer(st_buffer(geom_32618,0.000001),-0.000001)))::geometry(multipolygon,32618)
where st_isvalid(geom_32618) = false;

-----------

select * from datapolassess.fd_api_protection;
select count(*) from datapolassess.fd_api_protection;

----------------
-- 2) INTERSECT THESE PROJECTS WITH THE COMID
-- PROTECTION WILL BE EASIER BECUASE IT IS NOT A GEOMETRY COLLECTION

----------------
-- Protection
drop table if exists datapolassess.fd_api_protection_comid;
create table datapolassess.fd_api_protection_comid
as
select distinct b.comid, b.huc12, a.practice_name			
,practice_id
,program_name
,program_id
,organization
,description
,practice_type
,created_at
,modified_at
,st_area(st_transform(a.geom,32618))/4046.86 as bmp_acres
,round((st_area(st_transform(st_multi(st_intersection(a.geom_32618, b.catchment)),32618))/4046.86)::numeric,2) as bmp_size
,'ACRES' as bmp_size_unit
,st_transform(st_multi(st_intersection(a.geom_32618, b.catchment)),4326)::geometry(multipolygon,4326) as geom
from datapolassess.fd_api_protection as a
left join spatial.nhdplus_maregion as b
on st_intersects(a.geom_32618, b.catchment)
order by practice_id, b.comid
;

alter table datapolassess.fd_api_protection_comid add constraint pk_fd_api_protection_comid primary key (comid, practice_id);

select * from datapolassess.fd_api_protection_comid;

-- RUN THESE AGAIN TO BE SURE
UPDATE datapolassess.fd_api_protection SET GEOM = ST_MAKEVALID(GEOM) where st_isvalid(geom) = False;
UPDATE datapolassess.fd_api_protection_comid SET GEOM = ST_MAKEVALID(GEOM) where st_isvalid(geom) = false;

select * from datapolassess.fd_api_protection_comid where practice_id = 51769;

----------------
--Restoration
drop table if exists datapolassess.fd_api_restoration_comid;
create table datapolassess.fd_api_restoration_comid
as
-- points
select b.comid, b.huc12, 
practice_name
,practice_id
,program_name
,program_id
,organization
,description
,practice_type
,created_at
,modified_at
,round(tn/count(practice_id) over (partition by practice_id),2) as tn
,round(tp/count(practice_id) over (partition by practice_id),2) as tp
,round(tss/count(practice_id) over (partition by practice_id),2) as tss
,NULL as bmp_size
,'NA, POINT' as bmp_size_unit
,st_transform(ST_ForceCollection(a.geom),4326)::geometry(geometrycollection,4326) as geom
from (
	select practice_name
		,practice_id
		,program_name
		,program_id
		,organization
		,description
		,practice_type
		,created_at
		,modified_at
		,tn
		,tp
		,tss
		,st_area(st_transform(geom,32618))/4046.86 as bmp_acres
		,st_transform(st_makevalid(st_multi(st_collectionextract(geom,1))),32618)::geometry(multipoint,32618) as geom
	from datapolassess.fd_api_restoration
	where st_transform(st_makevalid(st_multi(st_collectionextract(geom,1))),32618)::geometry(multipoint,32618) != '01040000206A7F000000000000'
	order by practice_id
) as a
left join spatial.nhdplus_maregion as b
	on st_intersects(st_transform(a.geom,32618), b.catchment)

UNION ALL
-- lines
select comid,huc12, 
practice_name
	,practice_id
	,program_name
	,program_id
	,organization
	,description
	,practice_type
	,created_at
	,modified_at
	,round((tn * (bmp_comid_feet/bmp_feet)::numeric),2) as tn
	,round((tp * (bmp_comid_feet/bmp_feet)::numeric),2) as tp
	,round((tss * (bmp_comid_feet/bmp_feet)::numeric),2) as tss
	,round(bmp_comid_feet,2) as bmp_size
	,'LINEAR FEET' as bmp_size_unit
	,st_transform(ST_ForceCollection(clipped.geom),4326)::geometry(geometrycollection,4326) as geom
from (
	select b.comid, b.huc12, 
	practice_name
	,practice_id
	,program_name
	,program_id
	,organization
	,description
	,practice_type
	,created_at
	,modified_at
	,tn
	,tp
	,tss
	,bmp_feet
	,(st_length(st_transform(st_multi(st_intersection(st_transform(a.geom,32618), b.catchment)),32618)::geometry(MultiLineString,32618))*3.28084)::numeric as bmp_comid_feet
	,st_transform(st_multi(st_intersection(st_transform(a.geom,32618), b.catchment)),32618)::geometry(MultiLineString,32618) as geom
	from (
		select practice_name
			,practice_id
			,program_name
			,program_id
			,organization
			,description
			,practice_type
			,created_at
			,modified_at
			,tn
			,tp
			,tss
			,st_length(st_transform(geom,32618))*3.28084 as bmp_feet
			,st_collectionextract(st_transform(st_makevalid(st_multi(st_collectionextract(geom,2))),32618),2)::geometry(MultiLineString,32618) as geom
		from datapolassess.fd_api_restoration
		where st_collectionextract(st_transform(st_makevalid(st_multi(st_collectionextract(geom,2))),32618),2)::geometry(MultiLineString,32618) != '01050000206A7F000000000000'
		order by practice_id
	) as a
	left join spatial.nhdplus_maregion as b
		on st_intersects(st_transform(a.geom,32618), b.catchment)
) as clipped

UNION ALL
-- polygons
select comid,huc12, 
practice_name
	,practice_id
	,program_name
	,program_id
	,organization
	,description
	,practice_type
	,created_at
	,modified_at
	,round((tn * (bmp_comid_acres/bmp_acres)::numeric),2) as tn
	,round((tp * (bmp_comid_acres/bmp_acres)::numeric),2) as tp
	,round((tss * (bmp_comid_acres/bmp_acres)::numeric),2) as tss
	,round(bmp_comid_acres,2) as bmp_size
	,'ACRES' as bmp_size_unit
	,st_transform(ST_ForceCollection(clipped.geom),4326)::geometry(geometrycollection,4326) as geom
from (
	select b.comid, b.huc12, 
	practice_name
	,practice_id
	,program_name
	,program_id
	,organization
	,description
	,practice_type
	,created_at
	,modified_at
	,tn
	,tp
	,tss
	,bmp_acres
	,(st_area(st_transform(st_multi(st_intersection(st_transform(a.geom,32618), b.catchment)),32618)::geometry(multipolygon,32618))/4046.86)::numeric as bmp_comid_acres
	,st_transform(st_multi(st_intersection(st_transform(a.geom,32618), b.catchment)),32618)::geometry(multipolygon,32618) as geom
	from (
		select practice_name
			,practice_id
			,program_name
			,program_id
			,organization
			,description
			,practice_type
			,created_at
			,modified_at
			,tn
			,tp
			,tss
			,st_area(st_transform(geom,32618))/4046.86 as bmp_acres
			,st_collectionextract(st_transform(st_makevalid(st_multi(st_collectionextract(geom,3))),32618),3)::geometry(multipolygon,32618) as geom
		from datapolassess.fd_api_restoration
		where st_collectionextract(st_transform(st_makevalid(st_multi(st_collectionextract(geom,3))),32618),3)::geometry(multipolygon,32618) != '01060000206A7F000000000000'
		order by practice_id
	) as a
	left join spatial.nhdplus_maregion as b
		on st_intersects(st_transform(a.geom,32618), b.catchment)
) as clipped

order by practice_id, comid
;

-- MUST ADD bmp_size_unit to PKEY BECAUSE IT IS A GEOM COLLECTION
alter table datapolassess.fd_api_restoration_comid add constraint pk_fd_api_restoration_comid primary key (comid, practice_id, bmp_size_unit);

select comid,huc12,practice_name,practice_id,program_name,program_id,organization,description,practice_type,created_at,modified_at,bmp_size,bmp_size_unit,ST_AsGeoJson(geom) as geom from datapolassess.fd_api_protection_comid order by practice_id, comid;
select comid,huc12,practice_name,practice_id,program_name,program_id,organization,description,practice_type,created_at,modified_at,tn,tp,tss,bmp_size,bmp_size_unit,ST_AsGeoJson(geom) as geom from datapolassess.fd_api_restoration_comid order by practice_id, comid;

select sum(tp) from datapolassess.fd_api_restoration_comid;

select sum(tp_reduced_kg) from datapolassess.fd_api_restoration_lbsreduced_comid where source in ('Delaware Watershed Conservation Fund','Delaware River Operational Fund','Delaware River Restoration Fund');

-- RUN THESE ONE MORE TIME JUST TO MAKE SURE
UPDATE datapolassess.fd_api_restoration SET GEOM = ST_MAKEVALID(GEOM) where st_isvalid(geom) = False;
UPDATE datapolassess.fd_api_restoration_comid SET GEOM = ST_MAKEVALID(GEOM) where st_isvalid(geom) = false;

-- 3) PULL THESE BMPS INTO PARQUET
-- python pull_bmps_from_database.py

-- 4) MAKE SURE THESE TABLES ARE UPDATED
-- .\pollution-assessment\stage2\FieldDoc_API\padep_bmps.sql
select * from datapolassess.fd_api_restoration_lbsreduced_comid;

-- .\pollution-assessment\stage2\FieldDoc_API\protection_get_future_landuse.sql
select * from datapolassess.fd_api_protection_lbsavoided_comid;







