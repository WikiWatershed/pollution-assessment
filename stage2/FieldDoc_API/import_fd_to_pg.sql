
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

select * from datapolassess.fd_api_protection;
select count(*) from datapolassess.fd_api_protection;

----------------
-- 2) INTERSECT THESE PROJECTS WITH THE COMID
-- PROTECTION WILL BE EASIER
select * from datapolassess.fd_api_restoration where tp is not null limit 50;

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
--,st_area(st_transform(a.geom,32618))/4046.86 as bmp_acres
,round((st_area(st_transform(st_multi(st_intersection(st_transform(a.geom,32618), b.catchment)),32618))/4046.86)::numeric,2) as bmp_size
,'ACRES' as bmp_size_unit
,st_transform(st_multi(st_intersection(st_transform(a.geom,32618), b.catchment)),4326)::geometry(multipolygon,4326) as geom
from datapolassess.fd_api_protection as a
left join spatial.nhdplus_maregion as b
on st_intersects(st_transform(a.geom,32618), b.catchment)
order by practice_id, b.comid
;

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
			,st_transform(st_makevalid(st_multi(st_collectionextract(geom,2))),32618)::geometry(MultiLineString,32618) as geom
		from datapolassess.fd_api_restoration
		where st_transform(st_makevalid(st_multi(st_collectionextract(geom,2))),32618)::geometry(MultiLineString,32618) != '01050000206A7F000000000000'
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
			,st_transform(st_makevalid(st_multi(st_collectionextract(geom,3))),32618)::geometry(multipolygon,32618) as geom
		from datapolassess.fd_api_restoration
		where st_transform(st_makevalid(st_multi(st_collectionextract(geom,3))),32618)::geometry(multipolygon,32618) != '01060000206A7F000000000000'
		order by practice_id
	) as a
	left join spatial.nhdplus_maregion as b
		on st_intersects(st_transform(a.geom,32618), b.catchment)
) as clipped
order by practice_id, comid
;

alter table datapolassess.fd_api_protection_comid add constraint pk_fd_api_protection_comid primary key (comid, practice_id);
alter table datapolassess.fd_api_restoration_comid add constraint pk_fd_api_restoration_comid primary key (comid, practice_id);


select comid,huc12,practice_name,practice_id,program_name,program_id,organization,description,practice_type,created_at,modified_at,bmp_size,bmp_size_unit,ST_AsGeoJson(geom) as geom from datapolassess.fd_api_protection_comid order by practice_id, comid;

select comid,huc12,practice_name,practice_id,program_name,program_id,organization,description,practice_type,created_at,modified_at,tn,tp,tss,bmp_size,bmp_size_unit,ST_AsGeoJson(geom) as geom from datapolassess.fd_api_restoration_comid order by practice_id, comid;

-- 3) PULL THESE BMPS INTO PARQUET
-- python pull_bmps_from_database.py










