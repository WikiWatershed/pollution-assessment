select distinct a.practice_name, a.practice_id, a.organization, a.program_name, a.description, a.practice_type, b.bmp_name
--distinct a.practice_type, b.bmp_name
from datapolassess.fd_api_restoration as a
left join databmpapi.bmp_efficiencies as b
on a.practice_type like b.bmp_name
where b.bmp_name is null and (a.tn + a.tp + a.tss) = 0.0
order by a.practice_type;

select * from databmpapi.bmp_efficiencies order by bmp_name;

--should be 
'Animal Waste Management Systems'
-- was
'Animal Waste Management System'

-- should be 
'Barnyard Runoff Controls'
-- was
'Barnyard Runoff Control'

-- should be
'Bioretention/raingardens - A/B soils no underdrain'
-- was
'Bioretention/rain gardens - A/B soils, no underdrain'

-- should be
'Conservation Easement'
-- was
'Conservation easement'