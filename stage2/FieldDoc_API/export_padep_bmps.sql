
select * from datapolassess.padep_bmps;

select * from datapolassess.pa_nj_countyestimates;
select * from datapolassess.pa_nj_countyestimates_x_comid_bmprates;

-- 

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

select "id"
,"project no."
,"county"
,"lat"
,"lon"
,"date"
,"bmp name"
,"bmp/practice"
,"extent"
,"units"
,"extent2"
,"units2"
,"program"
,"location"
,"project description"
,"county_verify"
,"tn_lbs_reduced"
,"tp_lbs_reduced"
,"tss_lbs_reduced"
,"comid"
from datapolassess.padep_bmps where county = county_verify and comid is not null;

select * from datapolassess.pa_nj_countyestimates order by state, county;

select * from datapolassess.padep_devload_reduction where statefp in ('42','34') and (tn_dev_reduction_lbs + tp_dev_reduction_lbs + tss_dev_reduction_lbs) > 0.0;
select * from datapolassess.padep_agload_reduction where statefp in ('42','34') and (tn_ag_reduction_lbs + tp_ag_reduction_lbs + tss_ag_reduction_lbs) > 0.0;




