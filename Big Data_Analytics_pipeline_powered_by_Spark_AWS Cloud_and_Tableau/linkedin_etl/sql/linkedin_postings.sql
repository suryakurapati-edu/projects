SELECT distinct
    jp.*,
    zsm.state,
    i.industry_name,
    s.skill_name
FROM postings jp
INNER JOIN (select zipcode, max(state) state from 
zip_state_mapping group by zipcode) zsm ON jp.zip_code = zsm.zipcode
INNER JOIN (
select job_id, a.industry_id, industry_name from (
select job_id, max(industry_id) industry_id from job_industries group by 1
) a
join industries b
on a.industry_id = b.industry_id) i
on jp.job_id = i.job_id
INNER JOIN (
select job_id, a.skill_abr, skill_name from (
select job_id, max(skill_abr) skill_abr from job_skills group by 1
) a
join skills b
on a.skill_abr = b.skill_abr) s
on jp.job_id = s.job_id