
# coding: utf-8

# In[51]:

#general config
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

#AppNexus config
from link import lnk
mysql = lnk.dbs.mprod_api
vert = lnk.dbs.vertica


# In[52]:

######################## APAC Seller Member List ################
apac_members = '(7418,7209,7114,2358,7080,2741,2742,2740,2007,7151,2800,7179,6938,6899,7612,2539,3642,6860,7340,7262,7804)'
# print (apac_members)


# In[53]:

######################## List of APAC clients by Type (Direct vs. Indirect) ##########################
sql_andirect = """
SELECT m.id, billing_name, CASE WHEN (mg.created_on IS NOT NULL) THEN "AN Direct" ELSE "Indirect Supply" END as SMG,
date_format(mg.created_on,'%m/%d/%Y') as Add_Date
FROM api.member m LEFT JOIN common.member_seller_member_group mg on m.id=mg.member_id
WHERE m.id IN """ + apac_members + """
ORDER BY SMG;;
"""

df = mysql.select(sql_andirect).as_dataframe()
df


# In[56]:

#Connects to Vertica. Get top 10 domains (by Geo) from each Seller last 30 days
top10_sql="""
SELECT * FROM 
(
SELECT geo_country, m.billing_name, site_domain, sum(imps_seen) AS imps, 
to_char(sum(imps_blacklist_or_fraud)/sum(imps_seen),'0.99') as 'blacklist_%',
CASE 
WHEN audit_type IN (2,3) THEN 'Audited'
WHEN audit_type IN (1,0) THEN 'Unaudited'
END as audit_status, expose_domains,
RANK() OVER (PARTITION BY m.billing_name ORDER BY sum(imps_seen) DESC) AS imp_rank
FROM agg_platform_inventory_availability fact JOIN sup_api_member m ON fact.seller_member_id = m.id
WHERE 
  ymd >= now() - interval '31 days'
  AND seller_member_id IN """ + apac_members + """
  AND   geo_country IN ('JP','IN','CN','ID','PH','TW','TH','KR','MY','VN','HK','SG')
  -- AND site_domain <> '---'
GROUP BY 1,2,3
) ranked
WHERE imp_rank <= 10 -- top 10
ORDER BY billing_name, imp_rank asc;
"""

df = vert.select(top10_sql).as_dataframe()
df
#df.head() #show top rows #df.name #select 1 column 'name'


# In[ ]:

##### % of Unaudited Inventory by Seller 



# In[46]:

###
sql_audit = """
SELECT a.url, a.list, a.audit_status, a.auditor_notes, a.reason as reason_id, b.reason 
FROM api.inventory_url a, audit.domain_reason b 
WHERE a.reason=b.id AND a.deleted=0 AND a.url like '%domain.com%';
"""
df = mysql.select(sql_audit).as_dataframe()
df


# In[47]:

#### DEFINE MEMBER ID HERE
member_id = 

sql_vp ="""
SELECT VP.member_id as 'Seller Member ID', VPM.buyer_member_id as 'Buyer Member ID', M.name as 'Buyer Name', 
(CASE WHEN VPM.expose_publishers = 1 then 'Exposed' else 'Hidden' end) as 'Publisher', 
VPM.url_exposure as 'URLS', 
(CASE WHEN VPM.expose_universal_categories = 1 then 'Exposed' else 'Hidden' end) as 'Universal Categories', 
VPM.expose_custom_categories as 'Custom Categories', 
(CASE WHEN VPM.expose_tags = 1 then 'Exposed' else 'Hidden' end) as 'Placements' 
FROM visibility_profile VP LEFT JOIN common.visibility_profile_member VPM on VP.id = VPM.visibility_profile_id 
LEFT JOIN bidder.member M on VPM.buyer_member_id = M.id 
WHERE VP.member_id = 'field1' AND VPM.deleted = 0
"""
df = mysql.select(sql_vp).as_dataframe()
df


# In[ ]:

#### % of Blacklisted Impressions


# In[ ]:

### SPO Activity
"""SELECT
    CASE WHEN site_domain='---' THEN application_id ELSE site_domain END as site_app,
    CASE WHEN site_domain = '---' THEN 'application' ELSE 'site' END as type,
    seller_member_id,
    billing_name,
    TO_CHAR(SUM(imps_seen),'99,999,999,999') as imps_seen,
    TO_CHAR(SUM(CASE WHEN rtb_eligible='f' THEN imps_seen ELSE 0 END),'99,999,999,999') as 'RTB_eligible=f',
    TO_CHAR(SUM(CASE WHEN rtb_eligible='t' THEN imps_seen ELSE 0 END),'99,999,999,999') as 'RTB_eligible=t',
    TO_CHAR(SUM(CASE WHEN rtb_eligible='t' THEN imps_seen ELSE 0 END)/SUM(imps_seen)*100,'990%') as demand_access_rate
FROM
    agg_platform_inventory_availability
    LEFT JOIN sup_api_member ON id = seller_member_id
WHERE
    ymd = date(getdate()-1)
    AND (site_domain = '$1' OR application_id = '$1')
GROUP BY    1,2,3,4
ORDER BY    5 desc LIMIT 20;"""

