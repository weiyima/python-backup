#Intro
#Welcome comrade to the level one training module!


#1. Here we load the necessary Libraries for stuff to work, keep this part always just to be sure
from link import lnk
import pandas as pd
import argparse as ap
from anxtools import *
import sys

#2. Here we declare the links to the databases. This is the part that links to our .link config file. If this doesn't work, the login credentials are shot in the link file. This is what I meant in the set up part of the wiki. It is important that we use the same link names so our scripts work for everyone.
vertica = lnk.dbs.vertica
prod = lnk.dbs.mysql_prod_api

#2. Here is an example of an SQL query which i called camps. You can change the variable name and then change the query inside of the three """. Here I am getting campaign IDs from Advertisers.
camps = prod.select_dataframe("""select id from bidder.campaign where deleted=0 and advertiser_id = 6076 and member_id = 364;""")



#3. Here we are adding a vertica query. You could add more SQL queries by just copy pasting the above query, or multiply vertica queries by copy pasting the vertica query below. I called this query vertdata, b ut you can call it whatever you like. the 'vertica' at the beginning references the previous link we did in point 2. So it knows how to connect to vertica. Again, you can change the query inside of the three """.
vertdata = vertica.select_dataframe("""select campaign_id as 'campaign id', 
segment_id as 'segment ID', 
sum(imps) as Impressions, 
sum(clicks) as Clicks, 
sum(post_view_convs) as 'PV Convs', 
sum(post_click_convs) as 'PC Convs' 
from agg_dw_targeted_segments 
where ymdh>= date 'yesterday' - interval '3 days'
group by 1,2;""")



#4. Now we need to merge the SQL query with the Vertica Query. Usually you'd extract all the IDs from the SQL, format them into a line and comma seperated and then inserted in the vertica query. As you can see we didnt do that and just pulled a vertica query that pulled the segment data for everything globally. They system can take it for now, but in the next lesson, we will talk about how to smartly limit the scope of the vertica query to speed up the script. Since we are merging the SQL query with the vertica query, it will have the exact same effect as if I would have taken the IDs and built them into the vertica query manually.
vertcamps = vertdata.merge(camps, how='inner', left_on='campaign id', right_on='id')

#Since both the SQL query and the Vertica query had campaign ID as a column, my freshly joined dataframe has 2 campaign ID columns. With the below command I am deleting one of them.
del vertcamps['id']



#5. Export data into a CSV. This will be in your greenhouse dev and you can run gpull to retrieve it to your desktop.

vertcamps.to_csv('PeanutButterSegmentTime.csv', index=False)

#Homework for Level One
# 1.The above script will only have IDs but no names. Modify the script so your final output will also mention campaign names and segment names
# 2.Create your own script based on a query you usually do manually

