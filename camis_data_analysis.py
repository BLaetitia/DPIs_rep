# -*- coding: utf-8 -*-
"""
Created on Thu Dec 15 10:12:55 2022

@author: samso

We want to know:
1. Access
    - How many unique teachers have logged into CAMIS in one way or another 
    - How many teachers have accessed the platform via web
    - How many teachers have accessed the platform via ussd
    - How many have done both
    - Map this out by district/sector

2. Upload marks

    - Trend this over time to see when access was highest
"""
import pandas as pd
import numpy as np
import time
from datetime import datetime

# access_history = pd.read_csv('access_history.csv')
# ussd_flow = pd.read_csv('ussd_flow.csv')
users = pd.read_csv('users.csv')
# activity_logs = pd.read_csv('activity_logs.csv')
staff_registration = pd.read_csv('staff_registration.csv')
entity = pd.read_csv('entity.csv')
location = pd.read_csv('location.csv')

users['web'] = np.where(users['password'].isnull(),0,1)
users['ussd'] = np.where(users['ussd_pin'].isnull(),0,1)

table = pd.pivot_table(users, values='id', columns=['ussd'], 
                       index=['staff_type'], aggfunc=np.count_nonzero)

#layer in district
#start with getting school_id
users_merged = users.merge(staff_registration[['school_id','staff_id']], how='left', on='staff_id')
users_merged.drop_duplicates('staff_id',inplace=True)
#add in location id
users_merged = users_merged.merge(entity[['id','code','location_id']], how='left', left_on='school_id_x',
                            right_on='id')

users_merged.rename(columns={'code':'school_code'},inplace=True)                          
#process location dataset
village = location[location['location_type']=='VILLAGE'].rename(columns={'name':'village'})
cell = location[location['location_type']=='CELL'].rename(columns={'name':'cell'})
sector = location[location['location_type']=='SECTOR'].rename(columns={'name':'sector'})
district = location[location['location_type']=='DISTRICT'].rename(columns={'name':'district'})
province = location[location['location_type']=='PROVINCE'].rename(columns={'name':'province'})
#add cell
location_ = village.merge(cell[['id','cell','code','parent_id']],how='left',
                          left_on='parent_id',right_on='id',suffixes=('_village','_cell'))

location_.drop(columns=['description','location_type','parent_id_village'],inplace=True)
#add sector
location_ = location_.merge(sector[['id','sector','code','parent_id']],how='left',
                          left_on='parent_id_cell',right_on='id',suffixes=('_cell','_sector'))
location_.rename(columns={'parent_id':'parent_id_sector'},inplace=True)
#add district 
location_ = location_.merge(district[['id','district','code','parent_id']],how='left',
                          left_on='parent_id_sector',right_on='id',suffixes=('_sector','_district'))

location_.drop(columns=['parent_id_cell','parent_id_sector'],inplace=True)
location_.rename(columns={'parent_id':'parent_id_district'},inplace=True)

#add province 
location_ = location_.merge(province[['id','province','code','parent_id']],how='left',
                          left_on='parent_id_district',right_on='id',suffixes=('_district','_province'))

location_.drop(columns=['parent_id_district','parent_id'],inplace=True)

    
#get districts and analyse
users_merged = users_merged.merge(location_[['cell','village','sector','district','province','id_village']], how='left',
                                  left_on='location_id',right_on='id_village')

summary_table2 = pd.pivot_table(users_merged, values='id_x', index=['province','district'],
                        aggfunc=np.count_nonzero)

#load teachers per districts dataset
teachers_df = pd.read_excel('Number of Teachers per district.xlsx', sheet_name='District level')
teachers_df = teachers_df.merge(district[['code','district']], left_on='District_code',
                                right_on='code')
summary_table2.reset_index(inplace=True)
summary_table2 = summary_table2.merge(teachers_df[['Number of teachers','district']],how='left', on='district')
summary_table2.rename(columns={'id_x':'Teachers_Registered_on_CAMIS','Number of teachers':'Total_teachers'},inplace=True)
summary_table2['%_On'] = (summary_table2['Teachers_Registered_on_CAMIS']/summary_table2['Total_teachers'])
summary_table2['Teachers_not_registered_on_CAMIS'] = summary_table2['Total_teachers'] - summary_table2['Teachers_Registered_on_CAMIS']
today = datetime.now().strftime("%d_%b")
summary_table2.to_csv('teacher_registrations_'+today+'.csv')


#produce report on per school compliance
school_df = pd.read_excel('Number of teachers 19 12 2022.xlsx', sheet_name='Number of teachers 19 12 2022')
#create pivot with schools and teacher counts
summary_table3 = pd.pivot_table(users_merged, values=['id_x','web','ussd'], index=['school_code','village','cell','sector','district'],
                        aggfunc=np.count_nonzero)
                        
summary_table3.reset_index(inplace=True)
#merge with school df to find out how many teachers per school
summary_table3 = summary_table3.merge(school_df,how='right',on='school_code')
#set null field in number of teachers to 0
summary_table3['%_On'] = summary_table3['id_x']/summary_table3['numberofstaff']
summary_table3['Teachers_not_registered_on_CAMIS'] = summary_table3['numberofstaff'].sub(summary_table3['id_x'],axis=0)
summary_table3['numberofstaff'].fillna('school not found', inplace=True)
summary_table3.rename(columns={'id_x':'Teachers_Registered_on_CAMIS','numberofstaff':'Total_Teachers',}, inplace=True)

#for schools with missing locations, tabulate what each location means
summary_table3_ = summary_table3[['school_code', 'sector','district']].copy(deep=True)
summary_table3_['decode'] = summary_table3_['school_code'].astype(str).str[0:4]
summary_table3_.dropna(inplace=True)
summary_table3_.drop(columns='school_code',inplace=True)
summary_table3_.drop_duplicates(inplace=True)
#merge with original table to fill the unknown schools
summary_table3['decode'] = summary_table3['school_code'].astype(str).str[0:4] #create decode column
summary_table3 = summary_table3.merge(summary_table3_,on='decode',how='left') 

summary_table3.rename(columns={'sector_y':'sector','district':'district'},inplace=True)
summary_table3.drop(columns=['sector_x','district_x','decode'],inplace=True)
#sort by percentage ascending
summary_table3.sort_values(by='%_On',ascending=True,inplace=True)

#set to csv
#summary_table3.to_csv('teacher_registrations_per_school_'+today+'.csv')

#create district summary
#fill nan column with 0
summary_table3['%_On'] = summary_table3['%_On'].fillna(value=0.0)
#start by creating bins for the percentage on CAMIS
bins_ = [-0.01,0.1,0.3,0.5,0.7,100]
labels_ = ['below 10%','11%-30%','31%-50%','51%-70%','above 70%']
summary_table3['bins'] = pd.cut(x=summary_table3['%_On'], bins=bins_, labels=labels_)
#clean up Teachers not registered column
summary_table3['Teachers_not_registered_on_CAMIS'] = summary_table3['Teachers_not_registered_on_CAMIS'].fillna(value=0)

#district summary
districts = list(summary_table3.district_y.unique())

for i in districts:
    district_summary_table3 = pd.pivot_table(summary_table3[summary_table3['district_y']==i],values='school_code',
                                       index='bins',aggfunc='count')
    district_summary_table3.reset_index(inplace=True)
    district_summary_table3.rename(columns={'bins':'% of teachers registered in CAMIS in the school', 'school_code':'number of schools' }
                                   ,inplace=True)
    district_summary_table3.to_csv(i+' Teachers Registered on CAMIS Summary_'+today+'.csv')

#school summary
columns = ['district_y','sector','village','cell','school_code','school_name','Total_Teachers','Teachers_Registered_on_CAMIS','Teachers_not_registered_on_CAMIS','%_On','bins']
school_summary_table3 = summary_table3[columns]
school_summary_table3.rename(columns={'district_y':'district'},inplace=True)
#split by district so that each district has its own school dataset
districts = list(school_summary_table3.district.unique())

for i in districts:
    temp_df = school_summary_table3[school_summary_table3['district']==i]
    temp_df.to_csv(i+' Teachers Registered on CAMIS by School_'+today+'.csv')

