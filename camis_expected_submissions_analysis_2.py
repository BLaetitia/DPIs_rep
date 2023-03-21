# -*- coding: utf-8 -*-
"""
Created on Tue Jan 17 16:03:04 2023

@author: samso
"""

import pandas as pd
import numpy as np
from datetime import datetime

#will need to find out how many subjects each class group is supposed to do

class_group = pd.read_csv('class_group.csv')
combination_course = pd.read_csv('combination_course.csv')
entity = pd.read_csv('entity.csv')
location = pd.read_csv('location_.csv')
grade = pd.read_csv('grade.csv')
levels = pd.read_csv('levels.csv')

#add in level to the dataset
class_group = class_group.merge(grade[['id','level_id','short_name']],left_on='grade_id',right_on='id',
                                how='left')

class_group.drop(columns=['id_y','grade_id','created_at','updated_at'],inplace=True)
class_group.rename(columns={'short_name':'grade','id_x':'class_group_id'},inplace=True)

class_group = class_group.merge(levels[['id','title','short_name']],left_on='level_id',right_on='id',
                                how='left')
class_group.rename(columns={'title':'level'},inplace=True)

#create column with subject count for each class group
#class_group['subject_count'] = class_group['combination_id'].map(combination_course['combination_id'].value_counts())

#expected submissions per class group
no_of_subjects = pd.DataFrame(data = ([0,'PPR'],[9,'PR'],[20,'OL'],[11,'AL'],[25,'TVET'],[25,'TVETF']),
                              columns=['total_subjects','level_'])
#merge with class_group to create a column with expected submissions based on level
class_group = class_group.merge(no_of_subjects,right_on='level_',left_on='short_name',
                                how='left')

class_group['expected_submissions'] = class_group['total_subjects']*2
class_group.drop(columns=['level_','short_name','id','class_size','level_id'],inplace=True)
class_group.rename(columns={'tag_name':'stream'},inplace=True)

#merge with marks_dataset
#first dedup submissions_df
submissions_df = class_group.copy(deep=True)
submissions_df.drop(columns='period_id',inplace=True)

#add location, school name, information
submissions_df = submissions_df.merge(entity[['id','code','full_name','location_id','ownership_type']],
                          left_on='school_id',right_on='id',how='left')
submissions_df.drop(columns=['school_id','id'],inplace=True)
submissions_df.rename(columns={'code':'school_code'},inplace=True)

submissions_df = submissions_df.merge(location[['id_village','village','cell','sector','district','province']],left_on='location_id',
                          right_on='id_village', how='left')
submissions_df.drop(columns=['id_village','location_id','combination_id'],inplace=True)

#prep table3 from camis_submissions_analysis
table3 = pd.read_csv('table3.csv',encoding='latin-1')
table3_merge = table3.iloc[:,-7:]
table3_merge['classGroupId'] = table3['classGroupId']

#merge the datasets to create a dataset with all the classgroups and their submissions
submissions_df = submissions_df.merge(table3_merge,how='left',left_on='class_group_id',right_on='classGroupId')

submissions_df.drop(columns=['classGroupId','class_group_id'],inplace=True)
#clean up the nulls in the last columns
submissions_df.iloc[:,-7:] = submissions_df.iloc[:,-7:].fillna(value=0)
#rearrange the columns so that it is easier to read
columns = [ 'province', 'district','sector', 'cell', 'village', 'level','school_code', 'full_name', 'ownership_type', 'stream', 
           'grade', 'total_subjects', 'Comprehensive assessment', 'Continuous Assessment', 'End of Term',
           'End of Unit', 'Projects', 'Summative assessment', 'total_submissions', 'expected_submissions']

submissions_df = submissions_df[columns]
#tabulate the percentages
submissions_df['percentage_submissions'] = submissions_df['total_submissions'] / submissions_df['expected_submissions']

#summarize the tables by district
districts = list(submissions_df.district.unique())
districts = [x for x in districts if x == x]

today = datetime.now().strftime("%d_%b") #create variable to store today's dates for csv downloads

for i in districts:
    district_summary = pd.pivot_table(submissions_df[submissions_df['district']==i],
                                      values=['total_submissions','expected_submissions'],
                                      index='level',aggfunc='sum')
    
    district_summary['%_submissions'] = district_summary['total_submissions']/district_summary['expected_submissions']
    district_summary.replace(np.inf,0,inplace=True)
    #clean up the view
    district_summary['expected_submissions'] = district_summary['expected_submissions'].map('{:,.0f}'.format)
    district_summary['total_submissions'] = district_summary['total_submissions'].map('{:,.0f}'.format)
    district_summary['%_submissions'] = district_summary['%_submissions'].map('{:.1%}'.format)
    district_summary.to_csv(i+' Submissions Summary_'+today+'.csv')

#summary submissions data for each school, per district
for i in districts:
    school_summary = pd.pivot_table(submissions_df[submissions_df['district']==i], 
                                    values=['total_submissions','expected_submissions'],
                                    index=['district','sector','cell','village','level','school_code','full_name','ownership_type'],
                                           aggfunc='sum')
    school_summary.reset_index(inplace=True)
    school_summary['%_submissions'] = school_summary['total_submissions']/school_summary['expected_submissions']
    school_summary.replace([np.inf,np.nan],0,inplace=True)
    #clean up the view
    school_summary['expected_submissions'] = school_summary['expected_submissions'].map('{:,.0f}'.format)
    school_summary['total_submissions'] = school_summary['total_submissions'].map('{:,.0f}'.format)
    school_summary['%_submissions'] = school_summary['%_submissions'].map('{:.1%}'.format)
    school_summary.to_csv(i+' Submissions Per School_'+today+'.csv')
    #national summary
national_school_summary = pd.pivot_table(submissions_df, 
                                  values=['total_submissions','expected_submissions'],
                                  index=['district','sector','cell','village','level','school_code','full_name','ownership_type'],
                                  aggfunc='sum')
national_school_summary.reset_index(inplace=True)



national_school_summary.to_csv(r'national_school_summary.csv', index = False)

