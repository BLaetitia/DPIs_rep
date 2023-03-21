# -*- coding: utf-8 -*-
"""
Created on Mon Dec 19 12:38:26 2022

@author: samso
"""

import pandas as pd
import numpy as np
import time


marks = pd.read_csv('marks.csv')
location = pd.read_csv('location.csv')
course = pd.read_csv('course.csv')
entity= pd.read_csv('entity.csv')
grade = pd.read_csv('grade.csv')
period = pd.read_csv('period.csv')
class_group = pd.read_csv('class_group.csv')
assessment_types = pd.read_csv('assessment_types.csv')
staff = pd.read_csv('staff.csv')
staff_registration = pd.read_csv('staff_registration.csv')


#add the class grade to the dataset
marks_ = marks.merge(class_group[['id','grade_id','school_id','tag_name']],
                     left_on='classGroupId',right_on='id',how='left')
#drop unnecessary columns
marks_.drop(columns=['id_x','unitId','lessonId','total_marks','id_y','approved_by',
                     'approved_at',],inplace=True)

marks_ = marks_.merge(grade[['id','name']], left_on='grade_id',right_on='id',
                      how='left')
#rename to be easier to understand data field
marks_.rename(columns={'name':'grade','tag_name':'stream'},inplace=True)
#drop unnecessary columns
marks_.drop(columns=['id','grade_id'],inplace=True)

#add subject to the dataset
marks_ = marks_.merge(course[['id','name']],left_on='courseId',right_on='id',
                      how='left')
#rename
marks_.rename(columns={'name':'course'},inplace=True)
#drop
marks_.drop(columns=['id','courseId'],inplace=True)

#add academic year and term, starting with academic year
marks_ = marks_.merge(period[['id','name']],left_on='academicYearId', right_on='id',
                      how='left')
#rename
marks_.rename(columns={'name':'academic_year'},inplace=True)
#drop
marks_.drop(columns=['id','academicYearId'],inplace=True)
#do the same for academic term
marks_ = marks_.merge(period[['id','name']],left_on='academicTermId', right_on='id',
                      how='left')
#rename
marks_.rename(columns={'name':'academic_term'},inplace=True)
#drop
marks_.drop(columns=['id','academicTermId'],inplace=True)

#add in assessment type
marks_ = marks_.merge(assessment_types[['id','title']],left_on='assessmentType',
                      right_on='id',how='left')
marks_.rename(columns={'title':'assessment_type'},inplace=True)
marks_.drop(columns=['id','assessmentType'],inplace=True)

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
location_.rename(columns={'code':'province_code','id':'province_id'},inplace=True)

location_.to_csv(r'location_.csv', index = False)
#add in school name and location
marks_ = marks_.merge(entity[['id','code','full_name','location_id']],left_on='school_id',
                      right_on='id',how='left')

marks_.drop(columns=['status','school_id','id'],inplace=True)
marks_.rename(columns={'full_name':'school_name','code':'school_code'},inplace=True)

#add location
marks_= marks_.merge(location_,left_on='location_id',right_on='id_village',
                     how='left')

#add teacher name 
marks_ = marks_.merge(staff_registration[['id','staff_id']],left_on='operator',right_on='id',
                      how='left')

marks_.drop(columns=['id'],inplace=True)

marks_ = marks_.merge(staff[['id','names']],left_on='staff_id',right_on='id',
                      how='left')

marks_.drop(columns=['id'],inplace=True)

#we now have dataset we can summarize
#submissions per grade
table1 = pd.pivot_table(marks_[(marks_['academic_term']=='Term 1/2022/23') | (marks_['academic_term']=='Term 2/2022/23')],
                        values='created_at',index=['grade'],
                        columns=['assessment_type'],aggfunc=np.count_nonzero)
table1.reset_index(inplace=True)
#submissions per district
table2 = pd.pivot_table(marks_[marks_['academic_term']=='Term 1/2022/23'],
                        values='created_at',index=['province','district'],
                        columns=['assessment_type'],aggfunc=np.count_nonzero)
table2.reset_index(inplace=True)
#submissions per subject per school
table3 = pd.pivot_table(marks_[marks_['academic_term']=='Term 1/2022/23'],
                        values='created_at',index=['province','district','code_district',
                                                   'school_name','grade','stream','classGroupId',
                                                   'names','course'],
                        columns=['assessment_type'],aggfunc=np.count_nonzero)

table3.reset_index(inplace=True)
table3.fillna(0,inplace=True)
columns = list(table3.columns[-6:])
table3['total_submissions'] = table3[columns].sum(axis=1)

marks_.fillna('',inplace=True)

# table1.to_csv('submissions per grade '+today+'.csv')
# table2.to_csv('submissions per district ' + today+'.csv')
table3.to_csv(r'table3.csv', index = False)
marks_.to_csv(r'marks_.csv', index = False)






