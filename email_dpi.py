# -*- coding: utf-8 -*-
"""
Created on Tue Jan 17 23:03:49 2023

@author: samso
"""
import yagmail
import pandas as pd
import glob
from datetime import datetime

yag = yagmail.SMTP('samson.mbugua63@gmail.com','yewqmcvjspajspii')


#construct the email body
email_text1 = ['Hi,\n Please find attached the 4 datasets you need to get started with inspection:\n'] 
email_text2 = ['1.Summary of District Registration on CAMIS (this gives you a summary of how many teachers have registered on CAMIS for the whole district)\n']
email_text3 = ['2. Details of Registration per School on CAMIS (this gives you per school details of what percentage of teachers have not registered on CAMIS. Please start inspection with those schools that are indicated as below 10%\n']
email_text4 = ['3. Summary of district assessment submissions to CAMIS\n']
email_text5 = ['4. Details per school of how many assessments each school has submitted, and the percentage of submissions they have made. A lower percentage means they have submitted very few assessments.\n\n']
email_text6 = [' Happy inspection!\n\n']
#table_ = [yagmail.inline("C:/Users/samso/Documents/CAMIS_Data/image.jpg")]

email_text = email_text1 + email_text2 + email_text3 + email_text4 + email_text5 + email_text6
email_text = ''.join(email_text)

contents = [email_text]

today = datetime.now().strftime("%d_%b") #create a today variable to ensure you only send today's data

#load list of dpis, emails and their districts
dpi_list = pd.read_excel('QA STAFF CONTACT ADRESS  (1).xlsx')
#send the emails with attachments
for index, row in dpi_list.iterrows():
    #use glob to get all files for that particular district
    email_header = 'IMPORTANT: Inspection Data for ' + today
    filenames = glob.glob('*'+row['District']+'*'+today+'*')
    if len(filenames) > 0:
        yag.send(row['EMAIL ADRRESS'],email_header,contents,attachments=filenames)
        


#yag.send('benlae01@gmail.com','test5',email_text,attachments=filenames)