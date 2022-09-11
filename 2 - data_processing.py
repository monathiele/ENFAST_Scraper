
import pandas as pd
import numpy as np
from datetime import datetime
pd.options.mode.chained_assignment = None

# Load Data
scraped_data=pd.read_json('mostwanted.json', lines=True, encoding='utf-8',convert_dates=False)
provided_data = pd.read_csv('2021-06-25T130338_eu_most_wanted.csv',keep_default_na=False, converters={'crime': eval})

# scraped data
# Name partition
scraped_data[['last_name','first_name']] = scraped_data['name'].str.split(', ',expand=True)
scraped_data['first_name']=scraped_data['first_name'].str.title()
scraped_data['last_name']=scraped_data['last_name'].str.title()

# Date of birth: format as in the provided data
scraped_data['dob']=scraped_data['dob'].apply(lambda x: datetime.strptime(x,'%b %d, %Y').strftime("%Y-%m-%d"))

# Reorder columns
cols=['last_name','first_name','name','crime','gender','dob','nationality','state_of_case','url']
scraped_data=scraped_data[cols]


# Compare Data
# New Persons in the actual data compared to the provided data
new_list = np.setdiff1d(scraped_data['name'],provided_data['name']).tolist()
old_list = np.setdiff1d(provided_data['name'],scraped_data['name']).tolist()
common_list = list(set(provided_data['name']).intersection(scraped_data['name']))

# For common persons, compare data
common_list_changed=list()
for person in common_list:
    scraped_person=scraped_data.loc[scraped_data['name']==person,].reset_index(drop=True)
    provided_person=provided_data.loc[provided_data['name']==person,].reset_index(drop=True)
    print(person,scraped_person.equals(provided_person))
    if scraped_person.equals(provided_person)==False:
        common_list_changed.append(person)

common_list_unchanged=list(set(common_list) - set(common_list_changed))

# Checks
len(old_list)+len(common_list_changed)+len(common_list_unchanged)==len(provided_data)
len(common_list_changed)+len(common_list_unchanged)+len(new_list)==len(scraped_data)




# Prepare output with all data (provided and new) with new column "data_comparison"
# that keeps tracking of the differences:
#
# OLD_DATA: Data in the provided file which is no longer in the scraped file
# SAME DATA (NO CHANGES): That in both files and with no changes
# SAME DATA (SOME CHANGES): That in both files but with at least one different feature
# NEW DATA: Data in the scraped file which was not in the original provided file

old_data=provided_data[provided_data['name'].isin(old_list)].reset_index(drop=True)
old_data['data_comparison']='OLD DATA'
common_data_unchanged=scraped_data[scraped_data['name'].isin(common_list_unchanged)].reset_index(drop=True)
common_data_unchanged['data_comparison']='SAME DATA (NO CHANGES)'
common_data_changed=scraped_data[scraped_data['name'].isin(common_list_changed)].reset_index(drop=True)
common_data_changed['data_comparison']='SAME DATA (SOME CHANGES)'
new_data=scraped_data[scraped_data['name'].isin(new_list)].reset_index(drop=True)
new_data['data_comparison']='NEW DATA'

comp_data=pd.concat([old_data,common_data_unchanged,common_data_changed,new_data]).reset_index(drop=True)




# Output results
# scraper Output
scraped_data.to_csv('./Outputs/scraped_data.csv', index = False, encoding='utf-8')
# Comparison Output
comp_data.to_csv('./Outputs/comp_data.csv', index = False, encoding='utf-8')
