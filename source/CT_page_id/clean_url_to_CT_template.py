import pandas as pd
from datetime import date
import os

def create_CT_list(filename, target_col,listname):
    desired_list = target_col
    list_name = listname
    last_list = []
    new_list = pd.read_csv(filename,encoding='cp1252')[desired_list]
    finallist = set()
    unique_accounts = list(new_list.unique()) # on-sharing
    print(len(unique_accounts))
    for account in unique_accounts:
        if type(account) != str or len(account) == 0:
            continue
        added = False
        if account[-1] == '/':
            account = account[0:-1]
        account = account.replace('https://','')
        account = account.replace('http://','')
        account = account.replace('www.','')
        if not added:
            finallist.add(account)
    print(len(finallist))
    newDF = pd.DataFrame()
    newDF['Page or Account URL'] = list(finallist)
    newDF['List'] = [list_name] * len(list(finallist))

    newDF.to_csv('CT_template_'+listname+'.csv', index=None)

filename = 'Ad monitoring targets.csv'
target_col = "Page URL" # column with facebook page urls
list_name = "whitelist_test"
create_CT_list(filename,target_col,list_name)