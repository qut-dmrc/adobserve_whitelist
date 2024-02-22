import requests
import json

response = requests.get("https://api.crowdtangle.com/lists/[insert list ID]/accounts?token=[insert token]&count=100").json()["result"]
accounts = []
accounts.extend(response["accounts"])
while "pagination" in response.keys() and "nextPage" in response["pagination"].keys():
    print(response["pagination"]["nextPage"])
    response = requests.get(response["pagination"]["nextPage"]).json()["result"]
    accounts.extend(response["accounts"])
with open("accounts_CT.json",'w') as f:
    json.dump(accounts,f)