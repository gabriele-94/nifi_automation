import csv
import sys
import argparse

import requests
import json
import pandas as pd
from IPython.display import display

### queste funzioni possono essere eseguiti 1 sola volta per "run"
def parseArguments():
    parser = argparse.ArgumentParser(description='Process passed args')
    parser.add_argument('-file', dest='parameter_file', type=str, help="CSV file containing flow parameters")
    parser.add_argument('-user', dest='username', type=str, help="Nifi ")
    parser.add_argument('-file', dest='parameter_file', type=str, help="CSV file containing flow parameters")

def readCsvFile(filePath):
    with open(filePath, 'r') as file:
        return list(csv.DictReader(file))

def getToken():

    session = requests.Session()
    # Your request parameters
    url = "https://cic-lab.openstacklocal:8443/nifi-api/access/token"
    params = {"username": "admin", "password": "qrskrfA3wLOH079E1xT4hzVMSfpeyaRD"}
    payload = None  # Set your payload if needed
    headers_list = {
    "Accept": "*/*",
    "User-Agent": "Thunder Client (https://www.thunderclient.com)",
    "Content-Type": "application/x-www-form-urlencoded" 
    }

    # Disable SSL certificate verification
    #response = requests.post(url, params=params, verify=False, data=payload, headers=headers_list)

    response_login = session.post(url, params=params, verify=False, data=payload, headers=headers_list)

    cookie = response_login.cookies.get_dict()['__Secure-Request-Token']

    print(cookie)

    # Handle the response as needed
    print(response_login.status_code)
    #print(response.text)

    return session, cookie

def getTemplates(session, cookie):
    url = "https://cic-lab.openstacklocal:8443/nifi-api/flow/templates"
    headers_list = {
        "Accept": "*/*",
        "User-Agent": "Thunder Client (https://www.thunderclient.com)",
        "Request-Token": str(cookie)
    }

    print(headers_list)

    # Perform the GET request
    response = session.get(url, headers=headers_list, verify=False)

    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        result = response.text
        print(result)
    else:
        print(f"Error: {response.status_code} - {response.text}")

    return result

### queste funzioni devono essere eseguite per ogni flusso che si vuole istanziare

def instanceTemplate(session, cookie, groupId, templateId):

    reqUrl = "https://cic-lab.openstacklocal:8443/nifi-api/process-groups/{}/template-instance".format(groupId)

    headersList = {
    "Accept": "*/*",
    "User-Agent": "Thunder Client (https://www.thunderclient.com)",
    "Request-Token": str(cookie),
    "Content-Type": "application/json" 
    }

    payload = json.dumps({
        "originX": 352.0,
        "originY": -56.0,
        "templateId": templateId
    })

    response = session.request("POST", reqUrl, data=payload,  headers=headersList)

    print(response.text)

    result = response.text

    return result

def renameProcessGroup(session, cookie, groupId, newName):
    reqUrl = f"https://cic-lab.openstacklocal:8443/nifi-api/process-groups/{groupId}"

    headersList = {
    "Accept": "*/*",
    "User-Agent": "Thunder Client (https://www.thunderclient.com)",
    "Request-Token": str(cookie),
    "Content-Type": "application/json" 
    }

    payload = json.dumps({
        "revision":{
            "version": 0
        },
        "component":{
            "id": str(groupId),
            "name":str(newName)
        }
    })

    response = session.request("PUT", reqUrl, data=payload,  headers=headersList)

    print(response.text)
    result = response.text
    return result

def getProcessGroups(session, cookie, groupId):
    reqUrl = "https://cic-lab.openstacklocal:8443/nifi-api/process-groups/{}/process-groups".format(groupId)

    headersList = {
    "Accept": "*/*",
    "User-Agent": "Thunder Client (https://www.thunderclient.com)",
    "Request-Token": str(cookie) 
    }

    payload = ""

    response = session.request("GET", reqUrl, data=payload,  headers=headersList)

    print(response.text)

    result = response.text

    return result

def getVariables(session, cookie, groupId):

    reqUrl = "https://cic-lab.openstacklocal:8443/nifi-api/process-groups/{}/variable-registry".format(groupId)

    headersList = {
    "Accept": "*/*",
    "User-Agent": "Thunder Client (https://www.thunderclient.com)",
    "Request-Token": str(cookie),
    "Content-Type": "application/json" 
    }

    payload = ""

    response = session.request("GET", reqUrl, data=payload,  headers=headersList)

    print(response.text)
    result = response.text
    return result

### queste funzioni vengono eseguite più volte per lo stesso flusso

def setVariables(session, cookie, groupId, payload):

    reqUrl = "https://cic-lab.openstacklocal:8443/nifi-api/process-groups/{}/variable-registry".format(groupId)

    headersList = {
    "Accept": "*/*",
    "User-Agent": "Thunder Client (https://www.thunderclient.com)",
    "Request-Token": str(cookie),
    "Content-Type": "application/json" 
    }

    response = session.request("PUT", reqUrl, data=payload,  headers=headersList)

    print(response.text)
    result = response.text
    return result  

def getProcessGroupVersion(session, cookie, groupId):
    reqUrl = "https://cic-lab.openstacklocal:8443/nifi-api/versions/process-groups/{}".format(groupId)

    headersList = {
    "Accept": "*/*",
    "User-Agent": "Thunder Client (https://www.thunderclient.com)",
    "Request-Token": str(cookie) 
    }

    response = session.request("GET", reqUrl,  headers=headersList)
    print(response.text)
    result = response.text
    return result

### Fine Funzioni

session, cookie = getToken()

templates = getTemplates(session, cookie)

# Ora bisogna trovare il template di nostro interesse, ce lo dice un file che passeremo all'applicativo

template_df = json.loads(templates)
df = pd.json_normalize(template_df['templates'])

# For each ETL component in the Dictionary, we update the variables as we stated we OLNY work with them if possible.

parameters_list = readCsvFile(sys.argv[1])

print(parameters_list)

#flow_name = parameters_list[0]['Name']
desired_template = parameters_list[0]['Category']

filtered_df = df[df['template.name'] == desired_template]
#display(filtered_df)

#print(filtered_df['template.groupId'])

desired_template_id = filtered_df.iloc[0]['id']
desired_group_id = filtered_df.iloc[0]['template.groupId']

print(desired_template_id, desired_group_id)

instanced_tmpl = instanceTemplate(session, cookie, desired_group_id, desired_template_id)

instanced_tmpl_df = json.loads(instanced_tmpl)

process_group_ids = [group['id'] for group in instanced_tmpl_df['flow']['processGroups']]
print("Flow Process Groups IDs:", process_group_ids)



result = getProcessGroups(session, cookie, process_group_ids[0])

### Ora si configurano i 3 process groups per ETL

data = json.loads(result)
result_dict = {item["component"]["name"]: item["component"]["id"] for item in data["processGroups"]}

print(result_dict)

# Si può estrarre in un file esterno
column_mapping = {
    'E_readhost': 'hostsftp',
    'E_readuser': 'usersftp',
    'E_readpassword': 'passwordsftp',
    'E_readkey': 'privatekeypath',
    'E_readpassphrase': 'privatekeypassphrase',
    'E_readremotepath': 'remotepath',
    'T_selectquery': 'selecteddata',
    'L_writehost': 'hostsftp',
    'L_writeuser': 'usersftp',
    'L_writepassword': 'passwordsftp',
    'L_writekey': 'privatekeypath',
    'L_writepassphrase': 'privatekeypassphrase',
    'L_writeremotepath': 'remotepath'
}

group_category_mapping = {'E': 'EXTRACT', 'T': 'TRANSFORM', 'L': 'LOAD'}

# Si suppone che la versione di un nuovo flusso sia sempre 0, ma ce la prendiamo lo stesso.
version = json.loads(getProcessGroupVersion(session, cookie, result_dict['EXTRACT']))["processGroupRevision"]["version"]
print(version)


# Create dataframes for each category (E, T, L)
dfs = {}
for category in ['E', 'T', 'L']:
    # Filter columns based on the first letter
    columns = [col for col in parameters_list[0].keys() if col.startswith(category)]
    mapped_columns = [column_mapping[col] for col in columns]
    # Create dataframe
    dfs[category] = pd.DataFrame(parameters_list)[columns]
    dfs[category].columns = mapped_columns

print(dfs)

# Generate variables_payload JSON based on the dataframe of interest
variables_payload = {}
for group_key, df in dfs.items():
    category = group_category_mapping[group_key]
    variables_payload[category] = {
        "processGroupRevision": {"version": version},
        "variableRegistry": {
            "processGroupId": result_dict[category],
            "variables": [
                {"variable": {"name": name, "value": value.item()}} for name, value in df.items() if value.item() != ''
            ]
        }
    }

print(json.dumps(variables_payload))


for key in  result_dict:
    
    setVariables(session, cookie, result_dict[key], json.dumps(variables_payload[key]))




