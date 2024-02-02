import csv
import sys
import requests
import json
import pandas as pd
import argparse


class NiFiSession:
    def __init__(self, username, password):
        self.session = requests.Session()
        self.username = username
        self.password = password
        self.cookie = None
        self._authenticate()

    def _authenticate(self):
        url = "https://cic-lab.openstacklocal:8443/nifi-api/access/token"
        params = {"username": self.username, "password": self.password}
        headers = {
            "Accept": "*/*",
            "User-Agent": "Thunder Client (https://www.thunderclient.com)",
            "Content-Type": "application/x-www-form-urlencoded",
        }
        response = self.session.post(url, params=params, headers=headers, verify=False)
        self.cookie = response.cookies.get_dict().get("__Secure-Request-Token")

    def get_templates(self):
        url = "https://cic-lab.openstacklocal:8443/nifi-api/flow/templates"
        headers = {
            "Accept": "*/*",
            "User-Agent": "Thunder Client (https://www.thunderclient.com)",
            "Request-Token": str(self.cookie),
        }
        response = self.session.get(url, headers=headers, verify=False)
        return response.text


class NiFiTemplate:
    def __init__(self, templates):
        self.df = pd.json_normalize(json.loads(templates)["templates"])

    def get_template_info(self, template_name):
        filtered_df = self.df[self.df["template.name"] == template_name]
        desired_template_id = filtered_df.iloc[0]["id"]
        desired_group_id = filtered_df.iloc[0]["template.groupId"]
        return desired_template_id, desired_group_id


class NiFiInstance:
    def __init__(self, session, cookie):
        self.session = session
        self.cookie = cookie

    def instance_template(self, group_id, template_id):
        req_url = f"https://cic-lab.openstacklocal:8443/nifi-api/process-groups/{group_id}/template-instance"
        headers = {
            "Accept": "*/*",
            "User-Agent": "Thunder Client (https://www.thunderclient.com)",
            "Request-Token": str(self.cookie),
            "Content-Type": "application/json",
        }
        payload = json.dumps({"originX": 352.0, "originY": -56.0, "templateId": template_id})
        response = self.session.post(req_url, data=payload, headers=headers)
        return response.text


class NiFiProcessGroup:
    def __init__(self, session, cookie):
        self.session = session
        self.cookie = cookie

    def get_process_groups(self, group_id):
        req_url = f"https://cic-lab.openstacklocal:8443/nifi-api/process-groups/{group_id}/process-groups"
        headers = {
            "Accept": "*/*",
            "User-Agent": "Thunder Client (https://www.thunderclient.com)",
            "Request-Token": str(self.cookie),
        }
        response = self.session.get(req_url, headers=headers)
        return response.text


class NiFiVariables:
    def __init__(self, session, cookie):
        self.session = session
        self.cookie = cookie

    def get_variables(self, group_id):
        req_url = f"https://cic-lab.openstacklocal:8443/nifi-api/process-groups/{group_id}/variable-registry"
        headers = {
            "Accept": "*/*",
            "User-Agent": "Thunder Client (https://www.thunderclient.com)",
            "Request-Token": str(self.cookie),
            "Content-Type": "application/json",
        }
        response = self.session.get(req_url, headers=headers)
        return response.text

    def set_variables(self, group_id, payload):
        req_url = f"https://cic-lab.openstacklocal:8443/nifi-api/process-groups/{group_id}/variable-registry"
        headers = {
            "Accept": "*/*",
            "User-Agent": "Thunder Client (https://www.thunderclient.com)",
            "Request-Token": str(self.cookie),
            "Content-Type": "application/json",
        }
        response = self.session.put(req_url, data=payload, headers=headers)
        return response.text


class NiFiVersion:
    def __init__(self, session, cookie):
        self.session = session
        self.cookie = cookie

    def get_process_group_version(self, group_id):
        req_url = f"https://cic-lab.openstacklocal:8443/nifi-api/versions/process-groups/{group_id}"
        headers = {
            "Accept": "*/*",
            "User-Agent": "Thunder Client (https://www.thunderclient.com)",
            "Request-Token": str(self.cookie),
        }
        response = self.session.get(req_url, headers=headers)
        return json.loads(response.text)["processGroupRevision"]["version"]


def read_csv_file(file_path):
    try:
        with open(file_path, "r") as file:
            return list(csv.DictReader(file))
    except FileNotFoundError:
        print(f"Error: File '{file_path}' not found.")
        return []

def read_credentials():



def main():

    variables = read_csv_file(sys.argv[1])
    print(variables)  # Just to check if variables are read correctly

    username = "admin"
    password = "qrskrfA3wLOH079E1xT4hzVMSfpeyaRD"

    session = NiFiSession(username, password)
    templates = session.get_templates()

    template_handler = NiFiTemplate(templates)
    desired_template_id, desired_group_id = template_handler.get_template_info("SFTP2SFTP")

    instance_handler = NiFiInstance(session.session, session.cookie)
    instanced_tmpl = instance_handler.instance_template(desired_group_id, desired_template_id)

    process_group_ids = json.loads(instanced_tmpl)["flow"]["processGroups"]

    process_group_handler = NiFiProcessGroup(session.session, session.cookie)
    result = process_group_handler.get_process_groups(process_group_ids[0]["id"])
    result_dict = {
        item["component"]["name"]: item["component"]["id"] for item in json.loads(result)["processGroups"]
    }

    version_handler = NiFiVersion(session.session, session.cookie)
    version = version_handler.get_process_group_version(result_dict["EXTRACT"])

 

    # Continue with other operations as needed


if __name__ == "__main__":
    main()
