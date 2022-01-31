import pandas as pd
import requests
import json
from simple_salesforce import Salesforce

class EAQuery():
    def __init__(self, username, password, version = 53.0):
        """
        create salesforce login session instance using simple_salesforce.
        params:
            username: salesforce login username. Tableau CRM lisence is needed to use methods of this module.
            password: salesforce login password.
            version: Tableau CRM REST API version. default version is 53.
        """
        # create session instance
        self.sf = Salesforce(username=username,
                             password=password,
                             security_token=""
                            )
        self.instance_url = "https://" + self.sf.sf_instance 
        self.ea_url = self.instance_url + "/services/data/v{}/wave".format(version)
    def __get_data_id(self, dataname):

        params = {'pageSize': 50, 
                  'sort': 'Mru', 
                  'hasCurrentOnly': 'true', 
                  'q': dataname}
        dataset_json = requests.get(self.ea_url + '/datasets', headers=self.sf.headers, params=params) 
        return dataset_json
    
    def read_saql(self, dataname, query):
        """
            submit SAQL query to Tableau CRM datasets.
            dataset name must be specified in order to query.

            params:
                dataname: dataset name to query.
                query: SAQL query. DO NOT include 'load' statement.
                       query variable in SAQL query must be 'q'
                       EG: 'foreach q generate ...'
            returns:
                pandas DataFrame
        """
        # Tableau CRMデータセットにクエリする場合は「q = load "データセットID/バージョンID"」の形式で記述する必要がある
        dataset_json = self.__get_data_id(dataname)
        dsid = json.loads(dataset_json.text)["datasets"][0]['id']
        dsversion = json.loads(dataset_json.text)["datasets"][0]['currentVersionId']
        
        load_ds = dsid + "/" + dsversion
        load_saql = """q = load \"{0}\";""".format(load_ds)
        
        saql = load_saql + query
        r = requests.post(
                self.ea_url+"/query", 
                headers = self.sf.headers, 
                data = json.dumps({"query":saql})
        )

        return pd.json_normalize(json.loads(r.text)['results']['records'])
    
    def read_sql(self, query):
        """
            pandas read_sql like function to query Tableau CRM dataset using SQL.
            params:
                query: SQL query to access Tableau CRM dataset.
            returns:
                pandas DataFrame
        """
        r = requests.post(
                self.ea_url+"/query", 
                headers = self.sf.headers, 
                data = json.dumps(
                            {
                             "query":query,
                             "queryLanguage":"SQL"
                            }
                )
        )
        return pd.json_normalize(json.loads(r.text)['results']['records'])
    def _get_recipe_info(self):
        params = {
            'pageSize': 50, 
            'sort': 'Mru', 
            'hasCurrentOnly': 'true'
        }
        r = requests.get(
                self.ea_url+ "/recipes", 
                headers = self.sf.headers,
                params = params
            )
        return json.loads(r.text)["recipes"]
    def get_all_recipe(self):
        """
            get all json recipe definitions which are created in Tableau CRM.
            returns: list
        """
        params = {
            'pageSize': 50, 
            'sort': 'Mru', 
            'hasCurrentOnly': 'true'
        }
        recipe_infos = self._get_recipe_info()
        recipe_dict = {col["id"]: col["label"] for col in recipe_infos}
        params = {'pageSize': 50, 'sort': 'Mru', 'hasCurrentOnly': 'true'}
        li = []
        for key, val in recipe_dict.items():
            r = requests.get(
                    self.ea_url+ "/recipes/"+key+"/file", 
                    headers = self.sf.headers,
                    params = params
                )
            res = {
                "id": key,
                "label":val,
                "recipe":json.loads(r.text)
            }
            li.append(res)
        return li
