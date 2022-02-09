import pandas as pd
import requests
import json
from simple_salesforce import Salesforce

class EAQuery():
    def __init__(self, username, password, version = 53.0, domain = None):
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
                             security_token="",
                             domain = domain
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
                     each value of the list is python dictionary variable defined as following:
                     {
                        "id":"recipe id",
                        "label": "recipe label name",
                        "recipe": json formatted recipe definition
                     }
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

    def upload_df(
        self, 
        df, 
        dataset_api_name, 
        xmd=None, 
        encoding='UTF-8', 
        operation='Overwrite', 
        useNumericDefaults=True, 
        default_measure_val="0.0", 
        max_request_attempts=3,
        default_measure_fmt="0.0#", 
        charset="UTF-8", 
        deliminator=",", 
        lineterminator="\r\n", 
        removeNONascii=True, 
        ascii_columns=None, 
        fillna=True, 
        dataset_label=None, 
        verbose=False):
        '''
            field names will show up exactly as the column names in the supplied dataframe
        '''

        if verbose == True:     
            start = time.time()
            print('Loading Data to Einstein Analytics...')
            print('Process started at: '+str(self.get_local_time()))

        dataset_api_name = dataset_api_name.replace(" ","_")

        if fillna == True:
            for c in df.columns:
                if df[c].dtype == "O":
                    df[c].fillna('NONE', inplace=True)
                elif np.issubdtype(df[c].dtype, np.number):
                    df[c].fillna(0, inplace=True)
                elif df[c].dtype == "datetime64[ns]":
                    df[c].fillna(pd.to_datetime('1900-01-01 00:00:00'), inplace=True)


        if ascii_columns is not None:
            self.remove_non_ascii(df, columns=ascii_columns)
        elif removeNONascii == True:
            self.remove_non_ascii(df)

        
        # Upload Config Steps
        if xmd is not None:
            xmd64 = (base64.urlsafe_b64encode(
                    json.dumps(
                            xmd
                        )
                        .encode(
                            encoding
                        )
                )
                .decode()
            )
        else:
            xmd64 = (base64.urlsafe_b64encode(
                        self.create_xmd(
                                df, 
                                dataset_api_name, 
                                useNumericDefaults = useNumericDefaults, 
                                default_measure_val = default_measure_val, 
                                default_measure_fmt = default_measure_fmt, 
                                charset = charset, 
                                deliminator = deliminator, 
                                lineterminator = lineterminator
                            ).encode(encoding)
                    ).decode()
            
            )
        upload_config = {
                        'Format' : 'CSV',
                        'EdgemartAlias' : dataset_api_name,
                        'Operation' : operation,
                        'Action' : 'None',
                        'MetadataJson': xmd64
                    }


        r1 = requests.post(
                self.env_url+'/services/data/v46.0/sobjects/InsightsExternalData', 
                headers=self.header, 
                data=json.dumps(upload_config)
            )

        try:
            json.loads(r1.text)['success'] == True
        except: 
            logging.error(' Upload Config Failed', exc_info=True)
            logging.error(r1.text)
            sys.exit(1)
        
        if verbose == True:
            print('Upload Configuration Complete...')
            print('Chunking and Uploading Data Parts...')

        
        MAX_FILE_SIZE = 10 * 1000 * 1000 - 49
        df_memory = sys.getsizeof(df)
        rows_in_part = math.ceil(df.shape[0] / math.ceil(df_memory / MAX_FILE_SIZE))

        partnum = 0
        range_start = 0
        max_data_part = rows_in_part

        for chunk in range(0, math.ceil(df_memory / MAX_FILE_SIZE)):
            df_part = df.iloc[range_start:max_data_part,:]
            if chunk == 0:
                data_part64 = base64.b64encode(
                        df_part.to_csv(
                            index = False, 
                            quotechar = '"', 
                            quoting = csv.QUOTE_MINIMAL
                        ).encode('UTF-8')
                    ).decode()
            else:
                data_part64 = base64.b64encode(
                        df_part.to_csv(
                                index = False, 
                                header = False, 
                                quotechar = '"',
                                quoting = csv.QUOTE_MINIMAL
                            ).encode('UTF-8')
                    ).decode()
            
            range_start += rows_in_part
            max_data_part += rows_in_part
            partnum += 1
            if verbose == True:
                print('\rChunk '+str(chunk+1)+' of '+str(math.ceil(df_memory / MAX_FILE_SIZE))+' completed', end='', flush=True)

            payload = {
                "InsightsExternalDataId" : json.loads(r1.text)['id'],
                "PartNumber" : str(partnum),
                "DataFile" : data_part64
            }

            attempts = 0
            while attempts < max_request_attempts:
                try:
                    r2 = requests.post(self.env_url+'/services/data/v46.0/sobjects/InsightsExternalDataPart', headers=self.header, data=json.dumps(payload))
                    json.loads(r2.text)['success'] == True
                    break
                except: 
                    attempts += 1
                    logging.error('\n Datapart Upload Failed', exc_info=True)
                    logging.debug(r2.text)
                    
        
        if verbose == True:
            print('\nDatapart Upload Complete...')


        payload = {
                    "Action" : "Process"
                }

        attempts = 0

        while attempts < max_request_attempts:
            try:
                r3 = requests.patch(
                    self.env_url+'/services/data/v46.0/sobjects/InsightsExternalData/'+json.loads(r1.text)['id'], 
                    headers = self.header, 
                    data = json.dumps(payload)
                )
                break
            except TimeoutError as e:
                attempts += 1
                logging.debug(sys.exc_info()[0])
                logging.warning("Connection Timeout Error.  Trying again...")
        
        if verbose == True:
            end = time.time()
            print('Data Upload Process Started. Check Progress in Data Monitor.')
            print('Job ID: '+str(json.loads(r1.text)['id']))
            print('Completed in '+str(round(end-start,3))+'sec')
    def create_xmd(self, df, dataset_label, useNumericDefaults=True, default_measure_val="0.0", default_measure_fmt="0.0#", charset="UTF-8", deliminator=",", lineterminator="\r\n"):
        dataset_label = dataset_label
        dataset_api_name = dataset_label.replace(" ","_")

        fields = []
        for c in df.columns:
            if df[c].dtype == "datetime64[ns]":
                name = c.replace(" ","_")
                name = name.replace("__","_")
                date = {
                    "fullyQualifiedName": name,
                    "name": name,
                    "type": "Date",
                    "label": c,
                    "format": "yyyy-MM-dd HH:mm:ss"
                }
                fields.append(date)
            elif np.issubdtype(df[c].dtype, np.number):
                if useNumericDefaults == True:
                    precision = 18
                    scale = 2
                elif useNumericDefaults == False:
                    precision = df[c].astype('str').apply(lambda x: len(x.replace('.', ''))).max()
                    scale = -df[c].astype('str').apply(lambda x: Decimal(x).as_tuple().exponent).min()
                name = c.replace(" ","_")
                name = name.replace("__","_")
                measure = {
                    "fullyQualifiedName": name,
                    "name": name,
                    "type": "Numeric",
                    "label": c,
                    "precision": precision,
                    "defaultValue": default_measure_val,
                    "scale": scale,
                    "format": default_measure_fmt,
                    "decimalSeparator": "."
                }
                fields.append(measure)
            else:
                name = c.replace(" ","_")
                name = name.replace("__","_")
                dimension = {
                    "fullyQualifiedName": name,
                    "name": name,
                    "type": "Text",
                    "label": c
                }
                fields.append(dimension)

        xmd = {
            "fileFormat": {
                            "charsetName": charset,
                            "fieldsDelimitedBy": deliminator,
                            "linesTerminatedBy": lineterminator
                        },
            "objects": [
                        {
                            "connector": "CSV",
                            "fullyQualifiedName": dataset_api_name,
                            "label": dataset_label,
                            "name": dataset_api_name,
                            "fields": fields
                        }
                    ]
                }       
        return str(xmd).replace("'",'"')
        
    def remove_non_ascii(self, df, columns=None):
        if columns == None:
            columns = df.columns
        else:
            columns = columns

        for c in columns:
            if df[c].dtype == "O":
                df[c] = df[c].apply(lambda x: unidecode(x).replace("?",""))