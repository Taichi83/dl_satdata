import requests as r
import getpass, time, os, cgi, json
import geopandas as gpd
import pandas as pd
from pathlib import Path
from datetime import datetime
# from app.scripts.support import df_read
from tqdm import tqdm
import numpy as np
import shutil
from support import df_read


class USGSquery(object):
    def __init__(self, api):
        self.api = api

    def PickJsonFilenm(self, json_dir, files):
        if files is None:
            p = Path(json_dir)
            p_list = list(p.glob("*.geojson"))
            self.json_files = [p_list[s].name for s in range(len(p_list))]
        else:
            self.json_files = files
        self.json_dir = json_dir

    def check_produts(self, prods=None):
        product_response = r.get('{}product'.format(self.api)).json()  # request all products in the product service
        print('AρρEEARS currently supports {} products.'.format(
            len(product_response)))  # Print no. products available in AppEEARS
        products = {p['ProductAndVersion']: p for p in
                    product_response}  # Create a dictionary indexed by product name & version
        df_products = pd.DataFrame(products)

        list_response = []
        if prods is not None:
            if type(prods) == list:
                for p in range(len(prods)):
                    df_response = pd.DataFrame(r.get('{}product/{}'.format(self.api, prods[p])).json())
                    list_response.append(df_response)
            else:
                df_response = pd.DataFrame(r.get('{}product/{}'.format(self.api, prods)).json())
                list_response.append(df_response)

        return df_products, list_response

    def request_area_single(self, json_file, user, password, prods, m_data, task_type, task_name, startDate, endDate,
                            outFormat, proj_method):
        print('start:', json_file)
        if type(prods) == list:
            if type(m_data) == list:
                layers = [(prods[s], m_data[t]) for s in range(len(prods)) for t in range(len(m_data))]
            else:
                layers = [(prods[s], m_data) for s in range(len(prods))]
        else:
            if type(m_data) == list:
                layers = [(prods, m_data[t]) for t in range(len(m_data))]
            else:
                layers = [[prods, m_data]]
        prodLayer = []
        for l in layers:
            prodLayer.append({
                "layer": l[1],
                "product": l[0]
            })
        # json_file = self.json_files[i]
        gdf_jpn = gpd.read_file(os.path.join(self.json_dir, json_file))
        jsn_data = json.loads(gdf_jpn.to_json())

        projections = r.get('{}spatial/proj'.format(self.api)).json()
        projs = {}  # Create an empty dictionary
        for p in projections:
            projs[p['Name']] = p  # Fill dictionary with `Name` as keys
        proj = projs[proj_method]['Name']

        token_response = r.post('{}login'.format(self.api), auth=(user, password)).json()
        # Insert API URL, call login service, provide credentials & return json
        del user, password  # Remove user and password information

        task_name = task_name + '_' + json_file.split(sep='.geojson')[0]
        token = token_response['token']  # Save login token to a variable
        head = {'Authorization': 'Bearer {}'.format(
            token)}  # Create a header to store token information, needed to submit a request
        task = {
            'task_type': task_type,
            'task_name': task_name,
            'params': {
                'dates': [
                    {
                        'startDate': startDate,
                        'endDate': endDate
                    }],
                'layers': prodLayer,
                'output': {
                    'format': {
                        'type': outFormat},
                    'projection': proj},
                'geo': jsn_data,
            }
        }

        task_response = r.post('{}task'.format(self.api), json=task, headers=head).json()
        # Ping API until request is complete, then continue to Section 4
        task_id = task_response['task_id']

        return task_id

    def request_area_multi(self, user, password, prods, m_data, task_type, task_name, startDate, endDate, outFormat,
                           proj_method):

        if type(self.json_files) == list:
            self.task_id = [
                self.request_area_single(self.json_files[i], user, password, prods, m_data, task_type, task_name,
                                         startDate, endDate, outFormat, proj_method) for i in
                tqdm(range(len(self.json_files)))]
        else:
            self.task_id = self.request_area_single(self.json_files, user, password, prods, m_data, task_type,
                                                    task_name,
                                                    startDate, endDate, outFormat, proj_method)

        return self.task_id

    def saveupdate_taskid(self, dir_dest, dir_taskid='/src/data/taskid', filename='tasklist'):
        df_tasks = pd.DataFrame([self.task_id], index=['task_id']).T.assign(
            datetime=datetime.now().strftime('%Y-%m-%d %H:%M:%S'), status='pending')
        if type(self.json_files) != list:
            self.json_files = [self.json_files]
        df_tasks = df_tasks.assign(dir_jsonpath=[os.path.join(self.json_dir, self.json_files[i])
                                                 for i in range(len(self.json_files))],
                                   dir_dest=[os.path.join(dir_dest, self.json_files[i].split('.geojson')[0])
                                             for i in range(len(self.json_files))])

        df_tasks = df_tasks.assign(task_name=np.nan, expires_on=np.nan)

        path = os.path.join(dir_taskid, filename + '.pkl')

        if not os.path.exists(dir_taskid):
            os.makedirs(dir_taskid)

        if os.path.exists(path):
            df_tasks_org, _ = df_read(dir_taskid, filename + '.pkl')
            if len(df_tasks_org) > 0:
                df_tasks = pd.concat([df_tasks_org, df_tasks])
        df_tasks = df_tasks.reset_index(drop=True)
        # save
        df_tasks.to_pickle(os.path.join(dir_taskid, filename + '.pkl'))
        df_tasks.to_csv(os.path.join(dir_taskid, filename + '.csv'), index=False)

    def generate_token(self):
        user = getpass.getpass(
            prompt='Enter NASA Earthdata Login Username: ')  # Input NASA Earthdata Login Username
        password = getpass.getpass(
            prompt='Enter NASA Earthdata Login Password: ')  # Input NASA Earthdata Login Password
        token_response = r.post('{}login'.format(self.api), auth=(user, password)).json()
        # Insert API URL, call login service, provide credentials & return json
        del user, password  # Remove user and password information
        token = token_response['token']  # Save login token to a variable
        self.head = {'Authorization': 'Bearer {}'.format(token)}

    def check_status(self, dir_taskid='/data/taskid', filename='tasklist', flag_remove=True):
        path = os.path.join(dir_taskid, filename + '.pkl')
        if os.path.exists(path):
            df_tasks, _ = df_read(dir_taskid, filename + '.pkl')

            for i in tqdm(range(len(df_tasks))):
                task_id = df_tasks.task_id[i]
                status_response = r.get('{}status/{}'.format(self.api, task_id), headers=self.head).json()
                if 'status' in list(status_response.keys()):
                    df_tasks.loc[i, 'status'] = status_response['status']
                    if status_response['status'] != 'deleted':
                        df_tasks.loc[i, 'task_name'] = status_response['task_name']
                        df_tasks.loc[i, 'expires_on'] = status_response['expires_on']
                    else:
                        df_tasks.loc[i, 'task_name'] = np.nan
                        df_tasks.loc[i, 'expires_on'] = np.nan
                else:
                    print(status_response)

            if flag_remove:
                df_tasks = df_tasks.loc[df_tasks.loc[:, 'status'] != 'deleted', :].reset_index(drop=True)

            df_tasks.to_pickle(os.path.join(dir_taskid, filename + '.pkl'))
            df_tasks.to_csv(os.path.join(dir_taskid, filename + '.csv'), index=False)

        else:
            print('no taksid list file on this directory')

    def request_area(self, user, password, prods, m_data, gdf_data, task_type, task_name, startDate, endDate, outFormat,
                     proj_method):
        layers = [(prods[s], m_data[t]) for s in range(len(prods)) for t in range(len(m_data))]
        prodLayer = []
        for l in layers:
            prodLayer.append({
                "layer": l[1],
                "product": l[0]
            })

        jsn_data = gdf_data.to_json()  # Extract Grand Canyon NP and set to variable
        jsn_data = json.loads(jsn_data)

        # set projection
        projections = r.get('{}spatial/proj'.format(self.api)).json()
        projs = {}  # Create an empty dictionary
        for p in projections:
            projs[p['Name']] = p  # Fill dictionary with `Name` as keys
        proj = projs[proj_method]['Name']  # Set output projection

        token_response = r.post('{}login'.format(self.api), auth=(user, password)).json()
        # Insert API URL, call login service, provide credentials & return json
        del user, password  # Remove user and password information
        token = token_response['token']  # Save login token to a variable
        head = {'Authorization': 'Bearer {}'.format(
            token)}  # Create a header to store token information, needed to submit a request

        task = {
            'task_type': task_type,
            'task_name': task_name,
            'params': {
                'dates': [
                    {
                        'startDate': startDate,
                        'endDate': endDate
                    }],
                'layers': prodLayer,
                'output': {
                    'format': {
                        'type': outFormat},
                    'projection': proj},
                'geo': jsn_data,
            }
        }

        task_response = r.post('{}task'.format(self.api), json=task, headers=head).json()
        # Ping API until request is complete, then continue to Section 4
        task_id = task_response['task_id']  # Set task id from request submission
        return head, task_id

    def checking(self, head, task_id):
        starttime = time.time()
        while r.get('{}task/{}'.format(self.api, task_id), headers=head).json()['status'] != 'done':
            print(r.get('{}task/{}'.format(self.api, task_id), headers=head).json()['status'])
            time.sleep(20.0 - ((time.time() - starttime) % 20.0))
        print(r.get('{}task/{}'.format(self.api, task_id), headers=head).json()['status'])

    def get_fileinfo(self, task_id):
        # self.checking(head, task_id)
        bundle = r.get(
            '{}bundle/{}'.format(self.api,
                                 task_id)).json()  # Call API and return bundle contents for the task_id as json
        print(bundle)

        files = {}  # Create empty dictionary
        for f in bundle['files']:
            files[f['file_id']] = f['file_name']

        return files

    def save_bulk(self, dir_taskid='/data/taskid', filename='tasklist.pkl', flag_remove=False):
        path = os.path.join(dir_taskid, filename)
        if os.path.exists(path):
            df_tasks, out_filename = df_read(dir_taskid, filename)
            for i in range(len(df_tasks)):
                id = df_tasks.index[i]
                status = df_tasks.loc[id, 'status']
                if status == 'done':
                    # geojson ファイルを目的地にコピー
                    path_json = df_tasks.loc[id, 'dir_jsonpath']
                    dir_dest = df_tasks.loc[id, 'dir_dest']
                    if not os.path.exists(dir_dest):
                        os.makedirs(dir_dest)  # Create the output directory
                    if not os.path.exists(os.path.join(dir_dest, os.path.basename(path_json))):
                        shutil.copy2(path_json, os.path.join(dir_dest, os.path.basename(path_json)))
                    task_id = df_tasks.loc[id, 'task_id']
                    # task_name = df_tasks.loc[id, 'task_name']
                    files = self.get_fileinfo(task_id)
                    self.save(dir_dest, files, task_id)
                    df_tasks.loc[id, 'status'] = 'downloaded'
                    # todo: dataframeで記録するか必要に応じで再考

            if flag_remove:
                df_tasks = df_tasks.loc[df_tasks.loc[:, 'status'] != 'downloaded', :].reset_index(drop=True)
            if (df_tasks.status == 'done').sum() > 0:
                df_tasks.to_pickle(os.path.join(dir_taskid, out_filename + '.pkl'))
                df_tasks.to_csv(os.path.join(dir_taskid, out_filename + '.csv'), index=False)
        else:
            print('no prepared task now')

    def save(self, destDir, files, task_id):
        # todo: jsonファイル名からディレクトリを作成し、そこにいれる。そしてJSONファイルもコピー
        if not os.path.exists(destDir):
            os.makedirs(destDir)  # Create the output directory
        for f in files:
            dl = r.get('{}bundle/{}/{}'.format(self.api, task_id, f),
                       stream=True)  # Get a stream to the bundle file
            filename = os.path.basename(cgi.parse_header(dl.headers['Content-Disposition'])[1][
                                            'filename'])  # Parse the name from Content-Disposition header
            # filename = task_name + '_' + filename
            filepath = os.path.join(destDir, filename)  # Create output file path
            with open(filepath, 'wb') as f:  # Write file to dest dir
                for data in dl.iter_content(chunk_size=8192):
                    f.write(data)
        print('Downloaded files can be found at: {}'.format(destDir))


def main():
    flag = 'Download_with_taskfile' #Check_status' #'MODIS_product'  # 'Check_product', 'ASTER_GDEM' 'MODIS' 'Check_status', Download_single, 'Download_with_taskfile'

    if flag == 'Check_product':
        dir_dest = '/src/data/USGS'
        filename = 'usgs_products_list'

        api = 'https://lpdaacsvc.cr.usgs.gov/appeears/api/'
        usgs_qr = USGSquery(api)
        usgs_qr.check_produts()
        product_response = r.get('{}product'.format(api)).json()  # request all products in the product service
        print('AρρEEARS currently supports {} products.'.format(len(product_response)))
        # Print no. products available in AppEEARS
        products = {p['ProductAndVersion']: p for p in product_response}
        # Create a dictionary indexed by product name & version
        df_out = pd.DataFrame(products).T.reset_index().rename(columns={'index': 'filename'})
        layers = []
        for filenm in tqdm(df_out.loc[:, 'filename'].tolist()):
            lst_response = r.get('{}product/{}'.format(api, filenm)).json()
            layers.append(",".join(map(str, list(lst_response.keys()))))
        df_out = df_out.assign(Layers=layers)

        if not os.path.exists(dir_dest):
            os.makedirs(dir_dest)
        df_out.to_csv(os.path.join(dir_dest, filename + '.csv'), index=False)

    if flag == 'MODIS_product':
        data_dir = '/data'
        sub_dir = 'MODIS'
        dir_taskid = '/data/taskid'
        print(os.getcwd())

        prods = ['MYD11A2.006', 'MOD11A2.006']  # or'MOD11A1.006',  None MOD11A1  MYD11A1 'MOD11A2.006', 'MYD11A2.006'
        m_data = ['LST_Day_1km', 'LST_Night_1km']  # or None　, 'LST_Night_1km'

        task_name = 'MODIS_11A2_Japan'

        proj_method = 'geographic'
        task_type = 'area'  # 'point' or 'area'  Type of task, area or point
        outFormat = 'geotiff'  # 'geotiff' or 'netcdf4' Set output file format type
        startDate = '01-01-2017'  # Start of the date range for which to extract data: MM-DD-YYYY
        endDate = '12-31-2018'
        api = 'https://lpdaacsvc.cr.usgs.gov/appeears/api/'

        # for polygon
        json_dir = '/data/MODIS/japan' #None #'/data/VectorPublic/Prefecture'  # '/src/data/VectorPublic/Prefecture/'
        json_fn = 'japan.geojson' #None  # 'jpn_shape.geojson'

        usgs_qr = USGSquery(api)
        usgs_qr.PickJsonFilenm(json_dir, json_fn)

        user = getpass.getpass(
            prompt='Enter NASA Earthdata Login Username: ')  # Input NASA Earthdata Login Username
        password = getpass.getpass(
            prompt='Enter NASA Earthdata Login Password: ')  # Input NASA Earthdata Login Password
        print(user, password)

        task_id = usgs_qr.request_area_multi(user, password, prods, m_data, task_type, task_name, startDate,
                                             endDate, outFormat, proj_method)
        usgs_qr.saveupdate_taskid(os.path.join(data_dir, sub_dir), dir_taskid)

    if flag == 'ASTER_GDEM':
        data_dir = '/data'
        sub_dir = 'ASTER_GDEM'
        dir_taskid = '/data/taskid'

        prods = 'ASTGTM_NC.003'
        m_data = 'ASTER_GDEM_DEM'

        task_name = 'ASTER_GDEM_Prefect'

        proj_method = 'geographic'
        task_type = 'area'  # 'point' or 'area'  Type of task, area or point
        outFormat = 'geotiff'  # 'geotiff' or 'netcdf4' Set output file format type
        startDate = '01-01-2010'  # Start of the date range for which to extract data: MM-DD-YYYY
        endDate = '12-31-2014'
        api = 'https://lpdaacsvc.cr.usgs.gov/appeears/api/'

        # for polygon
        json_dir = '/data/VectorPublic/Prefecture'  # '/src/data/VectorPublic/Prefecture/'
        json_fn = None  # 'jpn_shape.geojson'

        usgs_qr = USGSquery(api)
        usgs_qr.PickJsonFilenm(json_dir, json_fn)

        user = getpass.getpass(
            prompt='Enter NASA Earthdata Login Username: ')  # Input NASA Earthdata Login Username
        password = getpass.getpass(
            prompt='Enter NASA Earthdata Login Password: ')  # Input NASA Earthdata Login Password
        print(user, password)

        task_id = usgs_qr.request_area_multi(user, password, prods, m_data, task_type, task_name, startDate,
                                             endDate, outFormat, proj_method)
        usgs_qr.saveupdate_taskid(os.path.join(data_dir, sub_dir), dir_taskid)

    if flag == 'Check_status':
        api = 'https://lpdaacsvc.cr.usgs.gov/appeears/api/'
        usgs_qr = USGSquery(api)
        usgs_qr.generate_token()
        usgs_qr.check_status()

    # if flag == 'MODIS':
    #     data_dir = '/src/data'
    #     sub_dir = 'MODIS'
    #     dir_taskid = '/src/data/taskid'
    #
    #     prods = ['MYD11A1.006', 'MOD11A1.006']  # or'MOD11A1.006',  None MOD11A1  MYD11A1 'MOD11A2.006', 'MYD11A2.006'
    #     m_data = ['LST_Day_1km', 'LST_Night_1km']  # or None　, 'LST_Night_1km'
    #
    #     task_name = 'MODIS_LST_A1_2017'
    #
    #     proj_method = 'geographic'
    #     task_type = 'area'  # 'point' or 'area'  Type of task, area or point
    #     outFormat = 'geotiff'  # 'geotiff' or 'netcdf4' Set output file format type
    #     startDate = '01-01-2017'  # Start of the date range for which to extract data: MM-DD-YYYY
    #     endDate = '12-31-2017'  # End of the date range for which to extract data: MM-DD-YYYY
    #     api = 'https://lpdaacsvc.cr.usgs.gov/appeears/api/'
    #
    #     flag_process = 'query'  # select 'check', 'query'
    #
    #     usgs_qr = USGSquery(api)
    #     usgs_qr.PickJsonFilenm(json_dir, json_fn)
    #
    #     if flag_process is 'check':
    #         df_products, list_response = usgs_qr.check_produts(prods)
    #         print(df_products.columns)
    #         print([list_response[s].columns for s in range(len(list_response))])
    #
    #     if flag_process is 'query':
    #         user = getpass.getpass(
    #             prompt='Enter NASA Earthdata Login Username: ')  # Input NASA Earthdata Login Username
    #         password = getpass.getpass(
    #             prompt='Enter NASA Earthdata Login Password: ')  # Input NASA Earthdata Login Password
    #         print(user, password)
    #
    #         task_id = usgs_qr.request_area_multi(user, password, prods, m_data, task_type, task_name, startDate,
    #                                              endDate, outFormat, proj_method)
    #         usgs_qr.saveupdate_taskid(os.path.join(data_dir, sub_dir), dir_taskid)
    #
    # if flag == 'Download_single':
    #     task_id_ar = None
    #     dir_dest = '/src/data/ASTER_GDEM'
    #
    #     api = 'https://lpdaacsvc.cr.usgs.gov/appeears/api/'
    #     usgs_qr = USGSquery(api)
    #
    #     if task_id_ar is None:
    #         task_id_ar = getpass.getpass(prompt='task_id: ')
    #     files_ar = usgs_qr.get_fileinfo(task_id_ar)
    #     usgs_qr.save(dir_dest, files_ar, task_id_ar)

    if flag == 'Download_with_taskfile':
        api = 'https://lpdaacsvc.cr.usgs.gov/appeears/api/'
        usgs_qr = USGSquery(api)
        usgs_qr.save_bulk(flag_remove=False)


if __name__ == "__main__":
    main()
