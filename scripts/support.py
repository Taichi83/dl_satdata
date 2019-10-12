import numpy as np
import os
import pandas as pd
from os import path
from multiprocessing import Pool
from tqdm import tqdm
from django.db import connection
from pathlib import Path

def df_read(fdir, fname):
    f_ext = fname.split('.')[-1]
    if f_ext == 'csv':
        df_data = pd.read_csv(os.path.join(fdir, fname), index_col=False)

    elif f_ext == 'pkl':
        df_data = pd.read_pickle(os.path.join(fdir, fname))
    else:
        print('check file extention')
        df_data = np.nan

    return df_data, fname.split('.')[:-1]


def df_read_list(dir_sat, filenms, add_word=None):
    out_filename = path.commonprefix(filenms)
    if add_word is not None:
        out_filename = out_filename + add_word
    return [df_read(dir_sat, fname)[0] for fname in filenms], out_filename

def argwrapper(args):
    return args[0](*args[1:])


def imap_unordered_bar(func, args, n_processes=15, django_process=False):
    if django_process:
        connection.close()
    p = Pool(n_processes)
    res_list = []
    with tqdm(total=len(args)) as pbar:
        for i, res in tqdm(enumerate(p.imap_unordered(func, args))):
            pbar.update()
            res_list.append(res)
    pbar.close()
    p.close()
    p.join()
    return res_list

def addCol_CropVar(df_sat, df_crop, col_uq=['Block_ID', 'Ranch_ID']):
    '''

    :param df_sat: Satellite data
    :param df_crop: Crop data
    :return: Satellite data with Crop Variety
    '''
    # df_sat = df_sat.assign(Crop_variety=np.nan)
    if col_uq==['Block_ID', 'Ranch_ID']:
        df_crop_temp = df_crop.loc[-df_crop.Block_ID.isnull(), :].drop_duplicates(subset=col_uq,
                                                                                  keep='first')
    else:
        df_crop_temp = df_crop.drop_duplicates(subset=col_uq, keep='first')

    df_sat_out = pd.merge(df_sat, df_crop_temp.loc[:, col_uq + ['Crop_variety']],
                          on=col_uq, how='left')

    # 足りないものはBlock nameから引っ張る
    df_sat_out.loc[df_sat_out.Block_name == '30Merlot', 'Block_name'] = '30 Merlot'
    bool_nan = df_sat_out.Crop_variety.isnull()
    list_cropvals = df_crop.Crop_variety.unique().tolist()
    new_cropvals = df_sat_out.loc[bool_nan, 'Block_name'].str.split(" |_", n=1, expand=True)[1].apply(
        lambda x: x if x in list_cropvals else np.nan)
    df_sat_out.loc[new_cropvals.index, 'Crop_variety'] = new_cropvals
    # Cropdata.pklに含まれない品種はここで削除
    df_sat_out = df_sat_out.dropna(subset=['Crop_variety'])
    return df_sat_out

def mergeDF_inLIst(list_df_data, list_uqcols, how='left'):
    if len(list_df_data) == 1:
        df_out = list_df_data[0]
    else:
        for s in range(len(list_df_data) - 1):
                if s == 0:
                    df_out = pd.merge(list_df_data[s], list_df_data[s + 1], on=list_uqcols, how=how)
                else:
                    df_out = pd.merge(df_out, list_df_data[s + 1], on=list_uqcols, how=how)
    return df_out

def read_filenms(dir_data, file_key=None):
    if file_key is None:
        file_key = '*'
    p = Path(dir_data)
    p_list = list(p.glob(file_key))
    if len(p_list)==1:
        out_filenames = p_list[0].name
    else:
        out_filenames = [p_list[s].name for s in range(len(p_list))]
    return out_filenames

# if len(list_df_indx) == 1:
#     df_sat_ranch_out = list_df_indx[0]
# else:
#     for s in range(len(list_df_indx) - 1):
#         if s == 0:
#             df_sat_ranch_out = pd.merge(list_df_indx[s], list_df_indx[s + 1], on=list_uqcols_sat_mrg,
#                                         how='left')
#         else:
#             df_sat_ranch_out = pd.merge(df_sat_ranch_out, list_df_indx[s + 1], on=list_uqcols_sat_mrg,
#                                         how='left')
#
# list_uqcols_yield = ['Ranch_ID', 'Year', 'Crop_variety', 'Crop_yield']
# df_sat_ranch_out_yield = pd.merge(df_sat_ranch_out, df_crop_ranch.loc[:, list_uqcols_yield],
#                                   on=list_uqcols, how='left').dropna(subset=['Crop_yield'])

if __name__ == "__main__":
    main()