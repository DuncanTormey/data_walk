#!/usr/bin/env python
# Author: Duncan Tormey
# Email: dut@stowers.org or duncantormey@gmail.com
import requests
from requests.auth import HTTPBasicAuth
from requests.utils import quote

# !/usr/bin/env python
# Author: Duncan Tormey
# Email: dut@stowers.org or duncantormey@gmail.com

import requests
import pandas as pd
from pandas.io.json import json_normalize
import json
import os

order_search = """
    [{
        "type": "grouper",
        "groupType": "and",
        "items": [{
            "dataType": "stowers.department",
            "value": [%s],
            "comparison": "in",
            "id": "requestingDepartment",
            "entity": "MolecularBiology.Ngs.NgsRequest",
            "fieldId": "requestingDepartment"
        }, {
            "dataType": "selectlist",
            "value": ["COMPLETED"],
            "comparison": "in",
            "id": "status",
            "entity": "MolecularBiology.Ngs.NgsRequest",
            "fieldId": "status"
        }]
    }]
"""

sample_search = """
  [{
        "type": "grouper",
        "groupType": "and",
        "items": [{
            "dataType": "selectlist",
            "value": ["COMPLETED"],
            "comparison": "in",
            "id": "workRequest.status",
            "entity": "MolecularBiology.Ngs.NgsRequestSample",
            "fieldId": "workRequest.status"
        }, {
            "dataType": "stowers.department",
            "value": [%s],
            "comparison": "in",
            "id": "workRequest.requestingDepartment",
            "entity": "MolecularBiology.Ngs.NgsRequestSample",
            "fieldId": "workRequest.requestingDepartment"
        }, {
            "dataType": "textfield",
            "value": "%s",
            "comparison": "like",
            "id": "workRequest.prnOrderId",
            "entity": "MolecularBiology.Ngs.NgsRequestSample",
            "fieldId": "workRequest.prnOrderId"
        }]
    }]
"""

lib_search = """
    [{
        "type": "grouper",
        "groupType": "and",
        "items": [{
            "dataType": "stowers.department",
            "value": [%s],
            "comparison": "in",
            "id": "workRequest.requestingDepartment",
            "entity": "MolecularBiology.Ngs.Library",
            "fieldId": "workRequest.requestingDepartment"
        }, {
            "dataType": "textfield",
            "value": "%s",
            "comparison": "like",
            "id": "workRequest.prnOrderId",
            "entity": "MolecularBiology.Ngs.Library",
            "fieldId": "workRequest.prnOrderId"
        }]
    }]
"""


def ret_lims_search_data(token_file, search, order=False):
    if order:
        search = search % order

    with open(token_file, 'r') as fh:
        token = fh.readline().strip('\n')

    # URL for POST request
    url = 'https://lims.stowers.org/zanmodules/site/advanced-search/results.json'
    headers = {'Accept': 'application/json'}
    payload = {
        'search': search,
        'addGroups': ['request']
    }
    # IMPORTANT: API token must have "write" access to perform a POST
    request_results = requests.post(url=url,
                                    auth=('apitoken', token),
                                    data=json.dumps(payload),
                                    headers=headers)

    json_data = request_results.json()

    return json_data


def ret_readable_order_data(json_order_data):
    df = json_normalize(json_order_data['results'])
    read_cols = [
        'analysisGoals',
        'comments',
        'completedAt',
        'coreCreatedLibraries',
        'createdAt',
        'creator.displayName',
        'creator.email',
        'creator.username',
        'displayString',
        'fulfiller.displayName',
        'fulfiller.email',
        'id',
        'machineMode.displayString',
        'machineMode.label',
        'machineType.displayString',
        'machineType.label',
        'ngsProjectType.displayString',
        'ngsProjectType.label',
        'numFlowcellsNeeded',
        'numLanesNeeded',
        'numPools',
        'numUnpooledLibraries',
        'orderTotal',
        'poolingComments',
        'prnOrderId',
        'readLength.displayString',
        'readLength.label',
        'readType',
        'requestTitle',
        'requestedAt',
        'requester.username',
        'requestingDepartment.departmentHead.displayName',
        'requestingDepartment.name',
        'specialInstructions',
        'staffComments',
        'updatedAt',
    ]

    df = df[read_cols]

    return df


def attchements_data_request(token_file, order_id):
    with open(token_file, 'r') as fh:
        token = fh.readline().strip('\n')
    url = 'https://lims.stowers.org/zanmodules/molecular-biology/ngs/requests/{}/attachments'.format(order_id)
    attachments_results = requests.get(
        url=url,
        auth=('apitoken', token),

    )

    attachments_json = attachments_results.json()
    attachments_df = json_normalize(attachments_json)

    return attachments_json, attachments_df


def download_attachments(token_file, attachments_df, dirpath=''):
    with open(token_file, 'r') as fh:
        token = fh.readline().strip('\n')
    outfiles = []
    if 'id' in attachments_df.columns:
        if len(attachments_df['id'].dropna()) > 0:
            for index, row in attachments_df.iterrows():
                url = 'https://lims.stowers.org/zanmodules/molecular-biology/ngs/requests/attachments/{}'.format(
                    row['id'])
                attachment = requests.get(
                    url=url,
                    auth=('apitoken', token),
                )
                outfile = dirpath + '/' + row['name']
                with open(outfile, 'wb') as fw:
                    fw.write(attachment.content)
                outfiles.append(outfile)

    return outfiles


# Kit Items
# /zanmodules/molecular-biology/ngs/requests/{id}/kit-items
# or
# /zanmodules/molecular-biology/ngs/requests/{id}/unique-kit-items

# {id} here is the request ID like MOLNG-123

# Pools
# /zanmodules/molecular-biology/ngs/requests/{identifier}/pools

# {identifier} here is the request ID like MOLNG-123


def request_additional_oder_items(token_file, order_id, url_template):
    with open(token_file, 'r') as fh:
        token = fh.readline().strip('\n')
    url = url_template.format(order_id)
    results = requests.get(
        url=url,
        auth=('apitoken', token),

    )

    return results.json()


def save_json(obj, file_path):
    with open(file_path, 'w') as f:
        json.dump(obj, f, ensure_ascii=False)


def make_dir(dir_name):
    if not os.path.exists(dir_name):
        os.makedirs(dir_name)


if __name__ == "__main__":
    # constants
    lims_department_id = "4"
    token_file = '../data/lims_token.txt'

    # constant modified searches
    order_search = order_search % LIMS_DEPARTMENT_ID
    sample_search = sample_search % (LIMS_DEPARTMENT_ID, '%s')
    lib_search = lib_search % (LIMS_DEPARTMENT_ID, '%s')

    ###########################################################################
    ############################# Baumann Lab Run #############################
    ###########################################################################

    # make root dir
    root_meta_dir = '../data/lims_meta_data'
    make_dir(root_meta_dir)

    # get order data
    order_data = ret_lims_search_data(token_file, order_search, order=False)
    save_json(order_data, '%s/baumann_orders.json' % root_meta_dir)

    order_data_df = ret_readable_order_data(order_data)
    order_data_df.to_excel('%s/all_orders_meta_data.xls' % root_meta_dir, index=False)

    # get order specific data
    for index, row in order_data_df.iterrows():
        # assign order ids
        order_name = row['prnOrderId']
        order_id = row['id']

        # make order subdir
        order_sub_dir = root_meta_dir + '/%s_meta' % order_name
        make_dir(order_sub_dir)

        # get search data (library and samples)
        sample_data = ret_lims_search_data(token_file, sample_search, order=order_name)
        library_data = ret_lims_search_data(token_file, lib_search, order=order_name)
        save_json(sample_data, '%s/%s_sample_data.json' % (order_sub_dir, order_name))
        save_json(library_data, '%s/%s_library_data.json' % (order_sub_dir, order_name))

        # get attachments
        attachments_json, attachments_df = attchements_data_request(token_file, order_id)
        save_json(attachments_json, '%s/%s_attachments_data.json' % (order_sub_dir, order_name))
        download_attachments(token_file, attachments_df, dirpath=order_sub_dir)

        # get additional order items
        kit_url = 'https://lims.stowers.org/zanmodules/molecular-biology/ngs/requests/{}/kit-items'
        kit_items_data = request_additional_oder_items(token_file, order_name, url_template=kit_url)
        save_json(kit_items_data, '%s/%s_kit_items_data.json' % (order_sub_dir, order_name))

        pool_url = 'https://lims.stowers.org/zanmodules/molecular-biology/ngs/requests/{}/pools'
        pool_data = request_additional_oder_items(token_file, order_name, url_template=pool_url)
        save_json(pool_data, '%s/%s_pool_data.json' % (order_sub_dir, order_name))