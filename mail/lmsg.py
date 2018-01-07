from __future__ import print_function
import pdb

import httplib2
import os
import pandas as pd

from apiclient import discovery
from oauth2client import client
from oauth2client import tools
from oauth2client.file import Storage


APPLICATION_NAME = 'Gmail API Python Quickstart'
CLIENT_SECRET_FILE = 'client_secret.json'
METADATA_HEADER_FIELDS = ['Date', 'From', 'Subject']
SCOPES = 'https://www.googleapis.com/auth/gmail.readonly'
SCOPES = 'https://www.googleapis.com/auth/gmail.metadata'


def get_message_data(service, message_ids, fields):
    results = []
    for id in message_ids:
        s_msg = (service
                 .users()
                 .messages()
                 .get(userId='me',
                      format='metadata',
                      id=id,
                      metadataHeaders=fields)
                 .execute()
                 )
        s_pl = s_msg['payload']
        data = {}
        for x in s_pl['headers']:
            data[x['name']] = x['value']
        data['id'] = id
        results.append(data)

    return pd.DataFrame(results)


def get_credentials():
    """Gets valid user credentials from storage.

    If nothing has been stored, or if the stored credentials are invalid,
    the OAuth2 flow is completed to obtain the new credentials.

    Returns:
        Credentials, the obtained credential.
    """
    home_dir = os.path.expanduser('~')
    credential_dir = os.path.join(home_dir, '.credentials')
    if not os.path.exists(credential_dir):
        os.makedirs(credential_dir)
    credential_path = os.path.join(credential_dir,
                                   'gmail-python-quickstart.json')

    store = Storage(credential_path)
    credentials = store.get()
    if not credentials or credentials.invalid:
        flow = client.flow_from_clientsecrets(CLIENT_SECRET_FILE, SCOPES)
        flow.user_agent = APPLICATION_NAME
        credentials = tools.run(flow, store)
        print('Storing credentials to ' + credential_path)
    return credentials


def mesg_by_label(service, label_ids=[]):
    df_list = []
    try:
        results = (service
                   .users()
                   .messages()
                   .list(userId='me', labelIds=label_ids)
                   .execute()
                   )
        num_messages = len(results['messages'])
        if 'INBOX'in label_ids:
            pdb.set_trace()
        while 'nextPageToken' in results:
            message_ids = [r['id'] for r in results['messages']]
            df = get_message_data(service,
                                  message_ids,
                                  fields=METADATA_HEADER_FIELDS)
            df_list.append(df)
            page_token = results['nextPageToken']
            results = (service
                       .users()
                       .messages()
                       .list(userId='me',
                             pageToken=page_token,
                             labelIds=label_ids)
                       .execute()
                       )
            num_messages += len(results['messages'])
        return pd.concat(df_list)
    except Exception as e:
        print(e)
        pass
        return None


def test_one():
    credentials = get_credentials()
    http = credentials.authorize(httplib2.Http())
    service = discovery.build('gmail', 'v1', http=http)

    # get labels
    results = service.users().labels().list(userId='me').execute()
    labels = results.get('labels', [])
    label_names = [x['name'] for x in labels]
    label_names = ['INBOX']
    df_list = []
    for l in label_names:
        df = mesg_by_label(service, label_ids=l)
        if df is not None:
            df_list.append(df)

    df = pd.concat(df_list)
    pdb.set_trace()

def test_two():
    id = '160ceb1f526ea000'
