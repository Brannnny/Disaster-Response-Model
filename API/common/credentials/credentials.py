
"""This file has all the access credentials for the project"""

from decouple import config

mongodg_cred = {
    'username': config('mongodb_username'),
    'password': config('mongodb_password')
}
