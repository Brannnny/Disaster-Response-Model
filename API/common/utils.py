"""Written by Omolewa Adaramola"""

from common.credentials import credentials


class InitDBConnection:
    @classmethod
    def mongodb_connection(cls):
        username = credentials.mongodg_cred.get('username')
        password = credentials.mongodg_cred.get('password')
        conn = f'mongodb+srv://{username}:{password}' \
               f'@object-detection-and-se.sx1doi8.mongodb.net/?retryWrites=true&w=majority'

        return conn


if __name__ == '__main__':
    print(InitDBConnection.mongodb_connection())
