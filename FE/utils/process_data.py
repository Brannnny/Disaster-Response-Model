# imports
import sys
import pandas as pd
from sqlalchemy import create_engine

from general_utils import utils
from general_utils.utils import ReadWriteS3


class TransformData:
    def __init__(self):
        pass

    def load_data(self, messages_filename, categories_filename):
        """
        - Takes inputs as two CSV files
        - Imports them as pandas dataframe.
        - Merges them into a single dataframe

        Args:
        messages_file_path str: Messages CSV file
        categories_file_path str: Categories CSV file

        Returns:
        merged_df pandas_dataframe: Dataframe obtained from merging the two input\
        data
        """

        messages = ReadWriteS3.create_connection().read_from_s3(messages_filename)
        categories = ReadWriteS3.create_connection().read_from_s3(categories_filename)

        df = messages.merge(categories, on='id')

        return df

    def clean_data(self, df):
        """
        - Cleans the combined dataframe for use by ML model

        Args:
        df pandas_dataframe: Merged dataframe returned from load_data() function

        Returns:
        df pandas_dataframe: Cleaned data to be used by ML model
        """

        # Split categories into separate category columns
        categories = df['categories'].str.split(";", \
                                                expand=True)

        # select the first row of the categories dataframe
        row = categories.iloc[0, :].values

        # use this row to extract a list of new column names for categories.
        new_cols = [r[:-2] for r in row]

        # rename the columns of `categories`
        categories.columns = new_cols

        # Convert category values to just numbers 0 or 1.
        for column in categories:
            # set each value to be the last character of the string
            categories[column] = categories[column].str[-1]

            # convert column from string to numeric
            categories[column] = pd.to_numeric(categories[column])

        # drop the original categories column from `df`
        df.drop('categories', axis=1, inplace=True)

        # concatenate the original dataframe with the new `categories` dataframe
        df[categories.columns] = categories

        # drop duplicates
        df.drop_duplicates(inplace=True)

        return df

    def fit(self, messages_filename, categories_filename):
        df = self.load_data(messages_filename, categories_filename)
        return df

    def transform(self, df):
        df = self.clean_data(df)
        return df

    def fit_transform(self, messages_filename, categories_filename):
        df = self.fit(messages_filename, categories_filename)
        df = self.transform(df)

        return df


def save_data(self, df, database_file_name):
    """
    Saves cleaned data to an SQL database

    Args:
    df pandas_dataframe: Cleaned data returned from clean_data() function
    database_file_name str: File path of SQL Database into which the cleaned\
    data is to be saved

    Returns:
    None
    """

    engine = create_engine('sqlite:///{}'.format(database_file_name))
    db_file_name = database_file_name.split("/")[-1]  # extract file name from \
    # the file path
    table_name = db_file_name.split(".")[0]
    df.to_sql(table_name, engine, index=False, if_exists='replace')


def main():
    if len(sys.argv) == 4:

        messages_filepath, categories_filepath, database_filepath = sys.argv[1:]

        print('Loading data...\n    MESSAGES: {}\n    CATEGORIES: {}'
              .format(messages_filepath, categories_filepath))
        transform_data = TransformData()

        df = TransformData(.fit_transform(messages_filename=)


        print('Cleaning data...')


        print('Saving data...\n    DATABASE: {}'.format(database_filepath))
        save_data(df, database_filepath)

        print('Cleaned data saved to database!')

    else:
        print('Please provide the filepaths of the messages and categories ' \
              'datasets as the first and second argument respectively, as ' \
              'well as the filepath of the database to save the cleaned data ' \
              'to as the third argument. \n\nExample: python process_data.py ' \
              'disaster_messages.csv disaster_categories.csv ' \
              'DisasterResponse.db')


# run
if __name__ == '__main__':
    main()