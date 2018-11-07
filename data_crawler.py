# -*- coding: utf-8 -*-

"""Data crawler and parser for Consumidor.gov website data.

This script is able to crawl the companies general reports from customers
in the http://consumidor.gov.br portal. The crawled data is then processed
to clean unused data and the remaining data is reshaped the required format.

All parsed data is then stored in a MongoDB database, hosted in MongoDB Atlas.

This script requires `pandas` to be installed in the Python environment where
the script will be executed. Pandas will be used to process the data.

It also requires `dnsython` in order to use mongodb+srv:// URIs.

This function contains the following functions:

    * get_file_codes - get all file codes for companies customer reports 
    * get_file_dataframe - generate pandas dataframe from crawled file
    * strip_dataframe_strings - remove surrounding spaces from all strings
    * get_age_average - get age average rounded up for a given age range
    * main - the main function of the script
"""

import argparse
import json
from urllib.request import urlopen
import pandas as pd
import pymongo
from datetime import datetime, tzinfo


# JSON endpoint where the list of file codes are stored.
file_codes_endpoint = 'https://www.consumidor.gov.br/pages/publicacao/externo/publicacoes.json?indicadorTipoPublicacao=2'

TITLE = 'titulo'
SOURCE_FILE_PARTIAL_NAME = 'Dados'
FIRST_FIVE_CHARS = 5
CODE = 'codigo'
MONGO_URI = 'mongodb+srv://{}:{}@bigpy-azwev.mongodb.net/test?retryWrites=true'
MONGO_DB = 'bigpy'
MONGO_COLLECTION = 'bigpy_companyreports'
AGE_LESS_20 = 'até 20 anos'
AGE_BTW_21_AND_30 = 'entre 21 e 30'
AGE_BTW_31_AND_40 = 'entre 31 e 40'
AGE_BTW_41_AND_50 = 'entre 41 e 50'
AGE_BTW_51_AND_60 = 'entre 51 e 60'
AGE_BTW_61_AND_70 = 'entre 61 e 70'


def get_file_codes(url):
    """Get all file codes for companies customer reports

    Parameters
    ----------
    url: str
        Endpoint URL where the file is hosted

    Returns
    -------
    list
        A list of the file codes to be downloaded 
    """

    response = urlopen(url)
    decoded_response = response.read().decode('utf-8')
    json_response = json.loads(decoded_response)

    file_codes = list()
    for source_file in json_response:
        if source_file[TITLE][:FIRST_FIVE_CHARS] == SOURCE_FILE_PARTIAL_NAME:
            file_codes.append(source_file[CODE])

    return file_codes


def get_file_dataframe(file_code):
    """Generate pandas dataframe from crawled file

    Parameters
    ----------
    file_code: str
        The file code to be downloaded

    Returns
    -------
    pandas dataframe
        The Pandas dataframe from the read file
    """
    file_url = f"https://www.consumidor.gov.br/pages/publicacao/externo/{file_code}/download"
    dataframe = pd.read_csv(file_url, compression="zip",
                            encoding="iso-8859-1", delimiter=";")

    return dataframe


def strip_dataframe_strings(dataframe):
    """Remove surrounding spaces from all strings

    Parameters
    ----------
    dataframe: pandas dataframe
        Dataframe with all string columns

    Returns
    -------
    pandas dataframe
    """

    tmp_dataframe = dataframe.select_dtypes(['object'])
    dataframe[tmp_dataframe.columns] = tmp_dataframe.apply(
        lambda x: x.str.strip())
    return dataframe


def get_age_average(age_range):
    """Get age average rounded up for a given age range

    Parameters
    ----------
    age_range: str
        Age range string. It could have some different formats, such as:
            'até 20 anos'
            'entre 21 e 30 anos'
            'mais de 70 anos'
        All those ranges are translated in constants in the top of the file.

    Returns
    -------
    int
        The age average
    """

    if age_range == AGE_LESS_20:
        return 20
    elif age_range == AGE_BTW_21_AND_30:
        return 26
    elif age_range == AGE_BTW_31_AND_40:
        return 36
    elif age_range == AGE_BTW_41_AND_50:
        return 46
    elif age_range == AGE_BTW_51_AND_60:
        return 56
    elif age_range == AGE_BTW_61_AND_70:
        return 66
    else:
        return 70


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('-u', '--user', help='MongoDB username', required=True)
    parser.add_argument('-p', '--password',
                        help="MongoDB user's password", required=True)

    args = parser.parse_args()
    db_user = args.user
    db_password = args.password
    db_uri = MONGO_URI.format(db_user, db_password)

    for file_code in get_file_codes(file_codes_endpoint):
        dataframe = get_file_dataframe(file_code)

        # Remove the list of unused columns from the dataframe.
        unused_columns = [9, 10, 11, 13, 14, 16, 17, 19]
        dataframe = dataframe.drop(
            dataframe.columns[unused_columns], axis='columns')

        # Rename the remaining dataframe columns to more reliable names
        dataframe.columns = [
            'region',
            'state',
            'city',
            'gender',
            'age_range',
            'conclusion_date',
            'days_to_reply',
            'company_name',
            'market_segment',
            'problem_reported',
            'company_replied',
            'customer_rating'
        ]

        dataframe = strip_dataframe_strings(dataframe)

        # Refactor age range to me the average age rounded up
        dataframe['age_range'] = dataframe['age_range'].apply(
            get_age_average
        )

        # Correctly format date column to be a date object
        dataframe['conclusion_date'] = pd.to_datetime(
            dataframe['conclusion_date'], format='%d/%m/%Y')

        # Replace 'S' (Yes) string to boolean
        dataframe['company_replied'] = dataframe['company_replied'].apply(
            lambda x: True if x == 'S' else False
        )

        parsed_documents = dataframe.to_json(
            orient='records',
            force_ascii=False,
            double_precision=0,
            date_format='iso'
        )

        parsed_documents = json.loads(parsed_documents)

        documents = list()
        for single_document in parsed_documents:
            single_document['conclusion_date'] = datetime.strptime(
                single_document['conclusion_date'], "%Y-%m-%dT%H:%M:%S.%fZ")
            documents.append(single_document)

        # Save all documents to MongoDB
        client = pymongo.MongoClient(db_uri)
        db = client[MONGO_DB]
        db[MONGO_COLLECTION].insert_many(documents)
        client.close()

    return True


if __name__ == '__main__':
    main()
