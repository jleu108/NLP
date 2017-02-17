# Provide Sentiment score based on answer field on trybe_reports_data_extract 
# and inserts into MySQL table and creates a CSV output file.

from __future__ import print_function
import httplib2
import os
import pandas as pd
import sys


from apiclient import discovery
import oauth2client
from oauth2client import client
from oauth2client import tools
import ast

# pymysql libraries
from urllib2 import Request, urlopen, URLError
from pprint import pprint
import urllib, json
#import _mysql
import pymysql.cursors
import sqlalchemy as sa

# Natural Language Processing
import nltk
import nltk.classify.util
from nltk.corpus import brown
from nltk.classify import NaiveBayesClassifier
from nltk.corpus import movie_reviews
 
import urllib

import csv

try:
    import argparse
    flags = argparse.ArgumentParser(parents=[tools.argparser]).parse_args()

except ImportError:
    flags = None

# Connect to the trybe_stats database
connection = pymysql.connect(host='xx',
                             user='xx',
                             password='xx',
                             db='trybe_stats',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)

# Initialize NLP Variables
pos_tweets = [('I love this car', 'positive'),
              ('This view is amazing', 'positive'),
              ('I feel great this morning', 'positive'),
              ('I am so excited about the concert', 'positive'),
              ('He is my best friend', 'positive')]

neg_tweets = [('I do not like this car', 'negative'),
              ('This view is horrible', 'negative'),
              ('I feel tired this morning', 'negative'),
              ('I am not looking forward to the concert', 'negative'),
              ('He is my enemy', 'negative')]


document = ['love', 'this', 'car']


def main():



    try:

        data = urllib.urlencode({"text": "I'm a very good boy "}) 
        u = urllib.urlopen("http://text-processing.com/api/sentiment/", data)
        the_page = u.read()

        sql_query = """ Enter Query Here
                """
                
        sql_df = pd.read_sql_query(sql_query, con=connection, params=None)

        print(sql_query)
        #convert back to list to insert into MySQL
        processed_dataset = sql_df.values.tolist()  

        new_list = []

        print('Initiale new_list.')

        if not processed_dataset:
            print('No data found.')
        else:
            with connection.cursor() as cursor:        



                # Go through Answer Field values and check each value in urllib.urlencode({"text": "I'm a very good boy "}) 
                for items in processed_dataset:  #[:4] for first 4 columns
                    answer = items[6]
                    #Clean up answer string
                    answer1 = answer.replace('"', '')
                    answer2 = answer1.replace('\n', '')
                    answer3 = answer2.replace('xc3', '')
                    answer4 = answer3.replace('0xc3', '')
                    clean_answer = answer4.replace('`', '')
                    #print(answer)

                    # Put list items into dictionary
                    data_line = '''{"text": ''' + '"' + clean_answer + """"}"""
                    
                    data_line_dict = ast.literal_eval(data_line)

                    inputdata = urllib.urlencode(data_line_dict)
                    u = urllib.urlopen("http://text-processing.com/api/sentiment/", inputdata)
                    the_page = u.read()
                    
                    #change back str to dict
                    the_page_dict = ast.literal_eval(the_page)
                    #print(type(the_page_dict))

                    #Create list of lists, Need to break up the_page dict - only pull Label
                    new_list.append([items[0], items[1], items[2], items[3], items[4], items[5], items[6], the_page_dict['label']])

                print(new_list)
                    #print(the_page_dict['probability'], the_page_dict['label'])

                # Create Table to store Mapped Values
                print('Creating sentiment_value_table table...')

                cursor.execute("""DROP TABLE IF EXISTS `sentiment_value_table`""")
                
                cursor.execute("""CREATE TABLE `sentiment_value_table` (
                                    `gender` varchar(20) NULL,
                                    `relationship` varchar(255) COLLATE utf8_bin NULL,
                                    `income_tier` varchar(255) COLLATE utf8_bin NULL,
                                    `education` varchar(255) COLLATE utf8_bin NULL,
                                    `age` varchar(255) COLLATE utf8_bin NULL,
                                    `trybe_score` int COLLATE utf8_bin NULL default null,
                                    `review` varchar(2000) COLLATE utf8_bin NULL default null,
                                    `sentiment` varchar(10) COLLATE utf8_bin NULL default null
                                ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

                """)

                # read list into MySQL Table
                for item in new_list:
                        insert_sql = """INSERT INTO `sentiment_value_table` (
                            `gender`
                            , `relationship`
                            , `income_tier`
                            , `education`
                            , `age`
                            , `trybe_score`
                            , `review`
                            , `sentiment`
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"""
                        cursor.execute(insert_sql, [item[0], item[1], item[2], item[3], item[4], item[5], item[6], item[7]])
                        print(item)                    
                        
                else:
                    print('No rows found')

                print('Finished inserting.')


                # COUNT NUMBER OF ROWS
                cursor.execute("SELECT COUNT(*) from `sentiment_value_table`")
                result=cursor.fetchone()
                print(result.values())   #returns a dictionary, so values() gets the values which is the row count

                print(' ')

            # connection is not autocommit by default. So you must commit to save
            # your changes.
            connection.commit()


    finally:
        connection.close()        



if __name__ == '__main__':
    main()