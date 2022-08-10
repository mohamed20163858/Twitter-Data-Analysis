import json
import pandas as pd
from textblob import TextBlob
class Clean_Tweets:
    """
    The PEP8 Standard AMAZING!!!
    """
    def __init__(self, df:pd.DataFrame):
        self.df = df
        print('Automation in Action...!!!')
        
    def drop_unwanted_column(self, df:pd.DataFrame)->pd.DataFrame:
        """
        remove rows that has column names. This error originated from
        the data collection stage.  
        """
        unwanted_rows = df[df['retweet_count'] == 'retweet_count' ].index
        df.drop(unwanted_rows , inplace=True)
        df = df[df['polarity'] != 'polarity']
        return df
    def drop_duplicate(self, df:pd.DataFrame)->pd.DataFrame:
        """
        drop duplicate rows
        """
        return df.drop_duplicates()
    def convert_to_datetime(self, df:pd.DataFrame)->pd.DataFrame:
        """
        convert column to datetime
        """ 
        df = df[df['created_at'] >= '2020-12-31' ]
        for i in range(len(df['created_at'])):
            from datetime import datetime
            try:
                datetime_object =datetime.strptime(df['created_at'][i], '%a %b %d %H:%M:%S +%f %Y')
                df['created_at'][i]=str(datetime_object) 
            except KeyError:
                df['created_at'][i]=''
        return df
    
    def convert_to_numbers(self, df:pd.DataFrame)->pd.DataFrame:
        """
        convert columns like polarity, subjectivity, retweet_count
        favorite_count etc to numbers
        """
        df['polarity'] = pd.to_numeric(df['polarity'])
        df['subjectivity'] = pd.to_numeric(df['subjectivity'])
        df['retweet_count'] = pd.to_numeric(df['retweet_count'])
        df['favorite_count'] = pd.to_numeric(df['favorite_count'])
        return df
    
    def remove_non_english_tweets(self, df:pd.DataFrame)->pd.DataFrame:
        """
        remove non english tweets from lang
        """
        df.drop(df[df['lang'] != 'en'].index,inplace=True)
        return df
    def clean_hashtags(self, df:pd.DataFrame)->pd.DataFrame:
        t=[]
        k=[]
        mystr=''
        for i in range(len(df['hashtags'])):
            try:
                t.append(df['hashtags'][i].replace('\'','"').replace('},','},,').split(',,'))
                if len(t[i][0])>0:
                    for js in t[i] :
                        mystr=mystr+json.loads(js)['text']+ ', '
                    mystr=mystr[0:-2]
                    k.append(mystr)
                    mystr='' 
                else:
                    k.append('')    
            except:
                t.append(['error'])
                k.append('')
        df['hashtags'] = k
        return df
    def clean_user_mentions(self, df:pd.DataFrame)->pd.DataFrame:
        t=[]
        k=[]
        mystr=''
        for i in range(len(df['user_mentions'])):
            try:
                t.append(df['user_mentions'][i].replace('\'','"').replace('},','},,').split(',,'))
                if len(t[i][0])>0:
                    for js in t[i] :
                        mystr=mystr+json.loads(js)['screen_name']+ ', '
                    mystr=mystr[0:-2]
                    k.append(mystr)
                    mystr='' 
                else:
                    k.append('')    
            except:
                t.append(['error'])
                k.append('')
        df['user_mentions'] = k
        return df
    def get_clean_tweet_df(self, save=False)->pd.DataFrame:
        clean_tweet_df = self.drop_unwanted_column(self.df)
        clean_tweet_df = self.convert_to_datetime(clean_tweet_df)
        clean_tweet_df = self.drop_duplicate(clean_tweet_df)
        clean_tweet_df = self.convert_to_numbers(clean_tweet_df)
        clean_tweet_df = self.remove_non_english_tweets(clean_tweet_df)
        clean_tweet_df = self.clean_hashtags(clean_tweet_df)
        clean_tweet_df = self.clean_user_mentions(clean_tweet_df)
        if save:
            clean_tweet_df.to_csv('processed_clean_tweet_data.csv', index=False)
            print('File Successfully Saved.!!!')
        return clean_tweet_df



