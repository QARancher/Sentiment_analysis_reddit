import datetime
import json
import threading
from multiprocessing.pool import ThreadPool
import asyncpraw
import pandas as pd
from psaw import PushshiftAPI
import praw
from utils import split_dates
from pathlib import Path
# cred = dict(
#     client_id="WEnBj2RgwnPo2yts9F99Gg",
#     client_secret="XEbxKnzknMOzFANmcwRYrLcDRTnynQ",
#     redirect_uri="http://localhost:8080",
#     user_agent="testscript"
#     )


class Reddit:
    def __init__(self, api_type: str = 'reddit'):
        """
        interface class for reddit's clients 
        :param api_type: type of the reddit's api, reddit or reddit
        """
        with open(Path('cred.txt')) as cred_file:
            cred = json.load(cred_file)
        self.reddit = praw.Reddit(**cred) if 'reddit' == api_type else asyncpraw.Reddit(**cred)

    def _get_subreddit(self, subreddit_name):
        pass

    def search_comments(self, **kwargs):
        return self._search_func(kind='comment', **kwargs)

    def search_submissions(self, **kwargs):
        return self._search_func(kind='submission', **kwargs)


class RedditClient(Reddit):
    def __init__(self, subreddit_name):
        super().__init__(api_type='reddit')
        self.subreddit = self._get_subreddit(subreddit_name=subreddit_name)

    def _get_subreddit(self, subreddit_name):
        return self.reddit.subreddit(subreddit_name)


class AsyncReddit(Reddit):
    def __init__(self, subreddit_name):
        super().__init__(api_type='async_reddit')
        self.subreddit = self._get_subreddit(subreddit_name=subreddit_name)

    async def _search(self, **kwargs):
        for submission in self.subreddit.search(**kwargs):
            print(submission.title)

    async def _get_subreddit(self, subreddit_name):
        return await self.reddit.subreddit(subreddit_name, fetch=True)

    def get_top_posts(self, filter: str = 'day'):
        return [submission for submission in self.subreddit.top(filter)]

    async def get_post_by_id(self, id):
        return await self.reddit.submission(id=id)

    async def get_comments_of_post(self, submission):
        comments = await submission.comments()
        for top_level_comment in comments:
            print(top_level_comment.body)

    def get_comments_stream(self):
        for comment in self.reddit.stream.comments():
            print(comment.body)

    def get_traffic(self):
        return self.reddit.traffic()


attempts = 3


class HistoricalReddit(RedditClient):
    def __init__(self, subreddit_name):
        super().__init__(subreddit_name)
        self.api = PushshiftAPI(self.reddit)
        self.col_names = ['created_utc', 'id', 'body', 'score']

    def _get_historical_post(self, start_date, end_date, **kwargs):
        col_names = ['id', 'title', 'score', 'created_utc']
        s_d = datetime.datetime.fromtimestamp(start_date).strftime('%Y-%m-%d_%H-%M')
        e_d = datetime.datetime.fromtimestamp(end_date).strftime('%Y-%m-%d_%H-%M')
        last_date = None
        response = []
        for i in range(attempts):
            print(f"Search for {s_d} until {e_d}; {i}; {threading.current_thread().name}")
            for sub in self.api.search_submissions(after=start_date,
                                                   before=end_date,
                                                   subreddit=self.subreddit.display_name,
                                                   **kwargs):
                last_date = int(getattr(sub, 'created_utc'))
                print(f"done with {s_d} - {e_d}; {i}")
                response.append({k: getattr(sub, k) for k in vars(sub) if k in col_names})
            if not last_date or not response:
                # retry
                print(f"empty response from API. retry {s_d} - {e_d}; {i}; {threading.current_thread().name}")
                continue
            return response

    def get_historical_posts(self, query):
        start_date = query.get('start_date')
        end_date = query.get('end_date')
        query.pop('end_date')
        query.pop('start_date')
        df = pd.DataFrame()
        dates = split_dates(start_date, end_date, freq="H")
        for dat in dates:
            df = df.append(self._get_historical_post(start_date=int(dat[0]), end_date=int(dat[1]), **query),
                           ignore_index=True)
            if df.empty:
                print(f"df is empty {int(dat[0])} - {int(dat[1])}")
                continue
            df['date'] = pd.to_datetime(df['created_utc'], utc=True, unit='s')
        df.to_csv(
            f"raw_posts_{self.subreddit.display_name}_{df['date'].tail(1).dt.strftime('%Y-%m-%d_%H-%M').values[0]}_{df['date'].head(1).dt.strftime('%Y-%m-%d_%H-%M').values[0]}.csv")

        return df

    def _get_historical_comments(self, start_date: int, end_date: int, **kwargs) -> [dict]:
        col_names = ['id', 'title', 'score', 'created_utc']
        s_d = datetime.datetime.fromtimestamp(start_date).strftime('%Y-%m-%d_%H-%M')
        e_d = datetime.datetime.fromtimestamp(end_date).strftime('%Y-%m-%d_%H-%M')
        last_date = None
        response = []
        for i in range(attempts):
            print(f"Search for {s_d} until {e_d}; {i}; {threading.current_thread().name}")
            for sub in self.api.search_comments(after=start_date,
                                                before=end_date,
                                                subreddit=self.subreddit.display_name,
                                                **kwargs):
                last_date = int(getattr(sub, 'created_utc'))
                print(f"done with {s_d} - {e_d}; {i}")
                response.append({k: getattr(sub, k) for k in vars(sub) if k in col_names})
            if not last_date or not response:
                # retry
                print(f"empty response from API. retry {s_d} - {e_d}; {i}; {threading.current_thread().name}")
                continue
            return response

    def get_historical_comments(self, query):
        """
        get historical comment, this func aggregates all the response from api to DF
        """
        start_date = query.get('start_date')
        end_date = query.get('end_date')
        query.pop('end_date')
        query.pop('start_date')
        df = pd.DataFrame()
        dates = split_dates(start_date, end_date, "H")
        for dat in dates:
            df = df.append(self._get_historical_comments(start_date=int(dat[0]), end_date=int(dat[1]), **query),
                           ignore_index=True)
            if df.empty:
                print(f"df is empty {int(dat[0])} - {int(dat[1])}")
                continue
            df['date'] = pd.to_datetime(df['created_utc'], utc=True, unit='s')
        df.to_csv(
            f"raw_posts_{self.subreddit.display_name}_{df['date'].tail(1).dt.strftime('%Y-%m-%d_%H-%M').values[0]}_{df['date'].head(1).dt.strftime('%Y-%m-%d_%H-%M').values[0]}.csv")

        return df

    def download_data(self, query):
        with ThreadPool() as thrd:
            results = thrd.map(self.get_historical_posts, query)
        return pd.concat(results, axis=0)
