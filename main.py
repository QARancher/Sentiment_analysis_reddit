import argparse
import datetime as dt
from pathlib import Path

import numpy as np
import pandas as pd

from reddit import HistoricalReddit
from sentiment_analysis import Prediction
from utils import split_dates


def run_download(subreddits: list, start_date: dt, end_date: dt, additional_args: dict):
    print("Starting...")
    results = pd.DataFrame()
    for sub in subreddits:
        print(f"collecting from subreddit {sub}")
        result = download_comments_sentiment_for_subreddit(subreddit=sub, start_date=start_date, end_date=end_date,
                                                           additional_args=additional_args)
        results = pd.concat([result, results], axis=1)
    return results


def run_analysis(flavor):
    """
    run sentiment analysis and save to csv the output
    :param flavor: 'ntlk' or 'flair'
    """
    print("running analysis")
    df_result = pd.Dataframe()
    for file in Path(__file__).parent.glob('*.csv'):
        print(f"working file {file}")
        results = pd.read_csv(file, index_col=0, parse_dates=True)
        pred = Prediction(flavor=flavor)
        df_subs = pred.apply_sentiment(results)

        df_subs['date'] = pd.to_datetime(df_subs['date'])
        txt_col = 'body' if 'body' in df_subs.columns else 'title'
        df_subs = df_subs.drop(columns=[txt_col, 'created_utc', 'score']).sort_values(by='date')
        df_subs = df_subs.groupby(df_subs['date'].dt.date).agg(
            {'sentiment': np.sum, 'id': np.size}).rename(
            columns={'id': 'volume'})
        df_result = pd.concat([df_result, df_subs], axis=1)
    #  get subreddit name from one of the raw files, use the last one
    subreddit = file.name.split('_')[1]
    df_result.to_csv(f'{subreddit}.csv')
    print(f"saved {subreddit}.csv")


def download_comments_sentiment_for_subreddit(subreddit, start_date, end_date,
                                              additional_args: dict = None) -> pd.Dataframe:
    """
    download comments for specific subreddit
    :param subreddit: the name of the subreddit to work with
    :param start_date: since when to collect comments
    :param end_date: until when to collect comments
    :param additional_args: any filters that supported by PushshiftAPI
    :return: dataframe with raw historical data
    """
    r = HistoricalReddit(subreddit)
    query = [{'start_date': int(dat[0]), 'end_date': int(dat[1]), **additional_args} for dat in
             split_dates(start_date, end_date, freq="D")]
    if not query:
        raise Exception("Failed to construct a query")
    print(f"Collecting posts")
    return r.download_data(query)


def main(args):
    additional_args = {}
    subreddits = getattr(args, 'subreddit', [])
    start_date = getattr(args, 'start_date', dt.datetime(2017, 1, 1))
    end_date = getattr(args, 'end_date', dt.datetime.today())
    filters = getattr(args, 'filters')
    mode = getattr(args, 'mode')
    flavor = getattr(args, 'flavor', 'ntlk')

    if 'download' in mode:
        if filters:
            additional_args.update({v.split('=')[0]: v.split('=')[1] for v in filters})
        run_download(subreddits=subreddits, start_date=start_date, end_date=end_date,
                     additional_args=additional_args)
    elif 'analysis' in mode:
        run_analysis(flavor)
    elif 'all' in mode:
        if filters:
            additional_args.update({v.split('=')[0]: v.split('=')[1] for v in filters})
        run_download(subreddits=subreddits, start_date=start_date, end_date=end_date,
                     additional_args=additional_args)
        run_analysis(flavor)
    else:
        raise Exception(f"wrong mode {mode}")
    print("Done")


if __name__ == '__main__':
    """
    Download historical data from reddit and analyse its sentiment
    1. download only subreddit 
    2. sentiment analysis only
    3. download + sentiment
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", required=True, type=str, choices=['download', 'analysis', 'all'],
                        help="in what mode to run. options=['download', 'analysis', 'all']")
    parser.add_argument("--subreddit", required=False, type=str, nargs='+',
                        help="name of subreddit group to collect data from")
    parser.add_argument("--filters", required=False, type=str, nargs='+', help="extra arguments for qurey reddit")
    parser.add_argument("--start_date", required=True, type=lambda x: dt.datetime.strptime(x, '%Y-%m-%d'),
                        help="start date in format '%Y-%m-%d'")
    parser.add_argument("--end_date", required=False, type=lambda x: dt.datetime.strptime(x, '%Y-%m-%d'),
                        default=dt.datetime.today())
    parser.add_argument("--flavor", required=False, type=str, default='ntlk',
                        help="which package to run for sentiment analysis. flair for more accurate and slower run, "
                             "use which small amount of data or ntlk for quicker results and less accurate sentiment")
    args = parser.parse_args()
    main(args)
