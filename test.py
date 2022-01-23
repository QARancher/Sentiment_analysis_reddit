from datetime import datetime

from reddit import HistoricalAsyncReddit, split_dates

a = HistoricalAsyncReddit("bitcoin")
res = []
for comment in a.api.search_submissions(after=int(datetime(2021, 3, 1).timestamp()),
                                        before=int(datetime(2021, 3, 2).timestamp()),
                                        subreddit="solana"):
    res.append({k: getattr(comment, k) for k in vars(comment) if k in a.col_names})
print(res)