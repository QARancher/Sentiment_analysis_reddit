Get access to sentiment analysis of Reddit.

The following application is a one stop to collect any subreddit and classify each comment to good or bad.
the output of the application is a csv file with aggregated sentiment per day.

how to use:

--mode: in what mode to run. options=['download', 'analysis', 'all']

--subreddit: name of subreddit group to collect data from

--filters: extra arguments for qurey reddit

--start_date: start date in format '%Y-%m-%d'

--end_date: end date in format '%Y-%m-%d'

--flavor: which package to run for sentiment analysis. flair for more accurate and slower run, 
use which small amount of data or ntlk for quicker results and less accurate sentiment

(!) please note that cred.json file should be placed to the working directory.
it should contain the credentials to redis, as follow:

client_id: ""
client_secret: ""
redirect_uri: "http://localhost:8080"
user_agent: "text"


example: