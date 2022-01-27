import multiprocessing
from pathlib import Path

import pandas as pd


class Prediction:
    """
    Class for ML sentiment analysis. uses 'ntlk' or 'flair' liberies to analyse text.
    ntlk is faster but less accurate. mainly used for historical data.
    flair is slower with more accuracy. mainly used for stream real time data.
    only one of the libaries can be used and will be loaded depends on the flavor flag.
    """
    def __init__(self, flavor: str = 'ntlk'):
        if 'ntlk' == flavor:
            if not Path(f'/home/reddit/{flavor}_data/').exists():
                import nltk
                nltk.download('vader_lexicon')
            from nltk.sentiment import SentimentIntensityAnalyzer
            self.sia = SentimentIntensityAnalyzer()

        elif 'flair':
            from flair.models import TextClassifier
            self.sia = TextClassifier.load('en-sentiment')

    def flair_prediction(self, text):
        try:
            from flair.data import Sentence
            sentence = Sentence(text)
            self.sia.predict(sentence)

            score = sentence.labels[0]
            if "POSITIVE" in str(score):
                return 1
            elif "NEGATIVE" in str(score):
                return -1
            else:
                return 0
        except Exception as e:
            print(f"Failed to get prediction of sentiment\n {e}")
            return 0

    def predict_ntlk(self, text: str) -> int:
        """True if text has positive compound sentiment, False otherwise."""
        return 1 if self.sia.polarity_scores(text)["compound"] > 0 else -1

    def _apply_sentiment(self, df: pd.DataFrame):
        """
        calculate sentiment of a dataframe, column with text should be named 'title' or 'body'.
        removed comments are removed from the df.
        :param df: raw dataframe with text as input
        :return: original df adding column 'sentiment' with the score.
        """
        col = 'body' if df.get('body') else 'title'
        if df.empty:
            return df
        df = df[df[col] != '[removed]']
        df['sentiment'] = df[col].apply(self.predict_ntlk)
        return df

    def apply_sentiment(self, df):
        """
        wrapper function for sentiment calculation, splits raw df to chunks using date.
        execute calculation in multy processing to speed up.
        :param df: raw df to split into chunks and analyse
        :return: original df adding column 'sentiment' with the score.
        """
        chunks = round(df.shape[0] / multiprocessing.cpu_count())
        list_sr = [df[i:i + chunks] for i in range(0, df.shape[0], chunks)]
        with multiprocessing.Pool() as pool:
            sentiment = pool.starmap(self._apply_sentiment, zip(list_sr))
        return pd.concat(sentiment, axis=0)

