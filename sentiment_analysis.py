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
            from nltk.sentiment import SentimentIntensityAnalyzer
            self.sia = SentimentIntensityAnalyzer()

        elif 'flair' == flavor:
            from flair.models import TextClassifier
            self.sia = TextClassifier.load('en-sentiment')
        else:
            raise Exception(f"The passed flavor {flavor} is not valid. please use 'ntlk' or 'flair'")
        self.flavor = flavor

    def _flair_analysis(self, text):
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

    def _ntlk_analysis(self, text: str) -> int:
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
        df['sentiment'] = df[col].apply(self.get_sentiment)
        return df

    def get_sentiment(self, text):
        """
        return sentiment function based on the flavor.
        :param text:
        :return:
        """
        return self._ntlk_analysis(text=text) if self.flavor == 'ntlk' else self._flair_analysis(text)

    def apply_sentiment(self, df) -> pd.DataFrame:
        """
        wrapper function for sentiment calculation, splits raw df to chunks using date.
        execute calculation in multy processing to speed up.
        :param df: raw df to split into chunks and analyse
        :return: original df adding column 'sentiment' with the score.
        """
        if df.empty or df.shape[0] == 0:
            print(f"got empty df, no text to analyze. returning original df {df}")
            return df
        chunks = round(df.shape[0] / multiprocessing.cpu_count())
        list_sr = [df[i:i + chunks] for i in range(0, df.shape[0], chunks)]
        with multiprocessing.Pool() as pool:
            sentiment = pool.starmap(self._apply_sentiment, zip(list_sr))
        return pd.concat(sentiment, axis=0)
