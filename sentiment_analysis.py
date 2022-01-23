import multiprocessing
from pathlib import Path

import pandas as pd


class Prediction:
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
        col = 'body' if df.get('body') else 'title'
        if df.empty:
            return df
        print(
            f" sentiment from {df['date'].tail(1).dt.strftime('%Y-%m-%d_%H-%M').values[0]} until"
            f" {df['date'].head(1).dt.strftime('%Y-%m-%d_%H-%M').values[0]}"
            f"size: {df.shape[0]}")
        df = df[df[col] != '[removed]']
        df['sentiment'] = df[col].apply(self.predict_ntlk)
        return df

    def apply_sentiment(self, df):
        chunks = round(df.shape[0] / multiprocessing.cpu_count())
        list_sr = [df[i:i + chunks] for i in range(0, df.shape[0], chunks)]
        with multiprocessing.Pool() as pool:
            sentiment = pool.starmap(self._apply_sentiment, zip(list_sr))
        return pd.concat(sentiment, axis=0)

