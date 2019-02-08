from pandas import Series
from sklearn.feature_extraction.text import CountVectorizer, TfidfTransformer
from sklearn.linear_model import SGDClassifier
from sklearn.model_selection import train_test_split
from stop_words import get_stop_words

import pandas as pd
import numpy as np

class ClassifierBuilder:
    def __init__(self):
        self._vectorizer_path = None
        self._weights_path = None
        self._tests_path = None
        self._tests_num = None
        self._test_quality_iters = None

    def save_vectorizer(self, path: str) -> 'ClassifierBuilder':
        self._vectorizer_path = path
        return self

    def save_weights(self, path: str) -> 'ClassifierBuilder':
        self._weights_path = path
        return self

    def save_tests(self, path: str, num: int) -> 'ClassifierBuilder':
        self._tests_path = path
        self._tests_num = num
        return self

    def test_quality(self, iters: int):
        self._test_quality_iters = iters
        return self

    def build(self, texts: Series, topics: Series):
        token_pattern = r"(?u)\b\w\w+\b"
        vectorizer = CountVectorizer(
            token_pattern=token_pattern,
            stop_words=get_stop_words('russian'),
            max_features=50000
        )
        X = vectorizer.fit_transform(texts)
        transformer = TfidfTransformer()
        X = transformer.fit_transform(X)

        if self._vectorizer_path is not None:
            with open(self._vectorizer_path, 'w') as f:
                for key, value in vectorizer.vocabulary_.items():
                    f.write("%s " % key)
                    f.write("%s \n" % value)

        classifier = SGDClassifier(
            loss="log", class_weight='balanced',
            penalty='l1', alpha=0.0000009, n_jobs=-1
        )
        if self._weights_path is not None or self._tests_path is not None:
            classifier.fit(X, topics)

            with open(self._weights_path, 'w') as f:
                f.write("%s " % classifier.coef_.shape[0])    # amount of classes
                f.write("%s \n" % classifier.coef_.shape[1])  # amount of features
                for line in classifier.classes_:
                    f.write("%s \n" % line)
                for line in classifier.coef_:
                    for index, item in enumerate(line):
                        if item != 0.0:
                            f.write("%s %s " % (index, item))
                    f.write("\n")
                for item in classifier.intercept_:
                    f.write("%s " % item)

            if self._tests_path is not None:
                with open(self._tests_path, 'w') as t:
                    t.write("%s %s\n" % (self._tests_num, X.shape[1]))
                    count = 0

                    for index in range(X.shape[0]):
                        doc = X[index]
                        probs = classifier.predict_proba(doc)
                        for item in probs[0]:
                            t.write("%s " % item)
                        t.write("\n")

                        orig_doc = texts[index]
                        t.write("%s \n" % orig_doc)
                        for item in doc.toarray()[0]:
                            t.write("%s " % item)
                        t.write("\n")
                        count += 1
                        if count == self._tests_num:
                            break

        if self._test_quality_iters is not None:
            for i in range(self._test_quality_iters):
                X_train, X_test, y_train, y_test = train_test_split(X, topics, test_size=0.33)
                classifier.fit(X_train, y_train)
                predicted = classifier.predict(X_test)
                print('Accuracy', np.mean(predicted == y_test))

        return classifier


def main():
    df = pd.read_csv('news_lenta.csv', nrows=500000)
    df = df.dropna()
    df = df[df['tags'] != 'Все']
    X = df['text']
    y = df['tags']

    ClassifierBuilder() \
        .test_quality(3) \
        .save_vectorizer('cnt_vectorizer') \
        .save_weights('classifier_weights') \
        .save_tests('sklearn_prediction', 10) \
        .build(X, y)


if __name__ == "__main__":
    main()
