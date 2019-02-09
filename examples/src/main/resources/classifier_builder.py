import numpy as np
import pandas as pd
from sklearn.feature_extraction.text import CountVectorizer, TfidfTransformer
from sklearn.linear_model import SGDClassifier
from sklearn.model_selection import train_test_split
from stop_words import get_stop_words


class ClassifierBuilder:
    def __init__(self):
        self._X_test = None
        self._y_test = None
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

    def save_tests(self, path: str) -> 'ClassifierBuilder':
        self._tests_path = path
        if self._X_test is None:
            self._tests_num = 0
        else:
            self._tests_num = len(self._X_test)
        return self

    def test_quality(self, iters: int, X_test=None, y_test=None):
        self._X_test = X_test
        self._y_test = y_test
        self._test_quality_iters = iters
        return self

    def build(self, texts: list, topics: list):
        token_pattern = r"(?u)\b\w\w+\b"
        vectorizer = CountVectorizer(
            token_pattern=token_pattern,
            stop_words=get_stop_words('russian')
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

        if self._test_quality_iters is not None:
            for i in range(self._test_quality_iters):
                X_train, X_test, y_train, y_test = train_test_split(X, topics, test_size=0.33)
                classifier.fit(X_train, y_train)
                predicted = classifier.predict(X_test)
                print('Accuracy', np.mean(predicted == y_test))

        if self._X_test is not None:
            X_test = self._X_test
            y_test = self._y_test

            classifier.fit(X, topics)
            sorted_topics = np.unique(topics)
            X_transformed = transformer.transform(vectorizer.transform(X_test))
            predicted = classifier.predict(X_transformed)
            print('Accuracy on real tests:', np.mean(predicted == y_test))
            # vocabulary = vectorizer.get_feature_names() : for human friendly features
            if self._tests_path is not None:
                with open(self._tests_path, 'w') as t:
                    t.write("%s %s\n" % (self._tests_num, X.shape[1]))

                    for index in range(len(X_test)):
                        doc = X_transformed[index]
                        probs = classifier.predict_proba(doc)
                        for item in probs[0]:
                            t.write("%s " % item)
                        t.write("\n")

                        orig_doc = X_test[index]
                        t.write("%s \n" % orig_doc)
                        for item in doc.toarray()[0]:
                            t.write("%s " % item)
                        t.write("\n")

                        print(orig_doc)
                        pred_topics = {}
                        for i in range(len(probs[0])):
                            probability = probs[0][i]
                            topic = sorted_topics[i]
                            pred_topics[topic] = probability

                        print(sorted(pred_topics.items(), key=lambda kv: kv[1], reverse=True))
                        print("______")

        if self._weights_path is not None or self._tests_path is not None:
            with open(self._weights_path, 'w') as f:
                f.write("%s " % classifier.coef_.shape[0])  # amount of classes
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

        return classifier


def split_data(df, tests_amount):
    X_train, X_test, y_train, y_test = [], [], [], []
    for i in range(df.shape[0]):
        try: # ???
            row = df.iloc[i]
            if i < tests_amount:
                X_test.append(row['text'])
                y_test.append(row['tags'])
            else:
                X_train.append(row['text'])
                y_train.append(row['tags'])
        except:
            print("key error at %s" % i)
            continue

    return X_train, X_test, y_train, y_test


def main():
    df = pd.read_csv('news_lenta.csv', nrows=500000)
    df = df.dropna()
    df = df[df['tags'] != 'Все']

    tests_amount = 10
    X_train, X_test, y_train, y_test = split_data(df, tests_amount)

    ClassifierBuilder() \
        .test_quality(1, X_test, y_test) \
        .save_vectorizer('cnt_vectorizer') \
        .save_weights('classifier_weights') \
        .save_tests('sklearn_prediction') \
        .build(X_train, y_train)


if __name__ == "__main__":
    main()
