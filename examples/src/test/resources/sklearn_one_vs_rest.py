import numpy as np
import pandas as pd
from sklearn.feature_extraction.text import CountVectorizer, TfidfTransformer
from sklearn.linear_model import SGDClassifier
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from datatable.models import Ftrl
import datatable as dt


def main():
    df = pd.read_csv('news_lenta.csv', nrows=14000)
    df = df.dropna()
    df = df[df['tags'] != 'Все']
    df = df[df['tags'] != '']

    X = df['text']
    y = df['tags']

    processing = Pipeline([
        ('vect', CountVectorizer()),
        ('tfidf', TfidfTransformer())])
    X = processing.fit_transform(X)

    X_train = X[0:10000]
    y_train = [1 if (t == 'Политика') else 0 for t in y[0:10000]]
    X_test = X[10000:13000]
    y_test = [1 if (t == 'Политика') else 0 for t in y[10000:13000]]

    classifier = SGDClassifier(loss='log', class_weight='balanced', n_jobs=-1)
    classifier.fit(X_train, y_train)
    predicted = classifier.predict(X_test)
    print("Accuracy")
    print(np.mean(predicted == y_test))
    print("Precision")
    print(np.sum(1 if (x == y and y == 1) else 0 for (x, y) in zip (predicted, y_test)) / np.sum(predicted))
    print("Recall")
    print(np.sum(1 if (x == y and y == 1) else 0 for (x, y) in zip (predicted, y_test))  / np.sum(y_test))

    # for i in range(20):
    #     X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.33)
    #
    #     classifier = SGDClassifier(loss='log', class_weight='balanced', n_jobs=-1)
    #     classifier.fit(X_train, y_train)
    #     predicted = classifier.predict(X_test)
    #     print("Classic")
    #     print(np.mean(predicted == y_test))

    ftrl_model = Ftrl()
    ftrl_model.labels = np.unique(y_train).tolist()
    ftrl_model.fit(dt.Frame(pd.DataFrame(X_train.todense())), dt.Frame(y_train))
    predicted = ftrl_model.predict(dt.Frame(X_test))
    print("FTRL")
    print(np.mean(predicted.to_pandas().idxmax(axis=1).reset_index(drop=True) == y_test.reset_index(drop=True)))


if __name__ == "__main__":
    main()