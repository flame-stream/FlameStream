# coding=utf-8
import pandas as pd
import sys
from sklearn.feature_extraction.text import CountVectorizer, TfidfTransformer
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline


def put(X, y, file):
    ouf = open(file, "w+")
    ouf.write(str(len(X)) + ' ' + str(X[0].shape[1]) + '\n')
    for (x, y1) in zip(X, y):
        nz = x.nonzero()[1]
        row = x.toarray()[0]
        ouf.write(y1)
        for j in nz:
            ouf.write(',' + str(j) + ',' + str(row[j]))
        ouf.write('\n')
    ouf.close()

def main(argv):
    trainSize = int(argv[1])
    testSize = int(argv[2])
    seed = int(argv[3])
    df = pd.read_csv('news_lenta.csv', nrows=(trainSize + testSize)*2)
    df = df.dropna()
    df = df[df['tags'] != 'Все']
    df = df[df['tags'] != '']
    df = df[:trainSize + testSize]

    X = df['text']
    y = df['tags']

    processing = Pipeline([
        ('vect', CountVectorizer()),
        ('tfidf', TfidfTransformer())])
    X = processing.fit_transform(X)
    X = list(X)
    y = list(y)

    (X_train, X_test, y_train, y_test) = train_test_split(X, y, test_size=testSize, random_state=seed)


    put(X_train, y_train, "tmp_train")
    put(X_test, y_test, "tmp_test")

if __name__ == "__main__":
    main(sys.argv)