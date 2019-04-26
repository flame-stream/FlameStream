# coding=utf-8
import time
import numpy as np
import pandas as pd
import sys
from sklearn.feature_extraction.text import CountVectorizer, TfidfTransformer
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
import scipy.sparse as sp

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

def calcWindowIdf(X, windowSizeDays, lam):
    X = X.astype(float)
    size, features = X.shape
    idf = sp.csr_matrix(([], ([], [])), shape=(1, features)).tolil()
    #cur_df = sp.csr_matrix(([], ([], [])), shape=(1, features)).tolil()

    windowSize = windowSizeDays * 200

    idf = np.array([0] * features, dtype=float)
    ans = []
    cur = 0
    index = 0
    for tf in X:
        # Go to the next window if it's time
        if index - cur >= windowSize:
            #idf += cur_df
            idf *= lam
            #cur_df = sp.csr_matrix(([], ([], [])), shape=(1, features)).tolil()
            cur = index

        # Update cur_idf
        for j in tf.nonzero()[1]:
            #cur_df[0, j] += 1
            idf[j] += 1

        tf = tf / tf.sum()
        tf = tf.multiply(np.log(size / idf))
        tf.data = tf.data + 1
        tf /= np.sqrt(tf.multiply(tf).sum())

        ans.append(tf)

        index += 1

    return ans

def main(argv):
    if len(argv) < 4:
        print("Usage {} trainSize testSize seed".format(argv[0]))
        exit(0)
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

    # processing = Pipeline([
    #     ('vect', CountVectorizer()),
    #     ('tfidf', TfidfTransformer(sublinear_tf=True))])
    vectorizer = CountVectorizer()
    X = vectorizer.fit_transform(X)

    print("Step inside calcWindowIdf")
    tm = time.monotonic()
    X = calcWindowIdf(X, 10, 0.99)
    print("Execution time is: ", time.monotonic() - tm)

    X = list(X)
    y = list(y)
    X = list(zip(range(len(X)), X))
    y = list(zip(range(len(y)), y))

    (X_train, X_test, y_train, y_test) = train_test_split(X, y, test_size=testSize, random_state=seed)

    X_train = [t[1] for t in sorted(X_train, key = lambda t: t[0])]
    y_train = [t[1] for t in sorted(y_train, key = lambda t: t[0])]

    X_test = [t[1] for t in sorted(X_test, key = lambda t: t[0])]
    y_test = [t[1] for t in sorted(y_test, key = lambda t: t[0])]

    put(X_train, y_train, "tmp_train")
    put(X_test, y_test, "tmp_test")

if __name__ == "__main__":
    main(sys.argv)