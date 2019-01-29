import os.path
import pickle

import pandas as pd
import numpy as np
from sklearn.feature_extraction.text import CountVectorizer, TfidfTransformer
from sklearn.linear_model import SGDClassifier
from sklearn.model_selection import train_test_split


def get_model(X_train, y_train):
    filename = 'model.sav'
    if os.path.isfile(filename):
        print("loading model")
        loaded_model = pickle.load(open(filename, 'rb'))
        return loaded_model

    classifier = SGDClassifier(loss="log", class_weight='balanced')
    classifier.fit(X_train, y_train)

    # saving model
    pickle.dump(classifier, open(filename, 'wb'))

    return classifier


# news_lenta.csv -- locally saved
def get_data():
    model_filename = 'model.sav'
    if os.path.isfile(model_filename):
        X_train = pickle.load(open("X_train", 'rb'))
        X_test = pickle.load(open("X_test", 'rb'))
        y_train = pickle.load(open("y_train", 'rb'))
        y_test = pickle.load(open("y_test", 'rb'))
        X = pickle.load(open("X", 'rb'))
        original_docs = pickle.load(open("original_docs", 'rb'))

        return original_docs, X, X_train, X_test, y_train, y_test

    limit = 100000
    df = pd.read_csv('news_lenta.csv')
    print(df.shape)
    df = df.head(limit).dropna()
    df = df[df['tags'] != 'Все']

    # Prepare features
    X = df['text']
    y = df['tags']

    original_docs = X
    cntVectorizer = CountVectorizer()
    X = cntVectorizer.fit_transform(X)

    tfidfVectorizer = TfidfTransformer()
    X = tfidfVectorizer.fit_transform(X)
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.33)

    with open("idf_matrix", 'w') as f:
        for item in tfidfVectorizer.idf_:
            f.write("%s " % item)

        f.write("\n")

    with open("cnt_vectorizer", 'w') as f:
        cnt_dict = cntVectorizer.vocabulary_
        for key in cnt_dict:
            f.write("%s " % key)
            f.write("%s \n" % cnt_dict[key])

    # save sample: csv lenta too long
    pickle.dump(X_train, open("X_train", 'wb'))
    pickle.dump(X_test, open("X_test", 'wb'))
    pickle.dump(y_train, open("y_train", 'wb'))
    pickle.dump(y_test, open("y_test", 'wb'))
    pickle.dump(X, open("X", 'wb'))
    pickle.dump(original_docs, open("original_docs", 'wb'))

    return original_docs, X, X_train, X_test, y_train, y_test


def save_classifier_weights(classifier, classes):
    with open('classifier_weights', 'w') as f:
        f.write("%s " % classifier.coef_.shape[0])  # amount of classes
        f.write("%s \n" % classifier.coef_.shape[1])  # amount of features

        for line in classes:
            f.write("%s \n" % line)

        for line in classifier.coef_:
            for item in line:
                f.write("%s " % item)

            f.write("\n")
        for item in classifier.intercept_:
            f.write("%s " % item)


def save_weights(classifier, original_docs, X, X_test, classes):
    if os.path.isfile('classifier_weights') and os.path.isfile('sklearn_prediction'):
        print("classifier_weights already saved")
        return

    save_classifier_weights(classifier, classes)
    test_amount = 3

    """
        probs ans
        original text
        tfidf representation
    """
    with open('sklearn_prediction', 'w') as t:
        prevX = X_test
        X_test = X_test.toarray()

        t.write("%s %s\n" % (test_amount, X_test.shape[1]))
        count = 0

        for test_line in X_test:
            #for test_item in test_line:
            #    t.write("%s " % test_item)

            #t.write("\n")

            doc = prevX[count]
            probs = classifier.predict_proba(doc)
            for prob_vector in probs:  # only one row
                for item in prob_vector:
                    t.write("%s " % item)

            t.write("\n")

            ind = -1
            print(X.shape)
            """
                extremeeeely dangerous
                X -- csr matrix, it's has non-consecutive indices
                
                we need an index of the document, for retrieving original document, but it's very full of caveats...
            """
            for i in range(X.shape[0]):
                line = X.getrow(i)
                diff = line - doc

                if diff.nnz == 0:
                    ind = i
                    break

            print(original_docs.shape)
            print(ind)
            orig_doc = original_docs[ind]

            # tfidf map
            print(orig_doc)
            print()
            t.write("%s \n" % orig_doc)
            for item in doc.toarray()[0]:
                t.write("%s " % item)

            t.write("\n")

            count += 1
            if count == test_amount:
                break

    print("classifier_weights for lenta ru saved")


def main():
    original_docs, X, X_train, X_test, y_train, y_test = get_data()
    classifier = get_model(X_train, y_train)

    predicted = classifier.predict(X_test)
    classes = np.unique(y_train)
    save_weights(classifier, original_docs, X, X_test, classes)

    # check predict proba lr of the first document
    first = X_test[0]
    res = classifier.predict_proba(first)


# assume, that news_lenta.csv in the same folder
if __name__ == "__main__":
    main()
