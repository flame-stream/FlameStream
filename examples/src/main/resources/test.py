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

        return X_train, X_test, y_train, y_test

    df = pd.read_csv('news_lenta.csv').head(100000)
    df = df.dropna()
    df = df[df['tags'] != 'Все']

    # Prepare features
    X = df['text']
    y = df['tags']

    cntVectorizer = CountVectorizer()
    X = cntVectorizer.fit_transform(X)
    X = TfidfTransformer().fit_transform(X)
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.33)

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

    return X_train, X_test, y_train, y_test


def save_weights(classifier, X_test, classes):
    if os.path.isfile('meta_data') and os.path.isfile('test_data'):
        print("classifier_weights already saved")
        return

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

    test_amount = 5
    with open('sklearn_prediction', 'w') as t:
        prevX = X_test
        X_test = X_test.toarray()

        t.write("%s\n" % test_amount)
        count = 0
        for test_line in X_test:
            for test_item in test_line:
                t.write("%s " % test_item)

            t.write("\n")

            probs = classifier.predict_proba(prevX[count])
            for prob_vector in probs:  # only one row
                for item in prob_vector:
                    t.write("%s " % item)

            t.write("\n")

            count += 1
            if count == test_amount:
                break

    print("classifier_weights for lenta ru saved")


def main():
    X_train, X_test, y_train, y_test = get_data()
    classifier = get_model(X_train, y_train)

    predicted = classifier.predict(X_test)
    classes = np.unique(y_train)
    save_weights(classifier, X_test, classes)

    # check predict proba lr of the first document
    first = X_test[0]
    res = classifier.predict_proba(first)


# assume, that news_lenta.csv in the same folder
if __name__ == "__main__":
    main()
