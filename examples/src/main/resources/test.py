import os.path
import pickle

import pandas as pd
import numpy as np
from sklearn.feature_extraction.text import CountVectorizer, TfidfTransformer
from sklearn.linear_model import SGDClassifier
from sklearn.model_selection import train_test_split
import re

cnt_var = None
tfidf_var = None

def get_data():
    global cnt_var
    global tfidf_var
    limit = 100

    df = None
    if os.path.isfile("lenta_backup"):
        df = pickle.load(open("lenta_backup", 'rb'))
    else:
        df = pd.read_csv('news_lenta.csv')
        df = df.head(limit).dropna()
        df = df[df['tags'] != 'Все']

    pickle.dump(df, open("lenta_backup", 'wb'))
    # Prepare features
    X = df['text']
    y = df['tags']

    original_docs = X
    cntVectorizer = CountVectorizer()
    X = cntVectorizer.fit_transform(X)
    cnt_var = cntVectorizer

    tfidfTransformer = TfidfTransformer()
    X = tfidfTransformer.fit_transform(X)
    tfidf_var = tfidfTransformer

    X_indices = range(X.shape[0])
    y_indices = range(y.shape[0])
    X_traini, X_testi, y_traini, y_testi = train_test_split(X_indices, y_indices, test_size=0.33)
    # but X_traini == y_traini and X_testi == y_testi

    X = np.array(X.toarray())
    y = np.array(y.tolist())
    X_train = [X[i] for i in X_traini]
    X_test = [X[i] for i in X_testi]
    y_train = [y[i] for i in y_traini]
    y_test = [y[i] for i in y_testi]

    with open("cnt_vectorizer", 'w') as f:
        cnt_dict = cntVectorizer.vocabulary_
        for key in cnt_dict:
            f.write("%s " % key)
            f.write("%s \n" % cnt_dict[key])

    return original_docs, X, X_train, X_test, X_testi, y_train, y_test


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


def save_weights(classifier, original_docs, X, X_test, X_testi, classes):
    if os.path.isfile('classifier_weights') and os.path.isfile('sklearn_prediction'):
        print("classifier_weights already saved")
        # return

    save_classifier_weights(classifier, classes)
    test_amount = 1

    """
        format:
        probabilities of the sklearn prediction
        original text of the document
        tfidf representation of the document
    """
    with open('sklearn_prediction', 'w') as t:
        t.write("%s %s\n" % (test_amount, len(X_test[0])))
        count = 0

        for index in X_testi:
            doc = X[index]

            probs = classifier.predict_proba([doc])
            for prob_vector in probs:  # only one row
                for item in prob_vector:
                    t.write("%s " % item)

            t.write("\n")

            orig_doc = original_docs[index]
            t.write("%s \n" % orig_doc)
            for item in doc:
                t.write("%s " % item)
            t.write("\n")

            print(orig_doc)

            # java way:
            # tokenize = re.compile(r"(?u)\b\w\w+\b")
            # words = tokenize.findall(orig_doc.lower())

            # for word in words:
            #    feature = cnt_var.vocabulary_[word]
            #    my_vectorized[feature] += 1

            cv_vectorized = cnt_var.transform([orig_doc]).toarray()[0]
            # cv_vectorized == my_vectorized

            tfidf_vectorized = tfidf_var.transform([cv_vectorized])
            # print(tfidf_vectorized1 == tfidf_vectorized2)
            tokenize = re.compile(r"(?u)\b\w\w+\b")
            words = tokenize.findall(orig_doc.lower())

            my_vec = [0.] * len(cnt_var.vocabulary_)
            print(len(doc))

            for word in set(words):
                feature = cnt_var.vocabulary_[word]
                tfidf_feature = doc[feature]
                my_vec[feature] += tfidf_feature

            print("my vectorization")
            print(list(my_vec))

            print("sklearn")
            print(list(doc))

            print(list(my_vec) == list(doc))
            # print("sklearn answer")
            real_ans = classifier.predict_proba([doc])
            # print(real_ans)
            # print("-------------------")

            my_weights = classifier.coef_
            my_vec = tfidf_vectorized.toarray()[0]
            my_ans = np.array(my_weights).dot(doc)  # with my_vec should be same result

            count += 1
            if count == test_amount:
                break

    print("classifier_weights for lenta ru saved")


def main():
    original_docs, X, X_train, X_test, X_testi, y_train, y_test = get_data()
    classifier = SGDClassifier(loss="log", class_weight='balanced')

    classifier.fit(X_train, y_train)

    classes = np.unique(y_train)
    save_weights(classifier, original_docs, X, X_test, X_testi, classes)

    predicted = classifier.predict(X_test)
    print(np.mean(predicted == y_test))


# assume, that news_lenta.csv in the same folder
if __name__ == "__main__":
    main()
