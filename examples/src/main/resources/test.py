import pandas as pd
from sklearn.feature_extraction.text import CountVectorizer, TfidfTransformer
from sklearn.linear_model import SGDClassifier


def main():
    # Read and clean data
    df = pd.read_csv('news_lenta.csv', nrows=500000)
    df = df.dropna()
    df = df[df['tags'] != 'Все']

    orig_docs = df['text']
    # Prepare features
    X = df['text']
    y = df['tags']

    token_pattern = r"(?u)\b\w\w+\b"
    vectorizer = CountVectorizer(token_pattern=token_pattern)
    X = vectorizer.fit_transform(X)
    transformer = TfidfTransformer()
    X = transformer.fit_transform(X)

    with open("cnt_vectorizer", 'w') as f:
        for key, value in vectorizer.vocabulary_.items():
            f.write("%s " % key)
            f.write("%s \n" % value)

    # token_pattern = re.compile(token_pattern)
    # for i in range(len(df)):
    #     txt = df.iloc[i]['text'].lower()
    #     words = token_pattern.findall(txt)
    #
    #     res = np.full(len(vectorizer.vocabulary_), 0.0)
    #     for word in set(words):
    #         index = vectorizer.vocabulary_[word]
    #         res[index] = X[i, index]
    #
    #     # print(np.sum(res))
    #     # print(np.sum(X[i].toarray()))
    #     # print('####')

    classifier = SGDClassifier(loss="log", class_weight='balanced', penalty='l1')
    classifier.fit(X, y)

    with open('classifier_weights', 'w') as f:
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

    classifier.predict_proba(X[0])

    test_amount = 5
    with open('sklearn_prediction', 'w') as t:
        t.write("%s %s\n" % (test_amount, X.shape[1]))
        count = 0

        for index in range(X.shape[0]):
            doc = X[index]
            probs = classifier.predict_proba(doc)
            for item in probs[0]:
                t.write("%s " % item)
            t.write("\n")

            orig_doc = orig_docs[index]
            t.write("%s \n" % orig_doc)
            for item in doc.toarray()[0]:
                t.write("%s " % item)
            t.write("\n")
            count += 1
            if count == test_amount:
                break

    # Test
    # for i in range(20):
    #     X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.33)
    #     classifier.fit(X_train, y_train)
    #     predicted = classifier.predict(X_test)
    #     print(np.mean(predicted == y_test))
    #
    # print()


if __name__ == "__main__":
    main()
