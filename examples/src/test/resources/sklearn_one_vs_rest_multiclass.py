import numpy as np
from sklearn.linear_model import SGDClassifier
from scipy.sparse import csc_matrix


def parseDoubles(ls):
    ans = []
    for i in range(0, len(ls), 2):
        ans.append((int(ls[i]), float(ls[i + 1])))
    return ans


def readFrom(file):
    inf = open(file, 'r')
    y = []
    row = []
    col = []
    val = []
    j = 0
    size, features = map(int, inf.readline().split(' '))
    for line in inf.readlines():
        tmp = line.split(',')
        y.append(tmp[0])
        for p in parseDoubles(tmp[1:]):
            row.append(j)
            col.append(p[0])
            val.append(p[1])
        j += 1
    X = csc_matrix((val, (row, col)), shape = (size, features))
    return (X, y)


def main():
    X_train, y_train = readFrom('tmp_train')
    X_test, y_test = readFrom('tmp_test')

    #classifier = SGDClassifier(loss='log', class_weight='balanced', n_jobs=-1, tol=1e-6, max_iter=2000, random_state=42)
    classifier = SGDClassifier(
        loss="log", class_weight='balanced', tol=1e-6,
        penalty='l1', alpha=0.00000495, n_jobs=-1, random_state=42, max_iter=5000
    )
    classifier.fit(X_train, y_train)
    predicted = classifier.predict(X_test)
    print(np.mean(predicted == y_test))

if __name__ == "__main__":
    main()