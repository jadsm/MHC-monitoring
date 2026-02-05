import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.base import clone

class FrankHallOrdinalClassifier:
    def __init__(self, base_estimator):
        self.base_estimator = base_estimator
        self.clfs = []

    def fit(self, X, y):
        self.unique_class = np.sort(np.unique(y))
        self.clfs = []
        # Create (K-1) binary classifiers
        for i in range(len(self.unique_class) - 1):
            # Binary target: Is label > current class?
            binary_y = (y > self.unique_class[i]).astype(int)
            clf = clone(self.base_estimator)
            clf.fit(X, binary_y)
            self.clfs.append(clf)
        return self

    def predict_proba(self, X):
        # Collect probabilities from all binary models
        probs = [clf.predict_proba(X)[:, 1] for clf in self.clfs]
        probs = np.column_stack(probs)
        
        # Convert cumulative probabilities back to individual class probabilities
        final_probs = np.zeros((X.shape[0], len(self.unique_class)))
        final_probs[:, 0] = 1 - probs[:, 0] # Prob(Class 1)
        for i in range(1, len(self.unique_class) - 1):
            final_probs[:, i] = probs[:, i-1] - probs[:, i] # Prob(Class i)
        final_probs[:, -1] = probs[:, -1] # Prob(Last Class)
        
        return final_probs

    def predict(self, X):
        return self.unique_class[np.argmax(self.predict_proba(X), axis=1)]

# Usage:
model = FrankHallOrdinalClassifier(RandomForestClassifier())
model.fit(X_train, y_train)
predictions = model.predict(X_test)
