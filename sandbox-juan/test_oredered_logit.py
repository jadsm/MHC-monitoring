import pandas as pd
from statsmodels.miscmodels.ordinal_model import OrderedModel

# Example: Predicting "Satisfaction" (0, 1, 2) based on two features
df = pd.DataFrame({
    'satisfaction': [0, 1, 2, 0, 1, 2, 1, 0, 2], # Ordered: Low, Med, High
    'feature1': [1.2, 2.3, 3.1, 1.0, 2.0, 3.5, 2.1, 1.1, 3.8],
    'feature2': [0.5, 0.8, 1.2, 0.4, 0.9, 1.5, 0.7, 0.3, 1.9]
})

# Define the model (distr='logit' for Ordered Logit, 'probit' for Ordered Probit)
mod = OrderedModel(df['satisfaction'], df[['feature1', 'feature2']], distr='logit')

# Fit the model
res = mod.fit(method='bfgs')

# Summary shows the coefficients for features and the "cutpoints" (thresholds)
print(res.summary())