import joblib

model = joblib.load('dump\model_lgbm.model')
scaler = joblib.load('dump\scaler_model_lgbm.scaler')

def predictive_model(dataframe):
    
    x = scaler.transform(dataframe.drop(columns=['IS_ACTIVE', 'ID']))
    
    predictions = model.predict_proba(x)
    
    return predictions
