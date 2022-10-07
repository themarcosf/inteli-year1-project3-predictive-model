def fraud_incidents_buckets(row):  
    if row['FRAUD_INCIDENTS'] == 0:
        return 1
    elif row['FRAUD_INCIDENTS'] > 0 and row['FRAUD_INCIDENTS'] < 6:
        return 2
    elif row['FRAUD_INCIDENTS'] >= 6 and row['FRAUD_INCIDENTS'] < 11:
        return 3
    elif row['FRAUD_INCIDENTS'] >= 11 and row['FRAUD_INCIDENTS'] < 21:
        return 4
    elif row['FRAUD_INCIDENTS'] >= 21 and row['FRAUD_INCIDENTS'] < max:
        return 5
    return 0

def discipline_incidents_buckets(row):  
    if row['DISCIPLINE_INCIDENTS'] == 0:
        return 1
    elif row['DISCIPLINE_INCIDENTS'] > 0 and row['DISCIPLINE_INCIDENTS'] < 6:
        return 2
    elif row['DISCIPLINE_INCIDENTS'] >= 6 and row['DISCIPLINE_INCIDENTS'] < 11:
        return 3
    elif row['DISCIPLINE_INCIDENTS'] >= 11 and row['DISCIPLINE_INCIDENTS'] < 21:
        return 4
    elif row['DISCIPLINE_INCIDENTS'] >= 21 and row['DISCIPLINE_INCIDENTS'] < max:
        return 5
    return 0
