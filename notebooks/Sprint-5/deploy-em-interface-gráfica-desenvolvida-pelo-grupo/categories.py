def prob_scale(predictions):
    prob_results = []
    
    for pred in predictions:
        if pred[0] > 0.7:
            prob_results.append("Churn")
        elif pred[2] > 0.7:
            prob_results.append("RT_Inconsistente")
        elif pred[1] >= 0.7:
            prob_results.append("Nao_Churn")
        elif pred[2] > pred[1]:
            prob_results.append("Provavel_Churn")
        elif pred[1] > pred[2]:
            prob_results.append("Provavel_Nao_Churn")

    return prob_results