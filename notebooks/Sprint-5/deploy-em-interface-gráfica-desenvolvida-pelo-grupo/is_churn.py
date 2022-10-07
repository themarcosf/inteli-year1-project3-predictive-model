from model import predictive_model
from categories import prob_scale
from feature_eng import feature_eng

def is_churn(earnings, supply, infos_gerais, attendance_rate, 
                    incidentes_regras_rt, orders_done_cancel, product_return, 
                    comp_defects, contas_churn, tempo_resolucao_modal, distance_user):
    
    df = feature_eng(earnings, supply, infos_gerais, attendance_rate, 
                    incidentes_regras_rt, orders_done_cancel, product_return, 
                    comp_defects, contas_churn, tempo_resolucao_modal, distance_user)
    
    x = df.drop(columns=['ID', 'IS_ACTIVE'])
    
    predictions = predictive_model(x)

    results = prob_scale(predictions)

    df['RESULTS'] = results

    return df[['ID', 'RESULTS']]
