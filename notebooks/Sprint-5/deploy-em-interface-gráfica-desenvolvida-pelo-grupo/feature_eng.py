import pandas as pd
from incidents_buckets import fraud_incidents_buckets, discipline_incidents_buckets

def feature_eng(earnings, supply, infos_gerais, attendance_rate, 
                incidentes_regras_rt, orders_done_cancel, product_return, 
                comp_defects, contas_churn, tempo_resolucao_modal, distance_user):
  
    # import das bases
    df_earnings = pd.read_csv(earnings.name, delimiter=';')
    df_supply = pd.read_csv(supply.name, delimiter=';')
    df_info = pd.read_csv(infos_gerais.name, delimiter=';')
    df_attendance = pd.read_csv(attendance_rate.name, delimiter=';')
    df_incidentes = pd.read_csv(incidentes_regras_rt.name, delimiter=';')
    df_orders = pd.read_csv(orders_done_cancel.name, delimiter=';')
    df_productReturn = pd.read_csv(product_return.name, delimiter=';')
    df_defects = pd.read_csv(comp_defects.name, delimiter=';')
    df_churn = pd.read_csv(contas_churn.name, delimiter=';')
    df_resTime = pd.read_csv(tempo_resolucao_modal.name, delimiter=';')
    df_distance = pd.read_csv(distance_user.name, delimiter=';')

    # df churn e df infos gerais
    df_churn.drop(labels="Unnamed: 0", axis=1, inplace = True)
    df_churn_ = (df_churn["CITY"].value_counts(normalize=True).cumsum() < 0.8).to_frame()
    df_churn_ = df_churn_[df_churn_["CITY"] == True]
    filter = df_churn_.index.to_list()
    df_churn__ = df_churn[df_churn['CITY'].isin(filter)]

    df_info.replace(
        {"CIDADE": {
            'Sao Paulo': 'Grande São Paulo',
            'São Paulo': 'Grande São Paulo',
            'SÃO PAULO': 'Grande São Paulo',
            'BELO HORIZINTE': 'Belo Horizonte',
            'RIO DE JANEIRO': 'Rio de Janeiro'
        }}, inplace=True
    )
    df_info_ = df_info[df_info['CIDADE'].isin(filter)]

    filter = df_churn__["ID"].to_list()
    mask = df_info_["ID"].isin(filter)
    column_name = "IS_ACTIVE"
    df_info_.loc[mask, column_name] = "Quasi" 
    

    filter = df_info_["ID"].to_list()
    mask = ~df_churn__["ID"].isin(filter)
    df_churn__ = df_churn__[mask]

    df_churn__.rename(columns={
        "FIRST_NAME": "NOME", 
        "GENDER": "GENERO",
        "CITY": "CIDADE",
        "TRANSPORT_MEDIA_TYPE": "TRANSPORTE"
        }, inplace=True)

    df_churn__.drop(columns=["SK.CREATED_AT::DATE", "CARTAO", "FECHA_ULT", "LEVEL_NAME"], inplace=True)
    df_churn__["IS_ACTIVE"] = False

    df_consol = pd.concat([df_info_, df_churn__])
    df_consol.drop(columns=["NOME", "SOBRENOME"], inplace=True)

    df_consol["DATA_NASCIMENTO"] = df_consol["DATA_NASCIMENTO"].map(lambda x : x[:4], na_action="ignore")
    df_consol["DATA_NASCIMENTO"] = pd.to_numeric(df_consol["DATA_NASCIMENTO"])
    mean_avg_birthyear = round(df_consol["DATA_NASCIMENTO"].mean(), 0)
    df_consol["DATA_NASCIMENTO"].fillna(mean_avg_birthyear, inplace=True)

    df_consol["COUNT_ORDERS_LAST_7D"].fillna(0, inplace=True)
    df_consol["COUNT_ORDERS_LAST_30D"].fillna(0, inplace=True)
    df_consol["COUNT_ORDERS_CANCELED_LAST_7D"].fillna(0, inplace=True)
    df_consol["COUNT_ORDERS_CANCELED_LAST_30D"].fillna(0, inplace=True)
    df_consol["GORJETA"].fillna(0, inplace=True)
    df_consol["COUNT_ORDERS_RESTAURANTES"].fillna(0, inplace=True)
    df_consol["COUNT_ORDERS_MERCADO"].fillna(0, inplace=True)
    df_consol["COUNT_ORDERS_FARMACIA"].fillna(0, inplace=True)
    df_consol["COUNT_ORDERS_EXPRESS"].fillna(0, inplace=True)
    df_consol["COUNT_ORDERS_ANTOJO"].fillna(0, inplace=True)

    # df distance
    df_distance = df_distance[~df_distance["STOREKEEPER_ID"].isna()]
    df_distance_ = df_distance.groupby(["STOREKEEPER_ID"]).mean()
    df_distance_.drop(["ORDER_ID"], axis=1, inplace=True)
    df_distance_ = df_distance_.reset_index()
    df_distance_.rename(columns={
        "STOREKEEPER_ID": "ID"
        }, inplace=True)
    df_consol = pd.merge(df_consol, df_distance_, how="left", on="ID")

    # df orders
    df_orders_ = df_orders.groupby(["STOREKEEPER_ID"]).sum()
    df_orders_ = df_orders_.reset_index()
    df_orders_.rename(columns={
        "STOREKEEPER_ID": "ID"
        }, inplace=True)
    df_consol = pd.merge(df_consol, df_orders_, how="left", on="ID")

    # df defects
    df_defects.drop(columns=["WEEK", "CITY", "LEVEL_ID", "LEVEL_NAME", "DEFECT_COMPENSATIONS", "DEFECT_ORDER"], inplace=True)
    df_defects.rename(columns={
        "STOREKEEPER_ID": "ID"
        }, inplace=True)
    df_defects_ = df_defects.groupby("ID").sum()
    df_consol = pd.merge(df_consol, df_defects_, how="left", on="ID")

    # df earnings
    df_earnings.drop(columns=["MONTH"], inplace=True)

    df_earnings.rename(columns={
        "STOREKEEPER_ID": "ID"
        }, inplace=True)

    df_earnings = df_earnings.groupby("ID").mean()
    df_consol = pd.merge(df_consol, df_earnings, how="left", on="ID")

    # df resolution time
    df_resTime = df_resTime.drop(columns=[ 'SENT_DATA', 'SENT_HOUR', 'RESPONSE_AT',
                                                            'CITY' ,'TRANSPORT_MEDIA_TYPE',
                                                            'RESPONSE_TIME', 'RESOLUTION_TIME_BUCKET'])

    df_resTime.rename(columns={
        "STOREKEEPER_ID": "ID"
        }, inplace=True)
    df_resTime = df_resTime.sort_values('RESOLUTION_TIME', ascending=False).drop_duplicates('TICKET_ID').sort_index()
    df_resTime['RESOLUTION_TIME'] = df_resTime['RESOLUTION_TIME'].div(3600)
    pd.options.display.float_format = '{:.2f}'.format
    temp_sum = df_resTime.groupby('ID').sum()
    temp_sum.rename(columns={'RESOLUTION_TIME': 'RES_TIME_TOTAL'}, inplace = True)

    temp_mean = df_resTime.groupby('ID').mean()
    temp_mean.rename(columns={'RESOLUTION_TIME': 'RES_TIME_MEAN'}, inplace = True)

    df_resTime['TOTAL_TICKETS'] = df_resTime['ID'].map(df_resTime['ID'].value_counts())
    df_resTime = df_resTime.drop(columns=['TICKET_ID', 'RESOLUTION_TIME']).drop_duplicates()

    df_resTime_ = pd.merge(df_resTime, temp_sum, on= 'ID')
    df_resTime_ = pd.merge(df_resTime_, temp_mean, on = 'ID')
    df_consol = pd.merge(df_consol, df_resTime_, how="left", on="ID")

    unfilled = df_attendance["ACCEPTANCE_RATE"].isnull().value_counts(normalize=True)
    df_attendance.rename(columns={
        "STOREKEEPER_ID": "ID"
        }, inplace=True)
    df_consol = pd.merge(df_consol, df_attendance, how="left", on="ID")

    # df product return
    df_productReturn.drop([
        'MODAL', 
        'CITY', 
        'STORE_ID', 
        'COUNT_TO_GMV', 
        'GMV', 
        'CREATED_AT', 
        'LEVEL_NAME', 
        'VERTICAL_SUB_GROUP' ], axis=1, inplace=True)

    df_productReturn.rename(columns={"ID_ENTREGADOR": "ID"}, inplace = True)

    df_temp_mean = df_productReturn.groupby(['ID']).mean().drop(['ORDER_ID'], axis= 1)
    df_temp_count = df_productReturn.groupby(['ID']).count().drop(['PRODUCT_RETURNS'], axis=1)
    df_productReturn_ = pd.merge(df_temp_mean, df_temp_count, on = 'ID').rename(columns={"ORDER_ID": "N°_PEDIDOS"})
    df_consol = pd.merge(df_consol, df_productReturn_, how="left", on="ID")

    # df incidentes
    df_incidentes.drop(columns=['ORDER_ID', 'INCIDENT_ID', 'DATE', 'DISCIPLINE_RULE_BUCKET', 'NAME'], inplace=True)
    df_incidentes = pd.get_dummies(df_incidentes, columns=['PUNISHMENT_TYPE'])
    df_incidentes = pd.get_dummies(df_incidentes, columns=['CATEGORY_RULE'])
    df_incidentes[[
        'CATEGORY_RULE_Covid', 
        'CATEGORY_RULE_Other', 
        'CATEGORY_RULE_Discipline', 
        'CATEGORY_RULE_Fraud', 
        'CATEGORY_RULE_Manual', 
        'CATEGORY_RULE_Performance', 
        'CATEGORY_RULE_Warning'
        ]].sum()
    df_incidentes.drop(columns=['CATEGORY_RULE_Covid', 'CATEGORY_RULE_Other'], inplace=True)
    df_incidentes = df_incidentes.rename(columns={
        'STOREKEEPER_ID': 'ID', 
        'PUNISHMENT_TYPE_permanent_block': 'PERMANENT_BLOCK', 
        'PUNISHMENT_TYPE_temporary_block': 'TEMPORARY_BLOCKS', 
        'PUNISHMENT_TYPE_warning': 'WARNINGS', 
        'CATEGORY_RULE_Discipline' : 'DISCIPLINE_INCIDENTS', 
        'CATEGORY_RULE_Fraud' : 'FRAUD_INCIDENTS', 
        'CATEGORY_RULE_Manual' : 'MANUAL_INCIDENTS', 
        'CATEGORY_RULE_Performance' : 'PERFORMANCE_INCIDENTS', 
        'CATEGORY_RULE_Warning' : 'WARNING_INCIDENTS'
        })
    df_incidentes = df_incidentes.groupby(by=['ID']).sum().reset_index()
    df_consol = pd.merge(df_consol, df_incidentes, how="left", on="ID")

    # df supply
    df_supply.drop(columns=['CITY', 'VEHICLE_TAG', 'DATE'], inplace=True)
    df_supply = df_supply.groupby(['STOREKEEPER_ID']).sum().reset_index()
    df_supply['ORDERS_PER_HOURS_CONNECTED'] = df_supply['ORDERS'] / df_supply['HOURS_CONNECTED']
    df_supply.drop(columns=['HOURS_CONNECTED', 'ORDERS'], inplace=True)
    df_supply.rename(columns={'STOREKEEPER_ID': 'ID'}, inplace=True)
    df_consol = pd.merge(df_consol, df_supply, how="left", on="ID")

    # df earnings
    column_index = df_consol['EARNINGS'][df_consol['EARNINGS'] > 2000].index.tolist()
    df_consol.drop(column_index, inplace=True)
    column_index = df_consol['TIPS'][df_consol['TIPS'] > 1000].index.tolist()
    df_consol.drop(column_index, inplace=True)
    df = df_consol
    df.drop(["CIDADE"], axis=1, inplace=True)

    # treat feat eng
    df["IS_ACTIVE"].replace({ False: "False"}, inplace=True)
    df["IS_ACTIVE"].replace({
        "False": 0,
        "True": 1,
        "Quasi": 2
    }, inplace=True)
    df["GENERO"].replace({'M':0, 'F':1, "O": 0}, inplace=True)

    df.drop(columns='DATA_NASCIMENTO', inplace=True)

    transport_dummies = pd.get_dummies(df['TRANSPORTE'])
    df = pd.concat([df, transport_dummies], axis=1)
    df["AUTO_ACEITE"].fillna(False, inplace=True)
    df["AUTO_ACEITE"].replace({
        True : 1,
        False : 0
    }, inplace=True) 

    df.drop("COUNT_ORDERS_LAST_7D", axis=1, inplace=True)

    df.drop("COUNT_ORDERS_LAST_30D", axis=1, inplace=True)
    df.drop("COUNT_ORDERS_CANCELED_LAST_7D", axis=1, inplace=True)
    df.drop("COUNT_ORDERS_CANCELED_LAST_30D", axis=1, inplace=True)
    df.drop(["PRIMEIRO_PEDIDO", "ULTIMO_PEDIDO"], axis=1, inplace=True)

    df.drop("COUNT_ORDERS_RESTAURANTES", axis=1, inplace=True)

    df.drop("COUNT_ORDERS_MERCADO", axis=1, inplace=True)

    df.drop("COUNT_ORDERS_FARMACIA", axis=1, inplace=True)

    df.drop("COUNT_ORDERS_EXPRESS", axis=1, inplace=True)

    df.drop("COUNT_ORDERS_ECOMMERCE", axis=1, inplace=True)

    df.drop("COUNT_ORDERS_ANTOJO", axis=1, inplace=True)
    df["FRETE_MEDIO"].fillna((df['FRETE_MEDIO'].mean()), inplace=True)
    df["COOKING_TIME_MEDIO"].fillna((df['COOKING_TIME_MEDIO'].mean()), inplace=True)
    df["ITENS_MEDIO"].fillna((df['ITENS_MEDIO'].mean()), inplace=True)
    df["ORDERS_DONE"].fillna((df['ORDERS_DONE'].mean()), inplace = True)
    df["ORDERS_CANCEL"].fillna((df['ORDERS_CANCEL'].mean()), inplace = True)
    df["CANCELS_OPS_RT"].fillna(value = 0, inplace = True)
    df["EARNINGS"].fillna(value = 0, inplace = True)
    df["TIPS"].fillna(value = 0, inplace = True)
    df['total_earnings'] = df["EARNINGS"] + df["TIPS"]
    df.drop(["EARNINGS", "TIPS"], axis=1, inplace=True)
    df["DISTANCE_TO_USER"].fillna(value = 0, inplace = True)
    df["PUNISHMENT_MINUTES"].fillna(value = 0, inplace = True)
    df["TEMPORARY_BLOCKS"].fillna(value = 0, inplace = True)
    df["WARNINGS"].fillna(value = 0, inplace = True)

    max = df["DISCIPLINE_INCIDENTS"].describe()[7]

    df['DISCIPLINE_INCIDENTS_treated'] = df.apply(lambda row: discipline_incidents_buckets(row), axis=1)

    max = df["FRAUD_INCIDENTS"].describe()[7]

    df['FRAUD_INCIDENTS_treated'] = df.apply(lambda row: fraud_incidents_buckets(row), axis=1)
    df.drop(["DISCIPLINE_INCIDENTS", "FRAUD_INCIDENTS", "MANUAL_INCIDENTS", "PERFORMANCE_INCIDENTS", "WARNING_INCIDENTS"], axis=1, inplace=True)

    df['total_incidents'] = df["DISCIPLINE_INCIDENTS_treated"] + df["FRAUD_INCIDENTS_treated"]
    incidents_dummies = pd.get_dummies(df["total_incidents"])
    df = pd.concat([df, incidents_dummies], 1)
    df.rename(columns={
        0: "incidents_na",
        1: "incidents_1",
        2: "incidents_2",
        3: "incidents_3",
        4: "incidents_4",
        5: "incidents_5",
        6: "incidents_6",
        7: "incidents_7",
        8: "incidents_8",
        9: "incidents_9",
        10: "incidents_10"
        }, inplace=True)
    df.drop(["DISCIPLINE_INCIDENTS_treated", "FRAUD_INCIDENTS_treated", "total_incidents"], axis=1, inplace=True)
    df["ACCEPTANCE_RATE"].fillna((df.ACCEPTANCE_RATE.mean()), inplace = True)

    df.drop(df[df.ORDERS_PER_HOURS_CONNECTED > 3].index, inplace=True)
    df["ORDERS_PER_HOURS_CONNECTED"].fillna((df.ORDERS_PER_HOURS_CONNECTED.mean()), inplace = True)

    df.drop(columns=['TOTAL_TICKETS', 'ORDERS', 'ORDERS_DONE', 'ORDERS_CANCEL', 'COMPENSATIONS', 
                    'GMV_TOTAL', 'GORJETA', 'RES_TIME_TOTAL', 'RES_TIME_MEAN', 'PERMANENT_BLOCK', 
                    'PRODUCT_RETURNS', 'N°_PEDIDOS', 'LEVEL_NAME', 'GENERO', 'DATA_NASCIMENTO',
                    'FRETE_MEDIO', 'TRANSPORTE'], inplace=True)
    
    return df