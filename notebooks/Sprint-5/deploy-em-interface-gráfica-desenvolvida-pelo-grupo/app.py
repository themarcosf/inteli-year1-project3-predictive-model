import streamlit as st
import pandas as pd
from is_churn import is_churn


earnings_file = st.file_uploader("Earnings CSV")
if earnings_file != None:
    earnings = pd.read_csv(earnings_file)

supply_file = st.file_uploader("Supply CSV")
if supply_file != None:
    supply = pd.read_csv(supply_file)

infos_gerais_file = st.file_uploader("Infos Gerais CSV")
if infos_gerais_file != None:
    infos_gerais = pd.read_csv(infos_gerais_file)

attendance_rate_file = st.file_uploader("Attendance Rate CSV")
if attendance_rate_file != None:
    attendance_rate = pd.read_csv(attendance_rate_file)

incidentes_regras_rt_file = st.file_uploader("Incidentes Regras RT CSV")
if incidentes_regras_rt_file != None:
    incidentes_regras_rt = pd.read_csv(incidentes_regras_rt_file)

orders_done_cancel_file = st.file_uploader("Orders Done and Cancel CSV")
if orders_done_cancel_file != None:
    orders_done_cancel = pd.read_csv(orders_done_cancel_file)

product_return_file = st.file_uploader("Product Return CSV")
if product_return_file != None:
    product_return = pd.read_csv(product_return_file)

comp_defects_file = st.file_uploader("Comp Defects CSV")
if comp_defects_file != None:
    comp_defects = pd.read_csv(comp_defects_file)

contas_churn_file = st.file_uploader("Criação Contas Churn CSV")
if contas_churn_file != None:
    contas_churn = pd.read_csv(contas_churn_file)

tempo_resolucao_modal_file = st.file_uploader("Tempo de Resolução Modal CSV")
if tempo_resolucao_modal_file != None:
    tempo_resolucao_modal = pd.read_csv(tempo_resolucao_modal_file)

distance_user_file = st.file_uploader("Distance User CSV")
if distance_user_file != None:
    distance_user = pd.read_csv(distance_user_file)

def show_results():

    if earnings and supply and infos_gerais and attendance_rate and incidentes_regras_rt and orders_done_cancel and product_return and comp_defects and contas_churn and tempo_resolucao_modal and distance_user:
        result = is_churn(earnings, supply, infos_gerais, attendance_rate, 
                            incidentes_regras_rt, orders_done_cancel, product_return, 
                            comp_defects, contas_churn, tempo_resolucao_modal, distance_user)
    
    
    
    return result

    


