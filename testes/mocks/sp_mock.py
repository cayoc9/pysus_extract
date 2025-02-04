import pandas as pd

def generate_sp_mock_data():
    return pd.DataFrame({
        'SP_GESTOR': ['350000', '270430'],  # Valores mais frequentes do resumo
        'SP_UF': ['35', '27'],  # UF compatível com gestores
        'SP_AA': [2024, 2024],  # Ano da competência
        'SP_MM': ['03', '03'],  # Mês formatado com 2 dígitos
        'SP_NAIH': ['2298031', '3343715'],  # CNES da lista fornecido
        'SP_PROCREA': ['0303010037', '0407020284'],  # Formato 10 dígitos com zeros
        'SP_DTINTER': ['20240304', '20240301'],  # Datas no formato AAAAMMDD
        'SP_DTSAIDA': ['20240306', '20240308'],  # Datas coerentes com intervalo
        'SP_NUM_PR': ['        ', '        '],  # 8 espaços vazios
        'SP_TIPO': ['  ', '  '],  # 2 espaços vazios
        'SP_ATOPROF': ['00000002298031', '0202020380'],  # 14 dígitos com zeros
        'SP_TP_ATO': ['  ', '  '],  # 2 espaços vazios
        'SP_QTD_ATO': [3, 1],  # Valores numéricos inteiros
        'SP_PTSP': [0, 0],  # Valores numéricos inteiros
        'SP_VALATO': [0.00, 1270.00],  # Valores decimais formatados
        'SP_M_HOSP': [330490, 330330],  # Códigos de município
        'SP_M_PAC': [0, 1],  # Valores binários conforme análise
        'SP_DES_HOS': ['0', '1'],  # Valores categóricos 0/1
        'SP_DES_PAC': ['      ', '      '],  # 6 espaços vazios
        'SP_COMPLEX': ['02', '06'],  # Códigos de complexidade
        'SP_FINANC': ['000000000000000', '00000006665454'],  # 15 dígitos com zeros
        'IN_TP_VAL': [1, 1],  # Valor mais frequente do resumo
        'SERV_CLA': ['000000', '126005'],  # Códigos válidos de 6 dígitos
        'SP_CIDPRI': ['O800', 'I841'],  # Códigos CID-10 válidos
        'SP_CIDSEC': ['0000', '0000'],  # Valor único conforme análise
        'SP_QT_PROC': [1, 3],  # Valores inteiros compatíveis
        'SP_U_AIH': ['0', '1']  # Valores binários
    })