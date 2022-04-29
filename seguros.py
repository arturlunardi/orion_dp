import pandas as pd
import streamlit as st
import tabula
import re


def get_df_boleto_seguro(tipo_seguro: str, seguradora: str, pdf_seguro: list):
    if tipo_seguro == "Incêndio" and seguradora == "Alfa":
        # criando uma lista pra armazenar cada dataframe de cada documento, ou seja, comercial e residencial
        list_of_dfs = []
        # para cada pdf anexado no streamlit
        for position, file in enumerate(pdf_seguro, 1):
            # leia o pdf pelo tabula, isso retorna uma lista
            tabula_list = tabula.read_pdf(file, pages='all', stream=False, pandas_options={'header': None})
            # como é uma lista, cada dataframe está localizado em uma posição, no caso do incêndio da alfa, é de 2 em 2
            tabula_df = pd.DataFrame()
            for df_ in list(map(tabula_list.__getitem__, [i for i in range(0, len(tabula_list), 2)])):
                df_.columns = ['Número', 'Inquilino', 'CPF_Inquilino', 'Endereço', 'Referência','Valor']
                tabula_df = tabula_df.append(df_)
            tabula_df.reset_index(drop=True, inplace=True)
            # aqui estamos dropando os NA pq ele buga na hora de ler e dropa tb uma linha que ele lê como header
            tabula_df = tabula_df.dropna()
            tabula_df = tabula_df.loc[tabula_df['Número'] != 'Certificado']
            list_of_dfs.append(tabula_df)
            st.warning(f'Confira se o valor do {position}º boleto é de R$ {round(tabula_df["Valor"].apply(lambda x: x.replace(".", "").replace(",", ".")).astype(float).sum(), 2)}!')
        df = pd.concat(list_of_dfs)
        df = df.reset_index(drop=True)
        st.warning(f'Confira se a soma dos boletos é de R$ {round(df["Valor"].apply(lambda x: x.replace(".", "").replace(",", ".")).astype(float).sum(), 2)}!')

    elif tipo_seguro == "Fiança":
        # como o fiança é somente um boleto, pegarei apenas o primeiro da lista
        file = pdf_seguro[0]
        if seguradora == "Liberty":
            df = tabula.read_pdf(file, pages='all', area=[100,0,200,800])
            df = df[0].dropna()
            df["CPF / CNPJ Produto"] = df["CPF / CNPJ Produto"].apply(lambda x: re.sub("[^0-9]", "", x))
            df.rename(columns={"CPF / CNPJ Produto": "CPF_Inquilino", "Segurado": "Inquilino", "Parc.": "Parcela", "Val": "Valor"}, inplace=True)
        elif seguradora == "Pottencial":
            dfs_tabula_read = tabula.read_pdf(file, pages='all', stream=False, lattice=True)
            dfs_pottencial = []
            for dfs in dfs_tabula_read:
                # pegando apenas as 8 primeiras colunas
                df_ = dfs.iloc[:, 1:7].copy()
                df_.columns = ['Proprietario', 'CPF_Proprietario', 'Inquilino', 'CPF_Inquilino', 'Parcela', 'Valor']
                dfs_pottencial.append(df_)
            df = pd.concat([i for i in dfs_pottencial])
            # aqui estou excluindo a ultima linha pq pega o valor total, checar sempre
            df = df[:-1]
        elif seguradora == "Tokio":
            df = tabula.read_pdf(file, pages='all')
            df = df[0]
            df.rename(columns={"Segurado": "Proprietario", "Locatário": "Inquilino", "Prêmio": "Valor"}, inplace=True)
        elif seguradora == "Porto Seguro":
            df = tabula.read_pdf(file, pages='1', pandas_options={'header': None})
            df = df[2]
            df.columns = ['Apolice', 'Parcela', 'Proposta', 'Valor', 'Vigencia', 'CPF_Proprietario', 'Proprietario']
        if df["Valor"].dtypes == "O":
            df["Valor"] = df["Valor"].apply(lambda x: x.replace(".", "").replace(",", ".").replace("R$", "").replace(" ", "")).astype(float)
        else:
            df["Valor"] = df["Valor"].astype(float)

        st.warning(f'Confira se o valor do boleto abaixo é de R$ {round(df["Valor"].sum(), 2)}!')

    for column in ["CPF_Inquilino", "CPF_Proprietario"]:
        if column in df.columns:
            df[column] = df[column].apply(lambda x: re.sub("[^0-9]", "", x))

    return df