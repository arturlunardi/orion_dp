import pandas as pd
from datetime import datetime
import requests
import json
from get_raw_data import conectar_sami
import streamlit as st


def map_fields_bd(bd_field, df_field, dataset, connect):
    """
    Função para listar os produtos
    """
    df_copy = dataset.copy()
    con = connect
    cursor = con.cursor()
    cursor.execute(f'SELECT * FROM {bd_field}')
    produtos = cursor.fetchall()

    origens = df_copy[df_field].unique()
    map_fields = {}
    if len(produtos) > 0:
        for origem in origens:
            for produto in produtos:
                if origem in produto:
                    map_fields[origem] = produto[0]
                    break
        df_copy[df_field] = df_copy[df_field].replace(map_fields)
    else:
        print('Não existem produtos cadastrados.')
    con.close()

    return df_copy


def duplicate_imovel_sami():
    imovel_padrao = pd.read_sql_query(st.secrets["query_sami_imovel_duplicar"], conectar_sami())
    imovel_padrao = imovel_padrao.drop(columns=['Id_CadImov'])

    for column in imovel_padrao.columns:
        imovel_padrao[column] = imovel_padrao[column].astype(str)

    set_insert = tuple(imovel_padrao.iloc[0].index)
    values_insert = tuple(["%s" for i in range(len(imovel_padrao.iloc[0].index))])

    sql_insert = st.secrets["sql_insert"].replace("set_insert", str(set_insert)).replace("values_insert", str(values_insert)).replace("'", "")

    con = conectar_sami()
    cursor = con.cursor()

    for i in range(0, imovel_padrao.shape[0]):
        cursor.execute(sql_insert, tuple(imovel_padrao.iloc[i].values))
        cod_imovel_sami = cursor.lastrowid
        print(cursor.lastrowid, " record inserted.")

    con.commit()
    con.close()

    return cod_imovel_sami


def edit_imovel_sami(cod_imovel_vista, cod_imovel_sami):
    headers = {
        'accept': 'application/json'
        }
    url = f'http://brasaolt-rest.vistahost.com.br/imoveis/detalhes?key={st.secrets["api_vista_key"]}&showInternal=1&showSuspended=1&imovel={cod_imovel_vista}&pesquisa={{"fields":["Codigo", "CEP", "TipoEndereco", "Endereco", "Numero", "Complemento", "GaragemNumeroBox", "ValorCondominio", "ValorIptu", "Bairro", "Cidade", "Dormitorios", "Suites", "BanheiroSocialQtd", "Vagas", "AreaPrivativa", "AreaTotal", "Categoria", "Caracteristicas", "Latitude", "Longitude", "DataCadastro"]}}'
    response = requests.get(url, headers = headers)

    imovel_vista = pd.DataFrame([json.loads(response.content)])

    imovel_vista['CEP'] = imovel_vista['CEP'].apply(lambda x: x.replace('-', ''))
    imovel_vista['TipoEndereco'] = imovel_vista['TipoEndereco'].apply(lambda x: x.upper())
    imovel_vista['Endereco'] = imovel_vista['Endereco'].apply(lambda x: x.upper())
    imovel_vista['ValorCondominio'] = imovel_vista['ValorCondominio'].apply(lambda x: int(float(x) * 100) if x != '' else 0)
    imovel_vista['ValorIptu'] = imovel_vista['ValorIptu'].apply(lambda x: int(float(x) * 100) if x != '' else 0)
    imovel_vista["AreaComum"] = imovel_vista['AreaTotal'].astype(float) - imovel_vista["AreaPrivativa"].astype(float)
    imovel_vista["AreaComum"] = imovel_vista["AreaComum"].apply(lambda x: format(x, '.2f'))
    imovel_vista["Mobiliado"] = imovel_vista['Caracteristicas'].apply(lambda x: 1 if x['Mobiliado'] == 'Sim' else 2 if x['Semi Mobiliado'] == 'Sim' else 0)
    imovel_vista["Cidade"] = imovel_vista["Cidade"].apply(lambda x: x.upper())
    imovel_vista["CodigoSami"] = cod_imovel_sami
    imovel_vista["DataHoje"] = datetime.now().strftime("%Y-%m-%d")

    bairros = st.secrets["bairros_to_change"]
    tipologias = st.secrets["tipologias_to_change"]
    cidade = st.secrets["cidade_to_change"]

    fields = bairros, tipologias, cidade
    for field in fields:
        imovel_vista = map_fields_bd(field.get('bd_field'), field.get('df_field'), imovel_vista, connect=conectar_sami())

    columns_to_0 = ['Dormitorios', 'Suites', 'BanheiroSocialQtd', "AreaPrivativa", "AreaComum"]
    for column in columns_to_0:
        imovel_vista[column] = imovel_vista[column].apply(lambda x: 0 if x == '' else x)

    st.warning('Por favor, confirme os dados do imóvel antes de alterá-lo.')

    st.write(imovel_vista)

    confirmou_dados_imovel = st.checkbox(label='Confirmar e alterar o imóvel', value=False)

    if confirmou_dados_imovel:
        numerical_columns = ["Bairro", "Categoria", "Cidade"]
        try:
            for column in numerical_columns:
                if not str(imovel_vista.iloc[0][column]).isdigit():
                    raise Exception(f"A coluna {column} não é numérica, verifique os dados.")
        except Exception as error:
            print(error)

        for column in imovel_vista.columns:
            imovel_vista[column] = imovel_vista[column].astype(str)

        columns_to_edit = st.secrets["columns_to_edit_sami_vista"]

        sql_update_set = [f'{columns_to_edit.get(column)} = %s' for column in columns_to_edit if column != "CodigoSami"]

        sql_update = st.secrets["sql_update"].replace("sql_update_set", str(sql_update_set)).replace("'", "").replace("[", "").replace("]", "")

        con = conectar_sami()
        cursor = con.cursor()

        cursor.execute(sql_update, tuple(imovel_vista[columns_to_edit.keys()].iloc[0].values))

        con.commit()
        con.close()

        st.success('Editado com sucesso.')