import pandas as pd
import get_raw_data
import geocoder
import numpy as np
import datetime
from dateutil.relativedelta import relativedelta


def get_semestral_dates(start_last_month=True):
    """
    return a dict with the dates from the imediate previous and the second previous trimester
    start_last_month: if True, the final date it's the last day of the imediate previous month
    monthly_report: if True, only get last month, not trimesters
    """
    if start_last_month:
        now = datetime.datetime.now()

        final_date = datetime.date(
            now.year, now.month, 1) + relativedelta(days=-1)

        begin_date = final_date + \
            relativedelta(months=-5)
        begin_date = datetime.date(
            begin_date.year, begin_date.month, 1)

    else:
        # aqui nao tem nada em relação ao monthly report, avaliar se vale a pena
        final_date = datetime.datetime.now()
        begin_date = final_date + relativedelta(months=-5)

    return {
        'final_date': pd.to_datetime(final_date),
        'begin_date': pd.to_datetime(begin_date),
    }


def do_geocoder(x):
    try:
        print('add')
        return geocoder.arcgis(x, readtimeout=15).latlng
    except:
        print('errou \n')
        print(x)


def get_dataset_for_alugados():
    df_sami_not_in_orion = get_raw_data.get_imoveis_sami()
    df_orion_for_alugados = get_raw_data.get_distinct_orion()
    df_sami = get_raw_data.get_distinct_sami()

    columns_to_lambda = [i for i in df_sami_not_in_orion.columns.tolist() if i not in ['cod_imov_sami', 'comodidades']]
    dict_to_agg = {i: lambda x: max(x) for i in columns_to_lambda}
    dict_to_agg['comodidades'] = lambda x: ', '.join(x) if x.any() else float("NaN")

    df_sami_not_in_orion['DataOcupacaoCont_CadCont'] = pd.to_datetime(df_sami_not_in_orion['DataOcupacaoCont_CadCont'])
    columns_to_lambda = [i for i in df_sami_not_in_orion.columns.tolist() if i not in ['cod_imov_sami', 'comodidades']]
    df_sami_not_in_orion = df_sami_not_in_orion.groupby('cod_imov_sami').agg(dict_to_agg).reset_index()

    df_sami_not_in_orion['sacada'] = df_sami_not_in_orion['comodidades'].apply(lambda x: 'Sim' if isinstance(x, str) and 'Sacada' in x else 'Não')
    df_sami_not_in_orion['churrasqueira'] = df_sami_not_in_orion['comodidades'].apply(lambda x: 'Sim' if isinstance(x, str) and 'Churrasqueira' in x else 'Não')
    df_sami_not_in_orion['valor_condominio'] /= 100
    df_sami_not_in_orion['valor_iptu_mensal'] /= 100
    df_sami_not_in_orion['mobiliado'] = df_sami_not_in_orion['mobiliado'].apply(lambda x: 'Sim' if x == 1 else 'Semi' if x == 2 else 'Não')
    df_sami_not_in_orion['finalidade'] = df_sami_not_in_orion['finalidade'].apply(lambda x: 'Residencial' if x == 'R' else 'Comercial' if x == 'C' else 'Box')

    if df_sami_not_in_orion[df_sami_not_in_orion['latitude'] == 0].shape[0] > 0:
        print('passou')
        import swifter
        df_sami_not_in_orion['coordinates'] = df_sami_not_in_orion.apply(lambda x: [x['latitude'], x['longitude']] if x['latitude'] != 0 else do_geocoder(x['endereco']), axis=1)
        df_sami_not_in_orion['latitude'] = df_sami_not_in_orion['coordinates'].apply(lambda x: x[0] if type(x)==list or type(x)==tuple else eval(x)[0])
        df_sami_not_in_orion['longitude'] = df_sami_not_in_orion['coordinates'].apply(lambda x: x[1] if type(x)==list or type(x)==tuple else eval(x)[1])
        df_sami_not_in_orion.drop(columns=['coordinates'], inplace=True)

        con = get_raw_data.conectar_sami()
        cursor = con.cursor()

        sql_update = "UPDATE loc_cadimov SET CoordenadaX_CadImov = %s, CoordenadaY_CadImov = %s WHERE CodImovel_CadImov = %s"

        for index, row in df_sami_not_in_orion.iterrows():
            cursor.execute(sql_update, tuple(row[['latitude', 'longitude', 'cod_imov_sami']].values))

        con.commit()
        con.close()

    df_orion_for_alugados = df_orion_for_alugados.drop_duplicates(subset='codigo_sistema', keep='last')
    df_orion_for_alugados['sacada'] = df_orion_for_alugados['comodidades'].apply(lambda x: 'Sim' if isinstance(x, str) and 'Sacada' in x else 'Não')
    df_orion_for_alugados['churrasqueira'] = df_orion_for_alugados['comodidades'].apply(lambda x: 'Sim' if isinstance(x, str) and 'Churrasqueira' in x else 'Não')
    df_sami['DataOcupacaoCont_CadCont'] = pd.to_datetime(df_sami['DataOcupacaoCont_CadCont'])
    df_sami['NumContratoInterno_CadCont'] = df_sami['NumContratoInterno_CadCont'].replace({'': 0, 'ID': ''}, regex=True).astype(int)
    # df_sami_filtered = df_sami.loc[df_sami['DataOcupacaoCont_CadCont'].between('2021-05-01', '2021-10-31')]

    dates = get_semestral_dates(start_last_month=True)
    df_sami_filtered = df_sami.loc[df_sami['DataOcupacaoCont_CadCont'].between(dates['begin_date'], dates['final_date'])]

    df_merged_alugados = df_sami_filtered.merge(df_orion_for_alugados, left_on='NumContratoInterno_CadCont', right_on='codigo_sistema', how='left')

    columns = ['finalidade', 'tipologia', 'bairro', 'valor_aluguel', 'valor_condominio', 'valor_iptu_mensal', 'dormitorios', 
    'suites', 'banheiros', 'vagas_garagem', 'area_privativa', 'mobiliado', 'latitude', 'longitude', 'sacada', 'churrasqueira']
    for index, row in df_merged_alugados.loc[df_merged_alugados['codigo_sistema'].isnull()].iterrows():
        for column in columns:
            df_merged_alugados.loc[index, column] = df_sami_not_in_orion.loc[df_sami_not_in_orion['cod_contrato'] == row['CodContrato_CadCont']][column].squeeze()

    df_merged_alugados.replace({0: float("NaN"), '0': float("NaN")}, inplace=True)
    df_merged_alugados.dropna(subset=['area_privativa'], inplace=True)

    isencao_condominio = ['Casa', 'Box', 'Pavilhão', 'Terreno', 'Chácara', 'Outros', 'Galpão', 'Prédio', 'Lote', 'Casa Comercial']
    # onde o valor do condominio é menor que e se não está dentro dos imóveis em que a tipologia é isenta de condominio, atribua nulo para fazer o fillna
    df_merged_alugados.loc[df_merged_alugados['tipologia'].isin(isencao_condominio), 'valor_condominio'] = 0

    # estamos transformando em 0 todos os kitnets e imóveis comerciais que não são casas ou casas comerciais
    df_merged_alugados.loc[(df_merged_alugados['tipologia'] == 'Kitnet') | (df_merged_alugados['finalidade'] == 'Comercial') & (df_merged_alugados['tipologia'] != 'Casa Comercial') & (df_merged_alugados['tipologia'] != 'Casa'), 'dormitorios'] = 0

    columns = ['area_privativa', 'finalidade', 'bairro', 'banheiros', 'dormitorios', 'latitude', 'longitude', 'mobiliado', 'suites', 'tipologia', 'vagas_garagem', 'valor_condominio', 'valor_iptu_mensal', 'sacada', 'churrasqueira', 'valor_aluguel', 'alugado']

    df_merged_alugados['alugado'] = 1

    return df_merged_alugados[columns]


def get_dataset_for_scraper():
    df_scraper = get_raw_data.get_distinct_scraper(last_monday=True)
    df_orion_for_scraper = get_raw_data.get_distinct_orion(last_monday=True)

    df_orion_for_scraper.insert(1, 'imobiliaria', 'Órion')

    df_merged_scraper = pd.concat([df_scraper, df_orion_for_scraper])

    df_merged_scraper['mobiliado'] = df_merged_scraper['mobiliado'].apply(lambda x: 'Não' if x not in ['Sim', 'Semi'] else x)

    df_merged_scraper.replace({0: float("NaN"), '0': float("NaN")}, inplace=True)

    df_merged_scraper['banheiros'].fillna(1, inplace=True)
    df_merged_scraper['dormitorios'].fillna(0, inplace=True)
    df_merged_scraper = df_merged_scraper.loc[df_merged_scraper['valor_aluguel'] < 20000]
    # df_merged_scraper = df_merged_scraper.loc[(df_merged_scraper['valor_condominio'] > 5) | (df_merged_scraper['valor_condominio'].isnull())]
    # df_merged_scraper = df_merged_scraper.loc[(df_merged_scraper['valor_iptu_mensal'] <= 300) | (df_merged_scraper['valor_iptu_mensal'].isnull())]
    df_merged_scraper = df_merged_scraper.loc[(df_merged_scraper['longitude'] > -54.5) & (df_merged_scraper['longitude'] < -53)]
    df_merged_scraper = df_merged_scraper.loc[(df_merged_scraper['latitude'] > -29.78) & (df_merged_scraper['latitude'] < -28)]

    df_merged_scraper['sacada'] = 'Não'
    df_merged_scraper['churrasqueira'] = 'Não'

    searchfor = ['Sacada', 'sacada', 'BALCONY', 'balcony', 'Sacadas', 'sacadas', 'Sacad', 'sacad']

    df_merged_scraper.loc[df_merged_scraper['comodidades'].fillna('').str.contains('|'.join(searchfor)), 'sacada'] = 'Sim'

    searchfor = ['Churrasqueira', 'churrasqueira', 'BARBECUE_GRILL', 'churras', 'Churras']
    df_merged_scraper.loc[df_merged_scraper['comodidades'].fillna('').str.contains('|'.join(searchfor)), 'churrasqueira'] = 'Sim'

    df_merged_scraper.drop(columns=['comodidades'], inplace=True)

    isencao_condominio = ['Casa', 'Box', 'Pavilhão', 'Terreno', 'Chácara', 'Outros', 'Galpão', 'Prédio', 'Lote', 'Casa Comercial']
    # onde o valor do condominio é menor que e se não está dentro dos imóveis em que a tipologia é isenta de condominio, atribua nulo para fazer o fillna
    df_merged_scraper.loc[df_merged_scraper['tipologia'].isin(isencao_condominio), 'valor_condominio'] = 0
    df_merged_scraper.loc[(df_merged_scraper['valor_condominio'] < 50) & ~(df_merged_scraper['tipologia'].isin(isencao_condominio)), 'valor_condominio'] = np.nan

    # df_merged_scraper.loc[(df_merged_scraper['valor_condominio'] < 50), 'valor_condominio'] = np.nan
    df_merged_scraper.loc[(df_merged_scraper['valor_iptu_mensal'] < 8.4) | (df_merged_scraper['valor_iptu_mensal'] >= 300), 'valor_iptu_mensal'] = np.nan

    # estamos transformando em 0 todos os kitnets e imóveis comerciais que não são casas ou casas comerciais
    df_merged_scraper.loc[(df_merged_scraper['tipologia'] == 'Kitnet') | (df_merged_scraper['finalidade'] == 'Comercial') & (df_merged_scraper['tipologia'] != 'Casa Comercial') & (df_merged_scraper['tipologia'] != 'Casa'), 'dormitorios'] = 0

    df_merged_scraper['alugado'] = 0

    columns = ['link', 'imobiliaria', 'area_privativa', 'finalidade', 'bairro', 'banheiros', 'dormitorios', 'latitude', 'longitude', 'mobiliado', 'suites', 'tipologia', 'vagas_garagem', 'valor_condominio', 'valor_iptu_mensal', 'sacada', 'churrasqueira', 'valor_aluguel', 'alugado']

    return df_merged_scraper[columns]

