import mysql.connector
import pandas as pd
import streamlit as st
import datetime
import requests
import json
from dateutil.relativedelta import relativedelta
import calendar


def conectar():
    """
    Função para conectar ao servidor
    """
    try:
        con = mysql.connector.connect(
            **st.secrets["mysql_scrap"],
        )
        return con
    except mysql.connector.Error as e:
        print(f'Erro na conexão ao MySQL Server: {e}')


def conectar_orion():
    """
    Função para conectar ao servidor
    """
    try:
        con = mysql.connector.connect(
            **st.secrets["mysql_orion"],
        )
        return con
    except mysql.connector.Error as e:
        print(f'Erro na conexão ao MySQL Server: {e}')


def conectar_sami():
    """
    Função para conectar ao SamiDB.
    """
    try:
        con = mysql.connector.connect(
            **st.secrets["mysql_sami"],
        )
        return con
    except mysql.connector.Error as e:
        print(f'Erro na conexão ao MySQL Server: {e}')


def get_distinct_scraper(last_monday=False):
    if last_monday:
        today = datetime.date.today()
        last_monday_date = today + datetime.timedelta(days=-today.weekday(), weeks=0)
        last_monday_date = last_monday_date.strftime('%Y-%m-%d')

        data = pd.read_sql_query(f"""
        {st.secrets['get_distinct_scraper']['last_monday_true'].replace('last_monday_date', last_monday_date)}""",
        conectar())
    else:
        data = pd.read_sql_query(f"""
        {st.secrets['get_distinct_scraper']['last_monday_false']}""",
        conectar())
    return data


def get_distinct_orion(last_monday=False):
    if last_monday:
        today = datetime.date.today()
        last_monday_date = today + datetime.timedelta(days=-today.weekday(), weeks=0)
        last_monday_date = last_monday_date.strftime('%Y-%m-%d')

        data = pd.read_sql_query(f"""
        {st.secrets['get_distinct_orion']['last_monday_true'].replace('last_monday_date', last_monday_date)}""", 
        conectar_orion())

    else:
        data = pd.read_sql_query(f"""
        {st.secrets['get_distinct_orion']['last_monday_false']}""", 
        conectar_orion())
    return data


def get_distinct_sami():
    data = pd.read_sql_query(f"""
    {st.secrets['get_distinct_sami']}""",
    conectar_sami())
    return data


def get_imoveis_sami():
    data = pd.read_sql_query(f"""{st.secrets['get_imoveis_sami']}""",
    conectar_sami())
    return data


def get_comissao_corretores(compacto, data_inicio, data_termino):
    import sys
    if compacto:
        data = pd.read_sql_query(f"""
        {st.secrets['get_comissao_corretores_locacao']['sami_bd_groupby_true'].replace('begin_comissao_locacao_date', data_inicio).replace('final_comissao_locacao_date', data_termino)}""",
        conectar_sami())
    else:
        data = pd.read_sql_query(f"""
        {st.secrets['get_comissao_corretores_locacao']['sami_bd_groupby_false'].replace('begin_comissao_locacao_date', data_inicio).replace('final_comissao_locacao_date', data_termino)}""",
        conectar_sami())
        print(data)
        print({st.secrets['get_comissao_corretores_locacao']['sami_bd_groupby_false'].replace('begin_comissao_locacao_date', data_inicio).replace('final_comissao_locacao_date', data_termino)})
        sys.stdout.flush()
    return data    


def get_vista_api(data_inicio: str, data_termino: str, agenciadores_vista: list) -> pd.DataFrame:
    """
    Access the vista api and return the original dataframe.

    Args:
        data_inicio (str): string em formato YYYY-MM-DD
        data_termino (str): string em formato YYYY-MM-DD
        agenciadores_vista (list): lista de código de agenciadores do vista. Lembrar em enviar formato json, json.dumps(lista)

    Returns:
        pd.DataFrame: dataframe with the vista data
    """

    headers = {
        'accept': 'application/json'
    }

    dataframes = []
    for i in range(1, 99999):
        # aqui eu coloquei imóveis desocupacao Nao pq eu n quero q ele pegue imoveis de desocupacao
        url = f'http://brasaolt-rest.vistahost.com.br/imoveis/listar?key={st.secrets["api_vista_key"]}&showInternal=1&showSuspended=1&showtotal=0&pesquisa={{"fields":["Status", "Codigo", "Endereco", "Numero", "Complemento", "Bairro", "Cidade", "Dormitorios", "Suites", "BanheiroSocialQtd", "Vagas", "ValorLocacao", "ValorVenda", "DataCadastro", "AreaPrivativa", "Categoria", "Finalidade", "Agenciador", "Latitude", "Longitude", "ImoveisDesocupacao", "CaptacaoPassiva"], "order": {{"Codigo": "asc"}}, "filter": {{"DataCadastro": [{data_inicio}, {data_termino}], "CodigoCorretor": {agenciadores_vista}, "ImoveisDesocupacao": ["Nao"]}}, "paginacao":{{"pagina":{i},"quantidade":50}}}}'
        response = requests.get(url, headers = headers)
        if response.status_code == 500:
            break
        elif response.status_code == 400:
            response.raise_for_status()
        dataframes.append(json.loads(response.content))

    datasets = []
    for item in dataframes:
        df = pd.DataFrame(item).T
        datasets.append(df)

    df_original = pd.concat(item for item in datasets)

    # Empreendimento não é imóvel
    df_original = df_original.loc[df_original['Categoria'] != 'Empreendimento']

    return df_original


def get_vista_api_historicos(dataframe, type_of_status):
    df_geral = pd.DataFrame()

    for imovel in dataframe['Codigo']:
        if imovel != '':
            headers = {
            'accept': 'application/json'
            }

            url = f'http://brasaolt-rest.vistahost.com.br/imoveis/detalhes?key={st.secrets["api_vista_key"]}&showInternal=1&showSuspended=1&imovel={imovel}&pesquisa={{"fields":["Status", "Codigo", "Endereco", "Numero", "Complemento", "Bairro", "Cidade", "Dormitorios", "Vagas", "ValorLocacao", "ValorVenda", "AreaPrivativa", "Categoria", "Finalidade", "Agenciador", "Latitude", "Longitude", "DataCadastro", {{"prontuarios":["Data","Hora","Assunto","Pendente","Texto","Bairro","Anunciado","Retranca","Corretor","PROPOSTA","Status","Datainicio","ValorProposta","VeiculoPublicado","DataAnuncio","Privado","Cliente","SolicitanteChave","Statusdoimóvel","CodigoCorretor"]}}],"filter":{{"prontuarios":{{"Assunto":["like","Colocado para Locação"]}}}}}}'

            response = requests.get(url, headers=headers)
            
            if response.status_code == 200:

                if len(json.loads(response.content)['prontuarios']) > 0:
                    df_imovel = pd.DataFrame(json.loads(response.content))
                else:
                    df_imovel = pd.DataFrame([json.loads(response.content)])
                df_imovel = df_imovel.dropna(subset=['prontuarios'])
                # aqui estamos buscando em todo os históricos do imóvel os que correspondem a cada assunto, isso gera um dataframe com todos os históricos do imóvel
                # cada linha representa um historico, então existem linhas que possuem datas preenchidas e outras não, por isso temos que dropar os nulos e pegar o último
                # df_imovel['data_ultima_disponibilidade'] = df_imovel['prontuarios'].apply(lambda x: x['Data'] if x['Assunto'] == 'Colocado para locação' else None)
                if type_of_status == 'Disponibilizados apenas para Locação':
                    df_imovel['data_ultima_disponibilidade'] = df_imovel['prontuarios'].apply(lambda x: x['Data'] if type(x) == dict and x['Assunto'] == 'Colocado para locação' else None)
                elif type_of_status == 'Disponibilizados apenas para Venda':
                    df_imovel['data_ultima_disponibilidade'] = df_imovel['prontuarios'].apply(lambda x: x['Data'] if type(x) == dict and x['Assunto'] == 'Colocado para venda' else None)
                elif type_of_status == 'Disponibilizados para Locação ou Venda':
                    df_imovel['data_ultima_disponibilidade'] = df_imovel['prontuarios'].apply(lambda x: x['Data'] if type(x) == dict and x['Assunto'] == 'Colocado para locação' or type(x) == dict and x['Assunto'] == 'Colocado para venda' else None)

                df_geral = df_geral.append(df_imovel).reset_index(drop=True)

    df_geral = df_geral.drop(columns=['prontuarios', 'FinalidadeStatus', 'Prontuario_Codigo'])

    # --- aqui só pra padronizar as coisas --------
    df_geral.loc[df_geral['Categoria'] == 'Salas/Conjuntos', 'Categoria'] = 'Sala'
    df_geral.loc[df_geral['Categoria'] == 'Prédio Comercial', 'Categoria'] = 'Prédio'
    df_geral.loc[df_geral['Categoria'] == 'Ponto Comercial', 'Categoria'] = 'Ponto'
    df_geral.loc[df_geral['Categoria'] == 'Casa Em Condomínio', 'Categoria'] = 'Casa'
    df_geral.loc[df_geral['Categoria'] == 'Casa em Condomínio', 'Categoria'] = 'Casa'
    df_geral.loc[df_geral['Categoria'] == 'Terreno Comercial', 'Categoria'] = 'Terreno'
    df_geral.loc[df_geral['Categoria'].isin(['Área', 'Rural']), 'Categoria'] = 'Área Rural'

    df_geral.loc[df_geral['Bairro'] == "Passo DAreia", 'Bairro'] = "Passo D'Areia"
    df_geral.loc[df_geral['Bairro'] == "perpetuo socorro", 'Bairro'] = "Nossa Senhora do Perpétuo Socorro"
    df_geral.loc[df_geral['Bairro'] == "Medianeira", 'Bairro'] = "Nossa Senhora Medianeira"
    df_geral.loc[df_geral['Bairro'] == "Nossa Senhora Das Dores", 'Bairro'] = "Nossa Senhora das Dores"
    df_geral.loc[df_geral['Bairro'].isin(["Zona rural", "Rural"]), 'Bairro'] = "Zona Rural"

    residenciais = ['Apartamento', 'Kitnet', 'Casa', 'Cobertura', 'Flat', 'Duplex', 'Sobrado', 'Loft', 'Casa de Condomínio', 'Box', 'Triplex', 'Prédio Residencial', 'Studio', 'JK', 'Outros Residencial', 'Casa Residencial', 'JK/Kitnet', 'Casa Em Condomínio']
    comerciais = ['Empreendimento', 'Sala', 'Galpão', 'Loja', 'Prédio/Edificio', 'Prédio', 'Ponto', 'Terreno', 'Outros', 'Pavilhão', 'Pavilhao', 'Lote', 'Sala Comercial', 'Casa Comercial', 'Casa comercial', 'Prédio Comercial', 'Predio', 'Ponto Comercial', 'Conjunto Comercial', 'Ponto Comercial', 'Depósito', 'Box Comercial', 'Sala/Conjunto Comercial', 'Terreno/Lote Comercial']
    rurais = ['Area', 'Área', 'Zona rural', 'Zona Rural', 'Campo', 'Chácara', 'Sítio', 'Chácara/Fazenda/Sítio', 'Área Rural', 'Rural']
    nao_identificado = ['', 'Nenhum']

    df_geral.loc[df_geral['Categoria'].isin(residenciais), 'Finalidade'] = 'Residencial'
    df_geral.loc[df_geral['Categoria'].isin(comerciais), 'Finalidade'] = 'Comercial'
    df_geral.loc[df_geral['Categoria'].isin(rurais), 'Finalidade'] = 'Rural'
    df_geral.loc[df_geral['Categoria'].isin(nao_identificado), 'Finalidade'] = 'Não Identificado'

    # -----------------------------------------------

    # # agora pegamos e filtramos aqueles que existem data de disponibilização
    df_geral = df_geral.loc[(df_geral['data_ultima_disponibilidade'].notnull())]

    # aqui serve como um filtro para pegar os imóveis que possuem datas preenchidas
    # ele faz o groupby por codigo e pega o máximo da data, que é a última atualização do historico.
    # ex: um imóvel tem 2 datas de colocado para locação, ele pega a mais recente.
    df_geral = df_geral.groupby('Codigo', as_index=False).max()

    # finalmente filtramos aqueles onde a data de disponibilização é maior que a data de cadastro
    df_geral = df_geral.loc[(df_geral['data_ultima_disponibilidade'] >= df_geral['DataCadastro'])].drop_duplicates(subset=['Codigo'])

    # calculando quantos dias demoraram para ser disponbilizado
    df_geral['tempo_para_disponibilidade'] = (pd.to_datetime(df_geral['data_ultima_disponibilidade']) - pd.to_datetime(df_geral['DataCadastro'])).apply(lambda x: x.days)

    return df_geral


def get_detail_imovel_vista(dataframe, codigo_usuarios_gerente):
    df_geral = pd.DataFrame()

    for imovel in dataframe['cod_crm']:
        if imovel != '':
            headers = {
            'accept': 'application/json'
            }

            url = f'http://brasaolt-rest.vistahost.com.br/imoveis/detalhes?key={st.secrets["api_vista_key"]}&showInternal=1&showSuspended=1&imovel={imovel}&pesquisa={{"fields":["Status", "Codigo", "Endereco", "Numero", "Complemento", "Bairro", "Cidade", "Dormitorios", "Vagas", "ValorLocacao", "ValorVenda", "AreaPrivativa", "Categoria", "Finalidade", "Latitude", "Longitude", "DataCadastro", "ImoveisDesocupacao", "CaptacaoPassiva", {{"Corretor":["Nomecompleto", "Datacadastro", "Tipo"]}}]}}'

            response = requests.get(url, headers=headers)
            
            if response.status_code == 200:
                # print(imovel, response)

                # df_imovel = pd.DataFrame(json.loads(response.content))
                df_imovel = pd.DataFrame([json.loads(response.content)])
                df_geral = df_geral.append(df_imovel).reset_index(drop=True)

    # df_geral = df_geral.drop(columns=['FinalidadeStatus'])

    # aqui estou filtrando apenas por corretores que são captadores e dropando os nulos
    # df_geral['Corretor'] = df_geral['Corretor'].apply(lambda x: x.get('Codigo') if 'Captador' in x.get('Tipo') or 'Agenciador' in x.get('Tipo') else None)
    # df_geral = df_geral.dropna(subset=['Corretor'])
    # df_geral = df_geral.loc[~df_geral['Corretor'].isin(codigo_usuarios_gerente)] 
    
    # aqui é pra imoveis que nao tem nenhum corretor, como eu faço x.items() embaixo, ele precisa ser dict e nao list
    df_geral['Corretor'] = df_geral['Corretor'].apply(lambda x: {} if x == [] else x)
    # aqui é pra pegar todos os códigos de captadores e agenciadores do imóvel
    df_geral['Corretor'] = df_geral['Corretor'].apply(lambda x: [x.get(key).get('Codigo') for key, value in x.items() if 'Captador' in value.get('Tipo') or 'Agenciador' in value.get('Tipo')])
    # aqui é pra tirar os códigos de gerentes dos corretores
    df_geral['Corretor'] = df_geral['Corretor'].apply(lambda x: [i for i in x if i not in codigo_usuarios_gerente])
    

    # --- aqui só pra padronizar as coisas --------
    df_geral.loc[df_geral['Categoria'] == 'Salas/Conjuntos', 'Categoria'] = 'Sala'
    df_geral.loc[df_geral['Categoria'] == 'Prédio Comercial', 'Categoria'] = 'Prédio'
    df_geral.loc[df_geral['Categoria'] == 'Ponto Comercial', 'Categoria'] = 'Ponto'
    df_geral.loc[df_geral['Categoria'] == 'Casa Em Condomínio', 'Categoria'] = 'Casa'
    df_geral.loc[df_geral['Categoria'] == 'Casa em Condomínio', 'Categoria'] = 'Casa'
    df_geral.loc[df_geral['Categoria'] == 'Terreno Comercial', 'Categoria'] = 'Terreno'
    df_geral.loc[df_geral['Categoria'].isin(['Área', 'Rural']), 'Categoria'] = 'Área Rural'

    df_geral.loc[df_geral['Bairro'] == "Passo DAreia", 'Bairro'] = "Passo D'Areia"
    df_geral.loc[df_geral['Bairro'] == "perpetuo socorro", 'Bairro'] = "Nossa Senhora do Perpétuo Socorro"
    df_geral.loc[df_geral['Bairro'] == "Medianeira", 'Bairro'] = "Nossa Senhora Medianeira"
    df_geral.loc[df_geral['Bairro'] == "Nossa Senhora Das Dores", 'Bairro'] = "Nossa Senhora das Dores"
    df_geral.loc[df_geral['Bairro'].isin(["Zona rural", "Rural"]), 'Bairro'] = "Zona Rural"

    residenciais = ['Apartamento', 'Kitnet', 'Casa', 'Cobertura', 'Flat', 'Duplex', 'Sobrado', 'Loft', 'Casa de Condomínio', 'Box', 'Triplex', 'Prédio Residencial', 'Studio', 'JK', 'Outros Residencial', 'Casa Residencial', 'JK/Kitnet', 'Casa Em Condomínio']
    comerciais = ['Empreendimento', 'Sala', 'Galpão', 'Loja', 'Prédio/Edificio', 'Prédio', 'Ponto', 'Terreno', 'Outros', 'Pavilhão', 'Pavilhao', 'Lote', 'Sala Comercial', 'Casa Comercial', 'Casa comercial', 'Prédio Comercial', 'Predio', 'Ponto Comercial', 'Conjunto Comercial', 'Ponto Comercial', 'Depósito', 'Box Comercial', 'Sala/Conjunto Comercial', 'Terreno/Lote Comercial']
    rurais = ['Area', 'Área', 'Zona rural', 'Zona Rural', 'Campo', 'Chácara', 'Sítio', 'Chácara/Fazenda/Sítio', 'Área Rural', 'Rural']
    nao_identificado = ['', 'Nenhum']

    df_geral.loc[df_geral['Categoria'].isin(residenciais), 'Finalidade'] = 'Residencial'
    df_geral.loc[df_geral['Categoria'].isin(comerciais), 'Finalidade'] = 'Comercial'
    df_geral.loc[df_geral['Categoria'].isin(rurais), 'Finalidade'] = 'Rural'
    df_geral.loc[df_geral['Categoria'].isin(nao_identificado), 'Finalidade'] = 'Não Identificado'

    # -----------------------------------------------

    return df_geral


def get_df_lancamentos_sami(tipo_seguro: str, data_referencia: datetime.date, seguradora: str) -> dict:
    """
    Essa função retorna um dicionário de DataFrames. Nele teremos dataframes com dados sobre
    os lançamentos de cada imóvel de acordo com o código do evento.

    Args:
        tipo_seguro (str): Tipo de seguro, incêndio ou fiança.
        data_referencia (datetime.date): Data de referência do boleto.
        seguradora (str): Seguradora para verificar se tem CPF no dataframe.

    Returns:
        dict: Um dicionário de dataframes.
    """    

    if tipo_seguro == "Incêndio":
        cod_evento_taxa = "1058"
        tipo_pessoa = "Inquilino"
    elif tipo_seguro == "Fiança":
        cod_evento_taxa = "1096"
        if seguradora in ["Pottencial", "Liberty", "Alfa"]:
            tipo_pessoa = "Inquilino"
        elif seguradora in ["Porto Seguro", "Tokio"]:
            tipo_pessoa = "Proprietario"

    mes_referencia = data_referencia.strftime('%Y%m')

    # Basicamente serão 2 dataframes. O primeiro eu verificarei se o evento foi lançado no Sami para o determinado imóvel.
    # Caso não identifiquemos o lançamento no imóvel, eu vou identificar o CPF do locatário/proprietário e juntá-lo a algum imóvel que tenha o mesmo CPF para assim buscar os dados do imóvel.
    
    if tipo_pessoa == "Inquilino":
        # esse df me retorna todos os lançamentos de um evento específico para contratos ativos
        # ele retorna o numero do contrato, numero do imóvel e o locatário
        df_checar_lancamento = pd.read_sql_query(f"""
        {st.secrets["get_df_lancamentos_evento_sami"]["inquilino"].replace('mes_referencia', mes_referencia).replace('cod_evento_taxa', cod_evento_taxa)}
        """, conectar_sami()
        )

        df_identificar_imovel = pd.read_sql_query(f"""
        {st.secrets["get_df_identificar_imovel_sami"]["inquilino"]}
        """, conectar_sami()
        )


    elif tipo_pessoa == "Proprietario":
        # esse df me retorna todos os lançamentos de um evento específico para contratos ativos
        # ele retorna o numero do contrato, numero do imóvel e o proprietário
        # é importante frisar que ele só capta 1 proprietário por imóvel, então se houver mais de um proprietário, ele vai pegar o primeiro
        df_checar_lancamento = pd.read_sql_query(f"""
        {st.secrets["get_df_lancamentos_evento_sami"]["proprietario"].replace('mes_referencia', mes_referencia).replace('cod_evento_taxa', cod_evento_taxa)}
        """, conectar_sami()
        )

        # ja esse df me retorna TODOS os proprietários de TODOS os imóveis que já possuiram contrato, inclusive o imóvel vem duplicado se houver mais de um contrato para o mesmo imóvel
        # eu escolhi retirar o contratos ativos pq ele limita e pode n aparecer
        # depois eu posso retirar se encontrarmos erros ou ficar mt grande o df
        df_identificar_imovel = pd.read_sql_query(f"""
        {st.secrets["get_df_identificar_imovel_sami"]["proprietario"]}
        """, conectar_sami()
        )

    return {"df_checar_lancamento": df_checar_lancamento, "df_identificar_imovel": df_identificar_imovel}


def month_range(year: int, month: int) -> tuple:
    """
    Return the first and the last day of a month-year date.

    Args:
        year (int): year of date
        month (int): month of date

    Returns:
        tuple: return a tuple filled with two strings, the initial and final date of the month.
    """    
    last_day = calendar.monthrange(year, month)[1]
    return f'{year}-{month}-01', f'{year}-{month}-{last_day}'


# @st.cache(hash_funcs={"_thread.RLock": lambda _: None, 'builtins.weakref': lambda _: None}, show_spinner=False)
def raw_data_monthly(last_month: bool=False, id_cidade: bool=1, id_status: tuple=(1,3)) -> pd.DataFrame:
    """
    Return a Advertiser DataFrame for the last 12 months.

    Args:
        last_month (bool, optional): if True, start last month. Defaults to False.
        id_cidade (int, optional): id of the city. Defaults to 1.
        id_status (tuple, optional): id of the status. Defaults to (1,3).

    Returns:
        pd.DataFrame: Advertiser DataFrame for the last 12 months.
    """ 

    if last_month:
        initial_date = datetime.datetime.now() + relativedelta(months=-13)
        final_date = datetime.datetime.now() + relativedelta(months=-1)
    else:
        initial_date = datetime.datetime.now() + relativedelta(months=-12)
        final_date = datetime.datetime.now()

    data_scrap = pd.read_sql_query(st.secrets["query_raw_monthly"]["scrap"].replace('initial_date', month_range(initial_date.year, initial_date.month)[0]).replace('final_date', month_range(final_date.year, final_date.month)[1]).replace('id_da_cidade', str(id_cidade)).replace('id_do_status', str(id_status)), conectar())
    data_orion = pd.read_sql_query(st.secrets["query_raw_monthly"]["orion"].replace('initial_date', month_range(initial_date.year, initial_date.month)[0]).replace('final_date', month_range(final_date.year, final_date.month)[1]).replace('id_da_cidade', str(id_cidade)).replace('id_do_status', str(id_status)), conectar_orion())

    data_orion['imobiliaria'] = 'Órion'

    data = pd.concat([data_scrap, data_orion], sort=False)

    # data['bairro'] = data['bairro'].apply(lambda x: " ".join(x.split()[-2:]) if len(x.split()) > 2 else x)

    return data