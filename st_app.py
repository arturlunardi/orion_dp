import pandas as pd
import streamlit as st
import create_dataset
import json
import requests
import os
import googleapiclient.discovery
import geocoder
from streamlit_folium import folium_static
import folium
from io import BytesIO
from folium import plugins
import integrate
import tabula
import re
from dateutil.relativedelta import relativedelta
import datetime
import seguros
import get_raw_data
import numpy as np
import plotly.express as px


st.set_page_config(
    page_title="Plataforma de Dados da Órion",
    page_icon="https://i.ibb.co/m6kBTBT/INS2020-07-AVATAR-1024.jpg",
    layout="wide",
    initial_sidebar_state="expanded",
)


def check_password(secrets_key):
    """Returns `True` if the user had the correct password."""

    def password_entered():
        """Checks whether a password entered by the user is correct."""
        if st.session_state[secrets_key] == st.secrets[secrets_key]:
            st.session_state[f"password_correct_{secrets_key}"] = True
            del st.session_state[secrets_key]  # don't store password
        else:
            st.session_state[f"password_correct_{secrets_key}"] = False

    if f"password_correct_{secrets_key}" not in st.session_state:
        # First run, show input for password.
        st.text_input(
            "Password", type="password", on_change=password_entered, key=secrets_key
        )
        return False
    elif not st.session_state[f"password_correct_{secrets_key}"]:
        # Password not correct, show input + error.
        st.text_input(
            "Password", type="password", on_change=password_entered, key=secrets_key
        )
        st.error("Password incorreto 😕")
        return False
    else:
        # Password correct.
        return True


def predict_json(project, model, instances, signature_name, version=None):
    """Send json data to a deployed model for prediction.

    Args:
        project (str): project where the Cloud ML Engine Model is deployed.
        model (str): model name.
        instances ([Mapping[str: Any]]): Keys should be the names of Tensors
            your deployed model expects as inputs. Values should be datatypes
            convertible to Tensors, or (potentially nested) lists of datatypes
            convertible to tensors.
        version: str, version of the model to target.
    Returns:
        Mapping[str: any]: dictionary of prediction results defined by the
            model.
    """
    # Create the ML Engine service object.
    # To authenticate set the environment variable
    CREDENTIAL_FILE = st.secrets['CREDENTIAL_FILE']
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = CREDENTIAL_FILE

    service = googleapiclient.discovery.build('ml', 'v1')
    name = 'projects/{}/models/{}'.format(project, model)

    if version is not None:
        name += '/versions/{}'.format(version)

    response = service.projects().predict(
        name=name,
        body={"instances": instances,
        "signature_name": signature_name},
    ).execute()

    if 'error' in response:
        raise RuntimeError(response['error'])

    return response['predictions']


def real_br_money_mask(my_value):
    if my_value == '' or my_value is None:
        return np.nan
    a = '{:,.2f}'.format(float(my_value))
    b = a.replace(',','v')
    c = b.replace('.',',')
    return c.replace('v','.')


def real_br_money_mask_to_float(my_value):
    a = my_value.replace('.','')
    b = a.replace(',','.')
    return float(b)


@st.cache
def to_excel(df):
    output = BytesIO()
    writer = pd.ExcelWriter(output, engine='xlsxwriter')
    df.to_excel(writer, index=False, sheet_name='Sheet1')
    writer.save()
    processed_data = output.getvalue()
    return processed_data


def plot_imoveis_fig(grouped_df):
    fig = px.line(grouped_df.loc[grouped_df['imobiliaria']!='VivaReal'], x='data_analise', y='count', color='imobiliaria', template='simple_white', markers=True, labels={'count': 'Imóveis Anunciados', 'data_analise': 'Data'}, height=450, width=1300)
    fig.update_xaxes(
        rangeslider_visible=True,
        rangeselector=dict(
            buttons=list([
                dict(count=1, label="1m", step="month", stepmode="backward"),
                dict(count=6, label="6m", step="month", stepmode="backward"),
                dict(count=1, label="Este Ano", step="year", stepmode="todate"),
                dict(count=1, label="1y", step="year", stepmode="backward"),
                dict(step="all")
            ])
        )
    )
    fig.update_traces(hovertemplate='<br><b>Imóveis</b>: %{y:.2d}</br>')
    fig.update_layout(hovermode="x unified")
    fig.update_xaxes(
        dtick="M1",
        tickformat="%b %d\n%Y")

    return fig

def change_dict_key(dictionary):
    # print(f"before {dictionary}")
    if type(dictionary) == str:
        dictionary = eval(dictionary)
    for key in dictionary.copy():
        dictionary[dict_replace_agenciadores.get(key)] = dictionary.pop(key)
    # print(f"after {dictionary}")
    return dictionary


if check_password("password"):

    # ----------- Global Sidebar ---------------

    condition = st.sidebar.selectbox(
        "Selecione a Aba",
        # ("Home", "Melhores Imóveis", "Previsão de Valor de Aluguel", "Desempenho de Equipes", "Cálculo de Comissões", "Criação Imóvel Sami/Vista", "Conferência de Seguros", "Gerador de Assinatura de E-mail")
        ("Home", "Desempenho de Equipes", "Cálculo de Comissões", "Criação Imóvel Sami/Vista", "Conferência de Seguros", "Gerador de Assinatura de E-mail")
    )

    # ------------- Introduction ------------------------

    if condition == 'Home':
        st.subheader('Sobre')
        
        st.write("""
        Esse aplicativo foi criado para ajudar você a utilizar os serviços de tecnologia da nossa empresa. O aplicativo é dividido em abas, onde cada aba
        possui uma função específica.
        """)

        st.subheader('Melhores Imóveis')

        st.write("""
        Na aba de Melhores Imóveis temos um conjunto de dados que mostra os imóveis mais prováveis de serem alugados. Você também pode filtrar os dados 
        conforme sua necessidade.
        """)

        st.subheader('Previsão de Valor de Aluguel')

        st.write("""
        Nessa aba você pode prever o valor de aluguel de um imóvel, passando os dados necessários. É necessário uma senha a parte para acessar esse módulo.
        """)

    # ------------- Melhores Imóveis ------------------------

    elif condition == 'Melhores Imóveis':

        @st.cache(hash_funcs={"_thread.RLock": lambda _: None, 'builtins.weakref': lambda _: None}, show_spinner=False)
        def get_datasets():
            """Returns a list of datasets."""

            df_oferta = create_dataset.get_dataset_for_scraper()

            df_oferta = df_oferta.loc[df_oferta['tipologia'] != 'Terreno']

            my_dict = df_oferta.drop(columns=['link', 'imobiliaria', 'alugado']).to_dict(orient='records')
            my_dict = [{k: [v] for k, v in i.items()} for i in my_dict]

            url = st.secrets['predict_best_imoveis_url']
            headers = {"content-type": "application/json"}
            data = {
            "signature_name": st.secrets['signature_name'],
            "instances": my_dict
            }

            data = json.dumps(data)
            json_response = requests.post(url, data=data, headers=headers)

            list_of_predictions = json.loads(json_response.content)['predictions']
            df_oferta['predict_proba'] = [i[0] for i in list_of_predictions]

            df_oferta['suites'] = df_oferta['suites'].fillna(0)
            df_oferta['vagas_garagem'] = df_oferta['vagas_garagem'].fillna(0)

            # df_orion = df_oferta.loc[df_oferta['imobiliaria'] == 'Órion'].drop(columns=['latitude', 'longitude', 'alugado'])
            # df_outros = df_oferta.loc[df_oferta['imobiliaria'] != 'Órion'].drop(columns=['latitude', 'longitude', 'alugado'])
            df_orion = df_oferta.loc[df_oferta['imobiliaria'] == 'Órion'].drop(columns=['alugado'])
            df_outros = df_oferta.loc[df_oferta['imobiliaria'] != 'Órion'].drop(columns=['alugado'])
        
            return df_oferta, df_orion, df_outros


        # Get Datasets
        with st.spinner('Carregando os dados...'):
            df_oferta, df_orion, df_outros = get_datasets()
            df_advertiser = get_raw_data.raw_data_monthly(last_month=False, id_cidade=1, id_status=(1, 3))

        # SideBar Filters
        st.sidebar.subheader("Filtros")

        filter_finalidade = {'finalidade': st.sidebar.selectbox(
        'Finalidade',
        ["Todos"] + [i for i in df_oferta['finalidade'].sort_values(ascending=True).unique()]
        )}

        filter_bairro = {'bairro': st.sidebar.selectbox(
        'Bairro',
        ["Todos"] + [i for i in df_oferta['bairro'].sort_values(ascending=True).unique()]
        )}

        filter_tipologia = {'tipologia': st.sidebar.selectbox(
        'Tipologia',
        ["Todos"] + [i for i in df_oferta['tipologia'].sort_values(ascending=True).unique()]
        )}

        filter_dormitorios = {'dormitorios': st.sidebar.text_input(
        label='Dormitórios',
        value="Todos",
        placeholder="Todos"
        # step=1  
        )}

        filter_area = {'area_privativa': st.sidebar.slider(
            label='Área (m²)',
            # min_value=0.,
            # max_value=max(df_oferta['area_privativa'].max(), df_orion['area_privativa'].max()),
            # max_value=2000.,
            value=[0., 2000.],
        )}
        
        filter_garagem = {'vagas_garagem': st.sidebar.text_input(
        label='Vagas Garagem',
        value="Todos",
        placeholder="Todos"
        # step=1 
        )}

        filter_button = st.sidebar.checkbox('Filtrar', help='Aperte o botão para filtrar os dados conforme os filtros selecionados')

        filters = [filter_finalidade, filter_bairro, filter_tipologia, filter_dormitorios, filter_area, filter_garagem]

        h = '<head><style type="text/css"></style></head>'
        bo = '<body><div style = "height:400px;width:100%;overflow:auto;">'
        bc = '</div></body>'

        # Display Datasets
        if filter_button:
            for filter in filters:
                key = [i for i in filter.keys()][0]
                value = filter.get(key)
                if value != 'Todos' and value != '' and type(value) != tuple:
                    if value.isdigit():
                        df_orion = df_orion.loc[df_orion[key] == int(value)]
                        df_oferta = df_oferta.loc[df_oferta[key] == int(value)]
                    else:
                        df_orion = df_orion.loc[df_orion[key] == value]
                        df_outros = df_outros.loc[df_outros[key] == value]
                        df_advertiser = df_advertiser.loc[df_advertiser[key] == value]
                elif type(value) == tuple:
                        df_orion = df_orion.loc[df_orion[key].between(value[0], value[1])]
                        df_oferta = df_oferta.loc[df_oferta[key].between(value[0], value[1])]
                        df_advertiser = df_advertiser.loc[df_advertiser[key].between(value[0], value[1])]
                elif value == '':
                    st.sidebar.warning(f"Por favor, selecione um valor válido no campo {key.title().replace('_', ' ')}")
                    # st.stop()

            st.header('Imóveis da Cidade')
            grouped_df = df_advertiser.groupby(['data_analise', 'imobiliaria']).size().reset_index(name='count')
            fig = plot_imoveis_fig(grouped_df)
            st.plotly_chart(fig)

            st.header('Imóveis da Órion')
            st.dataframe(df_orion.sort_values(by='predict_proba', ascending=False).reset_index(drop=True))

            st.header('Imóveis Concorrentes')
            st.dataframe(df_outros.sort_values(by='predict_proba', ascending=False).reset_index(drop=True))
            # st.write(h + bo + df_outros.sort_values(by='predict_proba', ascending=False).reset_index(drop=True).to_html(render_links=True, escape=False, bold_rows=False, float_format="%3s") + bc, unsafe_allow_html=True)
        else:
            st.header('Imóveis da Cidade')
            grouped_df = df_advertiser.groupby(['data_analise', 'imobiliaria']).size().reset_index(name='count')
            fig = plot_imoveis_fig(grouped_df)
            st.plotly_chart(fig)

            st.header('Imóveis da Órion')
            st.dataframe(df_orion.sort_values(by='predict_proba', ascending=False).reset_index(drop=True))

            st.header('Imóveis Concorrentes')
            st.dataframe(df_outros.sort_values(by='predict_proba', ascending=False).reset_index(drop=True))
            # st.write(h + bo + df_outros.sort_values(by='predict_proba', ascending=False).reset_index(drop=True).to_html(render_links=True, escape=False, bold_rows=False, float_format="%3s") + bc, unsafe_allow_html=True)

        # instantiate the map
        agenciadores_map = folium.Map(location=[-29.6837265,-53.7768193], zoom_start=12)

        # instantiate a mark cluster object for the imoveis in the dataframe
        agenciadores_clusters = plugins.MarkerCluster(options={'disableClusteringAtZoom': 16}).add_to(agenciadores_map)

        # adicionando cada imóvel para o df outros
        for lat, lng, codigo, tipologia, dormitorios, garagens, imobiliaria in zip(df_outros['latitude'], df_outros['longitude'],
        df_outros['link'], df_outros['tipologia'], df_outros['dormitorios'], df_outros['vagas_garagem'], df_outros['imobiliaria']):
            html=f'''
            <body style="font-size: 10px; font-family: Verdana;">
            <p>Tipologia: {tipologia}</p>
            <p>Dormitórios: {int(dormitorios)}</p>
            <p>Vagas: {int(garagens)}</p>
            <p>Imobiliária: {imobiliaria}</p>
            <p><a href="{codigo}" target="_blank">Abrir Imóvel</a></p>
            </body>
            '''
            iframe = folium.IFrame(html, width=150, height=150)
            popup = folium.Popup(iframe , max_width=400)
            folium.CircleMarker(
                [lat, lng],
                radius=5,
                # popup=f'<a href="https://orionsm.com.br/imovel/{codigo}" target="_blank">Abrir Imóvel</a>',
                # popup=f'Tipologia: {tipologia}\n \
                #     Imobiliaria: {imobiliaria}\n \
                # <a href="{codigo}" target="_blank">Abrir Imóvel</a>',
                popup=popup,
                color='yellow',
                fill=True,
                fill_color='blue',
                fill_opacity=0.6
            ).add_to(agenciadores_clusters)

        # adicionando cada imóvel para o df orion - em preto
        for lat, lng, codigo, tipologia, dormitorios, garagens, imobiliaria in zip(df_orion['latitude'], df_orion['longitude'],
        df_orion['link'], df_orion['tipologia'], df_orion['dormitorios'], df_orion['vagas_garagem'], df_orion['imobiliaria']):
            html=f'''
            <body style="font-size: 10px; font-family: Verdana;">
            <p>Tipologia: {tipologia}</p>
            <p>Dormitórios: {int(dormitorios)}</p>
            <p>Vagas: {int(garagens)}</p>
            <p>Imobiliária: {imobiliaria}</p>
            <p><a href="https://orionsm.com.br/imovel/{codigo}" target="_blank">Abrir Imóvel</a></p>
            </body>
            '''
            iframe = folium.IFrame(html, width=150, height=150)
            popup = folium.Popup(iframe , max_width=400)
            folium.CircleMarker(
                [lat, lng],
                radius=5,
                # popup=f'<a href="https://orionsm.com.br/imovel/{codigo}" target="_blank">Abrir Imóvel</a>',
                # popup=f'Tipologia: {tipologia}\n \
                #     Imobiliaria: {imobiliaria}\n \
                # <a href="{codigo}" target="_blank">Abrir Imóvel</a>',
                popup=popup,
                color='black',
                fill=True,
                fill_color='black',
                fill_opacity=0.6
            ).add_to(agenciadores_clusters)

        folium_static(agenciadores_map, width=1420)    
    
        # df_excel = to_excel(df_outros.sort_values(by='predict_proba', ascending=False).reset_index(drop=True))

        # st.download_button(
        # label="Pressione para Download",
        # data=df_excel,
        # file_name='extract.xlsx',
        # )

    # ------------- Predict Rent ------------------------

    elif condition == "Previsão de Valor de Aluguel":
        if check_password("gerencia_password"):
            st.subheader("Informações sobre o Modelo")

            st.markdown("Esse modelo foi desenvolvido com o objetivo de prever o valor de imóveis para **Aluguel**. O modelo possui algumas limitações, são elas: <br><br>\
            Apenas são aceitas as seguintes tipologias: **Apartamento, Kitnet, Duplex, Cobertura, Loft**. <br>\
            Apenas são aceitos os seguintes bairros: **Centro, Camobi, Nossa Senhora de Fátima, Nossa Senhora do Rosário, Bonfim, Nossa Senhora de Lourdes, Nossa Senhora Medianeira, Nossa Senhora das Dores**. <br><br>\
            Modelos não são perfeitos, sendo assim, **podem haver diferenças entre o valor estimado e o valor real.** \
            ", unsafe_allow_html=True)

            with st.form("Informações sobre o imóvel"):
                st.subheader('Por favor, preencha as características do imóvel')

                accepted_tipologia = ["Apartamento", "Kitnet", "Duplex", "Cobertura", "Loft"]
                accepted_bairro = ["Centro", "Camobi", "Nossa Senhora de Fátima", "Nossa Senhora do Rosário", "Bonfim", "Nossa Senhora de Lourdes", "Nossa Senhora Medianeira", "Nossa Senhora das Dores"]
                accepted_sacada = ["Sim", "Não"]
                accepted_churrasqueira = ["Sim", "Não"]
                accepted_mobiliado = ["Sim", "Semi", "Não"]

                CATEGORICAL_FEATURE_KEYS = [
                    'tipologia', 'bairro', 'mobiliado', 'sacada', 'churrasqueira'
                ]

                NUMERIC_FEATURE_KEYS = ['valor_condominio', 'valor_iptu_mensal', 'area_privativa']

                DENSE_NUMERIC_FEATURE_KEYS = ['dormitorios', 'suites', 'banheiros', 'vagas_garagem']

                DISTANCE_FEATURE_KEYS = ['latitude', 'longitude']

                feature_dict = {}

                col_1, col_2 = st.columns(2)

                with col_1:
                    for feature in CATEGORICAL_FEATURE_KEYS:
                        feature_dict[feature] = st.selectbox(options=[i for i in eval(f"accepted_{feature}")], label=feature.title().replace('_', ' '))
                    feature_dict['valor_iptu_mensal'] = st.number_input(label='Valor do IPTU Mensal', value=0, min_value=0, max_value=1500)
                with col_2:
                    for feature in DENSE_NUMERIC_FEATURE_KEYS:
                        if feature == 'suites' or feature == 'vagas_garagem':
                            feature_dict[feature] = st.number_input(label=feature.title().replace('_', ' '), value=0, min_value=0, max_value=10)
                        else:
                            feature_dict[feature] = st.number_input(label=feature.title().replace('_', ' '), value=1, min_value=0, max_value=10)
                    feature_dict['area_privativa'] = st.number_input(label='Área Privativa', value=30, min_value=1, max_value=1500)
                    feature_dict['valor_condominio'] = st.number_input(label='Valor do Condomínio', value=0, min_value=0, max_value=1500)
                submit_endereco = st.text_input(label='Endereço', help='Informe apenas a Rua e o nº do imóvel.')
                
                submitted = st.form_submit_button("Prever")
                if submit_endereco == '':
                    st.warning('Informe o endereço corretamente.')
                    st.stop()
                endereco_to_predict = submit_endereco + f', {feature_dict["bairro"]}, Santa Maria, Brazil'
                latlng_for_predict = geocoder.arcgis(endereco_to_predict).latlng
                feature_dict['latitude'] = latlng_for_predict[0]
                feature_dict['longitude'] = latlng_for_predict[1]

                instances = [{key: [feature_dict[key]] for key in feature_dict}]

                if submitted:
                    if not st.secrets['CREDENTIAL_FILE'] in os.listdir():
                        with open(st.secrets['CREDENTIAL_FILE'], 'w', encoding='utf-8') as f:
                            json.dump(st.secrets['json_file'], f, ensure_ascii=False, indent=4)

                    with st.spinner('Prevendo...'):
                        model = st.secrets['model']
                        signature_name = st.secrets['signature_name']
                        LOCATION = st.secrets['LOCATION']
                        GOOGLE_CLOUD_PROJECT = st.secrets['GOOGLE_CLOUD_PROJECT']
                        predictions = predict_json(GOOGLE_CLOUD_PROJECT, model, instances, signature_name, version=None)
                        predictions = [real_br_money_mask(round(i['outputs'][0]/10)*10) for i in predictions][0]
                        st.success(f"O aluguel previsto é de **R$ {predictions}**")

                        st.write('Confirme a localização do seu imóvel e os arredores.')

                        m = folium.Map(location=[feature_dict['latitude'], feature_dict['longitude']], zoom_start=20)

                        # tooltip = "Imóvel"
                        folium.Marker(
                            [feature_dict['latitude'], feature_dict['longitude']], # popup="Imóvel", # tooltip=tooltip
                        ).add_to(m)

                        #
                        # call to render Folium map in Streamlit
                        folium_static(m)

    # ------------- Desempenho de Equipes ------------------------

    elif condition == 'Desempenho de Equipes':
        df_usuarios_vista = create_dataset.get_df_usuarios(only_vendas=False)
        # df_usuarios_vista = create_dataset.get_df_usuarios(only_vendas=False, only_exibir_site=False)

        col_1, col_2 = st.columns(2)
        with col_1:
            data_inicio = st.date_input(label='Data de Início')
        with col_2:
            data_termino = st.date_input(label='Data de Término')

        type_of_role = st.radio(label='Cargo', options=['Corretor (Apenas Locação)', 'Agenciador'])

        type_of_report = st.radio(label='Tipo de Relatório', options=['Compacto', 'Detalhado'])

        if type_of_role == 'Agenciador':
            st.info("Atenção! Para um imóvel ser considerado como agenciado significa que: \n- Ele foi **criado** no Vista no período informado acima \n- Foi **disponibilizado** para o Status definido abaixo. \n - **Não é um Imóvel de Desocupação** marcado no Vista.")
            nome_agenciadores_list = ["Todos"] + df_usuarios_vista['Nomecompleto'].tolist()
            nome_agenciadores = st.multiselect(options=nome_agenciadores_list, label='Selecione os Agenciadores')
            if "Todos" in nome_agenciadores:
                agenciadores_to_filter = df_usuarios_vista['Nomecompleto'].tolist()
            else:
                agenciadores_to_filter = nome_agenciadores
            type_of_status = st.radio(label='Status', options=['Disponibilizados para Locação ou Venda', 'Disponibilizados apenas para Locação', 'Disponibilizados apenas para Venda'])
            if type_of_report == 'Compacto':
                groupby_type = st.selectbox(label='Selecione o tipo de agrupamento', options=['Agenciador', 'Finalidade', 'Status', 'Categoria', 'Bairro'])
            else:
                groupby_type=None 

        submit = st.button('Calcular')

        if submit:
            with st.spinner('Carregando os dados...'):
                if type_of_role == 'Corretor (Apenas Locação)':
                    df_imoveis_locados = create_dataset.get_corretores_vendas_table(type_of_report, data_inicio.strftime('%Y/%m/%d'), data_termino.strftime('%Y/%m/%d'))               
                    st.dataframe(df_imoveis_locados)
                elif type_of_role == 'Agenciador':
                    agenciadores_vista = df_usuarios_vista.loc[df_usuarios_vista['Nomecompleto'].isin(agenciadores_to_filter)]['Codigo'].tolist()
                    dict_replace_agenciadores = df_usuarios_vista.set_index('Codigo')['Nomecompleto'].to_dict()
                    # TODO: arrumar o dataset. Eu estou identificando os imóveis dos corretores dos códigos passados, mas quando faço o request pra pegar o imóvel,
                    # ele vem com o código do primeiro corretor do imóvel. Ex: 2022-04-01 | 2022-04-30 | imóvel 647014, corretor selecionado Odivan, mas vem Juliana.
                    df_agenciamentos = create_dataset.get_agenciamentos_tables(type_of_report, data_inicio.strftime('%Y-%m-%d'), data_termino.strftime('%Y-%m-%d'), agenciadores_vista, dict_replace_agenciadores, type_of_status, groupby_type)
                    st.dataframe(df_agenciamentos)
                    if type_of_report == 'Detalhado':
                        # instantiate the map
                        agenciamentos_map = folium.Map(location=[-29.6837265,-53.7768193], zoom_start=12)
                        # instantiate a mark cluster object for the imoveis in the dataframe
                        agenciamentos_cluster = plugins.MarkerCluster(options={'disableClusteringAtZoom': 16}).add_to(agenciamentos_map)
                        # adicionando cada imóvel para o df outros
                        for lat, lng, codigo, tipologia, dormitorios, garagens, valor_loc, valor_venda in zip(df_agenciamentos['Latitude'], df_agenciamentos['Longitude'], df_agenciamentos['Codigo'], df_agenciamentos['Categoria'], df_agenciamentos['Dormitorios'], df_agenciamentos['Vagas'], df_agenciamentos['ValorLocacao'], df_agenciamentos['ValorVenda']):
                            html=f'''
                            <body style="font-size: 10px; font-family: Verdana;">
                            <p>Codigo: {codigo}</p>
                            <p>Tipologia: {tipologia}</p>
                            <p>Dormitórios: {int(dormitorios)}</p>
                            <p>Vagas: {int(garagens)}</p>
                            <p>Valor Locação: {valor_loc}</p>
                            <p>Valor Venda: {valor_venda}</p>
                            </body>
                            '''
                            iframe = folium.IFrame(html, width=150, height=150)
                            popup = folium.Popup(iframe , max_width=400)
                            folium.CircleMarker(
                                [lat, lng],
                                radius=5,
                                popup=popup,
                                color='yellow',
                                fill=True,
                                fill_color='blue',
                                fill_opacity=0.6
                            ).add_to(agenciamentos_cluster)

                        folium_static(agenciamentos_map, width=1420)

    # ------------- Cálculo de Comissões ------------------------

    elif condition == 'Cálculo de Comissões':
        if check_password("gerencia_password"):
            df_usuarios_vista = create_dataset.get_df_usuarios(only_vendas=False, only_exibir_site=True)
            # df_usuarios_vista_ativos = create_dataset.get_df_usuarios(only_vendas=False, only_exibir_site=True)

            col_1, col_2 = st.columns(2)
            with col_1:
                data_inicio = st.date_input(label='Data de Início')
            with col_2:
                data_termino = st.date_input(label='Data de Término')

            type_of_role = st.radio(label='Cargo', options=['Corretor (Apenas Locação)', 'Agenciador'])

            type_of_report = st.radio(label='Tipo de Relatório', options=['Compacto', 'Detalhado'])

            submit = st.button('Calcular')

            if submit:
                with st.spinner('Carregando os dados...'):
                    if type_of_role == 'Corretor (Apenas Locação)':
                        st.subheader('Corretores de Locação')
                        df_comissao_corretores = create_dataset.get_corretores_vendas_table(type_of_report, data_inicio.strftime('%Y-%m-%d'), data_termino.strftime('%Y-%m-%d'))
                        corretores_que_bateram_a_meta = []
                        
                        if type_of_report == 'Compacto':
                            df_comissao_corretores['comissao_corretor'] = df_comissao_corretores['Soma de Valor do Aluguel'].apply(real_br_money_mask_to_float).apply(lambda x: x*st.secrets['codigos_importantes']['comissao_corretor_meta_batida'] if x >= st.secrets["metas"]["corretores"] else x*st.secrets['codigos_importantes']['comissao_corretor_meta_nao_batida']).apply(real_br_money_mask)
                            for corretor in df_comissao_corretores['Nome'].dropna().unique():
                                if real_br_money_mask_to_float(df_comissao_corretores.loc[df_comissao_corretores['Nome'] == corretor]['Soma de Valor do Aluguel'].squeeze()) > st.secrets["metas"]["corretores"]:
                                    corretores_que_bateram_a_meta.append(f"**{corretor}**")
                        elif type_of_report == 'Detalhado':
                            # primeiro transformo o valor do aluguel em float novamente
                            df_comissao_corretores['Valor do Aluguel'] = df_comissao_corretores['Valor do Aluguel'].apply(real_br_money_mask_to_float)


                            # crio um df agrupado de valor do aluguel por corretor
                            df_agrupado = df_comissao_corretores.groupby('Nome').sum()['Valor do Aluguel']
                            st.write(df_agrupado)

                            # agora para cada corretor, veja se o valor agrupado do aluguel é maior que a meta
                            # se for, multiplique cada valor desse corretor por meta batida, se não meta não batida
                            for corretor in df_comissao_corretores['Nome'].dropna().unique():
                                if df_agrupado.loc[corretor] >= st.secrets["metas"]["corretores"]:
                                    df_comissao_corretores.loc[df_comissao_corretores['Nome'] == corretor, 'comissao_corretor'] = df_comissao_corretores.loc[df_comissao_corretores['Nome'] == corretor]['Valor do Aluguel'].apply(lambda x: x*st.secrets['codigos_importantes']['comissao_corretor_meta_batida']).apply(real_br_money_mask)
                                    corretores_que_bateram_a_meta.append(f"**{corretor}**")
                                else:
                                    df_comissao_corretores.loc[df_comissao_corretores['Nome'] == corretor, 'comissao_corretor'] = df_comissao_corretores.loc[df_comissao_corretores['Nome'] == corretor]['Valor do Aluguel'].apply(lambda x: x*st.secrets['codigos_importantes']['comissao_corretor_meta_nao_batida']).apply(real_br_money_mask) 
                            df_comissao_corretores['Valor do Aluguel'] = df_comissao_corretores['Valor do Aluguel'].apply(real_br_money_mask)

                        new_line = '\n - '
                        if len(corretores_que_bateram_a_meta) > 0:
                            st.success(f'Corretores que bateram a meta: {new_line}{new_line.join(corretores_que_bateram_a_meta)}')
                        else:
                            st.warning('Nenhum corretor bateu a meta!!!!')
                        st.dataframe(df_comissao_corretores)

                        df_excel = to_excel(df_comissao_corretores.reset_index(drop=True))

                        st.download_button(
                        label="Pressione para Download",
                        data=df_excel,
                        file_name='extract.xlsx',
                        )
                    
                    elif type_of_role == 'Agenciador':
                        sistema_lider = False

                        st.info("Atenção! Para um imóvel ser considerado como agenciado significa que: \n- Ele foi **criado** no Vista no período informado acima \n- Foi **disponibilizado** para o Status Locação. \n - **Não é um Imóvel de Desocupação** marcado no Vista.")
                        # a logica é essa:
                        # para todos os imóveis locados, entre em cada imóvel e verifique os agenciadores, depois calcule em cima disso.
                        # eu vou ter que fazer 2 métodos, o primeiro é verificar se a equipe bateu a meta de imóveis, e a outra é porcentagem em cima de locações
                        st.subheader('Agenciadores')
                        df_comissao_detalhado_locados_agenciadores = create_dataset.get_corretores_vendas_table(type_of_report='Detalhado', data_inicio=data_inicio.strftime('%Y/%m/%d'), data_termino=data_termino.strftime('%Y/%m/%d'))

                        dict_replace_agenciadores = df_usuarios_vista.set_index('Codigo')['Nomecompleto'].to_dict()

                        # to pegando os codigos de usuarios gerente pq se tiver um gerente como agenciador, não vou contabilizar ele no pagamento
                        codigo_usuarios_gerente = df_usuarios_vista.loc[df_usuarios_vista['Gerente'] == 'Sim']['Codigo'].tolist()

                        # retornando o dataframe com os imoveis locados e os corretores responsaveis por ele

                        # na logica do algoritmo, eu calculo tudo por códigos do corretor e só depois eu troco o código pelo nome do corretor direto no dataframe/dict
                        # tem como trocar pelo nome antes, mas isso pode dar problema se duas pessoas tiverem o nome exatamente igual.
                        df_agenciamentos_comissoes = create_dataset.get_agenciamentos_comissoes(df_comissao_detalhado_locados_agenciadores, type_of_report, codigo_usuarios_gerente) 

                        # pegando todos os usuarios do vista que fazem parte da equipe de agenciamentos
                        df_usuarios_equipe_agenciadores = df_usuarios_vista.loc[df_usuarios_vista['Equipe'] == 'Agenciador de Imóveis']

                        # pegando todos os usuarios do vista que fazem parte da equipe de vendas
                        df_usuarios_equipe_vendas = df_usuarios_vista.loc[df_usuarios_vista['Equipe'] == 'Vendas']

                        # ----------------- comissões de metas -----------------
                        st.subheader('Comissões de Metas')
                        df_agenciamentos_metas_imoveis = create_dataset.get_agenciamentos_tables(type_of_report, data_inicio.strftime('%Y-%m-%d'), data_termino.strftime('%Y-%m-%d'), df_usuarios_equipe_agenciadores['Codigo'].tolist(), dict_replace_agenciadores, 'Disponibilizados apenas para Locação', 'Agenciador')
                    
                        if type_of_report == 'Compacto':
                            # df_agenciamentos_metas_imoveis_compacto = create_dataset.get_agenciamentos_tables('Compacto', data_inicio.strftime('%Y-%m-%d'), data_termino.strftime('%Y-%m-%d'), df_usuarios_equipe_agenciadores['Codigo'].tolist(), dict_replace_agenciadores, 'Disponibilizados apenas para Locação', 'Agenciador')
                            # st.write(df_agenciamentos_metas_imoveis_compacto)
                            ags_feitos = df_agenciamentos_metas_imoveis["Quantidade"].sum()

                        elif type_of_report == 'Detalhado':
                            # df_agenciamentos_metas_imoveis_detalhado = create_dataset.get_agenciamentos_tables('Detalhado', data_inicio.strftime('%Y-%m-%d'), data_termino.strftime('%Y-%m-%d'), df_usuarios_equipe_agenciadores['Codigo'].tolist(), dict_replace_agenciadores, 'Disponibilizados apenas para Locação', None)
                            ags_feitos = len(df_agenciamentos_metas_imoveis)

                        if ags_feitos >= st.secrets["metas"]["agenciadores_locacao"]:
                            st.success(f'Parabéns, a meta foi batida! Foram agenciados **{ags_feitos} imóveis** e a meta é de **{st.secrets["metas"]["agenciadores_locacao"]} imóveis**.')
                            if type_of_report == 'Compacto':
                                df_agenciamentos_metas_imoveis['Valor da Comissão'] = real_br_money_mask(st.secrets["codigos_importantes"]["comissao_agenciadores_meta_imoveis"])
                        else:
                            st.warning(f'Infelizmente a meta de agenciamentos não foi alcançada. Foram agenciados **{ags_feitos} imóveis** e a meta é de **{st.secrets["metas"]["agenciadores_locacao"]} imóveis**.')
                            if type_of_report == 'Compacto':
                                df_agenciamentos_metas_imoveis['Valor da Comissão'] = real_br_money_mask(st.secrets["codigos_importantes"]["comissao_agenciadores_meta_nao_batida_imoveis"])
                            
                        st.write(df_agenciamentos_metas_imoveis)

                        # --------------------- comissões de locados --------------------------
                        st.subheader('Comissões de Imóveis Locados')

                        st.info(f"Foram locados **{len(df_comissao_detalhado_locados_agenciadores)} imóveis** no período entre **{data_inicio.strftime('%Y-%m-%d')}** e **{data_termino.strftime('%Y-%m-%d')}**.")
                        
                        comissao_agenciadores = {}

                        # para todos os imóveis locados.
                        for imovel in df_comissao_detalhado_locados_agenciadores['cod_crm']:
                            # to colocando só nesse imóvel aqui um strip, pq no sami pode ter um imóvel com um espaço a mais, e no vista não.
                            imovel_no_vista = df_agenciamentos_comissoes.loc[df_agenciamentos_comissoes['Codigo'] == imovel.strip()]
                            # pegue o valor do imóvel
                            valor_imovel = real_br_money_mask_to_float(df_comissao_detalhado_locados_agenciadores.loc[df_comissao_detalhado_locados_agenciadores['cod_crm'] == imovel]['Valor do Aluguel'].squeeze())
                            # pegue todos os agenciadores do imóvel em formato de lista
                            agenciadores = imovel_no_vista['Corretor'].squeeze()

                            # aqui eu to filtrando os agenciadores somente para aqueles que estão dentro dos usuarios ativos do vista
                            # pq como eu pego os corretores de imovel por imovel, pode acontecer de vir um corretor que não faz parte da empresa mais mas que está vinculado ao imóvel
                            agenciadores = [x for x in agenciadores if x in df_usuarios_vista['Codigo'].tolist()]

                            # aqui estou transformando os codigos em nomes pra ficar mais legível pro pessoal
                            # agenciadores = [dict_replace_agenciadores.get(agenciador) for agenciador in agenciadores]
                            # codigo_lider = dict_replace_agenciadores.get(st.secrets['codigos_importantes']['lider'])

                            if sistema_lider:

                                codigo_lider = st.secrets['codigos_importantes']['lider']

                                # pegando os valores de cada colaborador
                                if imovel_no_vista["ImoveisDesocupacao"].squeeze() == "Nao" and imovel_no_vista["CaptacaoPassiva"].squeeze() == "Nao":
                                    valor_lider = st.secrets['codigos_importantes']['lider_valor_cheio']
                                    valor_agenciador = st.secrets['codigos_importantes']['agenciadores_valor_cheio']
                                
                                elif imovel_no_vista["ImoveisDesocupacao"].squeeze() == "Nao" and imovel_no_vista["CaptacaoPassiva"].squeeze() == "Sim":
                                    valor_lider = st.secrets['codigos_importantes']['lider_valor_passivo']
                                    valor_agenciador = st.secrets['codigos_importantes']['agenciadores_valor_passivo']

                                else:
                                    valor_lider = st.secrets['codigos_importantes']['lider_valor_desocupacao']
                                    valor_agenciador = st.secrets['codigos_importantes']['agenciadores_valor_desocupacao']


                                # valor_lider = st.secrets['codigos_importantes']['lider_valor_cheio'] if imovel_no_vista["ImoveisDesocupacao"].squeeze() == "Nao" and imovel_no_vista["CaptacaoPassiva"].squeeze() == "Nao" else st.secrets['codigos_importantes']['lider_valor_passivo_desocupacao']
                                # valor_agenciador = st.secrets['codigos_importantes']['agenciadores_valor_cheio'] if imovel_no_vista["ImoveisDesocupacao"].squeeze() == "Nao" and imovel_no_vista["CaptacaoPassiva"].squeeze() == "Nao" else st.secrets['codigos_importantes']['agenciadores_passivo_desocupacao']

                                # aqui eu to usando o nomecompleto pq no dict replace agenciadores tb usei nome completo
                                # lider_ganha = any([True for x in agenciadores if x in df_usuarios_equipe_agenciadores['Nomecompleto'].unique().tolist()])
                                # se nenhum corretor da equipe agenciadores ou vendas estiver no imóvel, o lider também não ganha
                                lider_ganha = any([True for x in agenciadores if x in df_usuarios_equipe_agenciadores['Codigo'].unique().tolist() or x in df_usuarios_equipe_vendas['Codigo'].unique().tolist()])
                                
                                # print({imovel: comissao_agenciadores.get('29'), "antes": True})

                                # se o lider n estiver no dict, crie a chave e forneça o valor do lider
                                if not codigo_lider in comissao_agenciadores:
                                    comissao_agenciadores[codigo_lider] = valor_imovel * valor_lider if lider_ganha else 0
                                # caso já exista, incremente o valor do lider
                                else:
                                    comissao_agenciadores[codigo_lider] += valor_imovel * valor_lider if lider_ganha else 0

                                # para cada agenciador, verifique se ele está no dict, se não, crie a chave e forneça o valor do agenciador
                                for agenciador in agenciadores:
                                    if not agenciador in comissao_agenciadores:
                                        comissao_agenciadores[agenciador] = valor_imovel * valor_agenciador / len(agenciadores)
                                    # caso já exista, incremente o valor do agenciador
                                    else:
                                        comissao_agenciadores[agenciador] += valor_imovel * valor_agenciador / len(agenciadores)

                                # print({imovel: comissao_agenciadores.get('29'), "antes": False})

                                # só pra eu ver se deu certo
                                # nao faço mais com json dumps pq eu uso o dict pra explodir no dataframe
                                # df_comissao_detalhado_locados_agenciadores.loc[df_comissao_detalhado_locados_agenciadores['cod_crm'] == imovel, 'comissao_lider'] = json.dumps({codigo_lider: valor_imovel * valor_lider if lider_ganha else 0})
                                # df_comissao_detalhado_locados_agenciadores.loc[df_comissao_detalhado_locados_agenciadores['cod_crm'] == imovel, 'comissao_agenciadores'] = json.dumps({agenciador: round(valor_imovel * valor_agenciador / len(agenciadores), 3) for agenciador in agenciadores})
                                df_comissao_detalhado_locados_agenciadores.loc[df_comissao_detalhado_locados_agenciadores['cod_crm'] == imovel, 'comissao_lider'] = [{codigo_lider: valor_imovel * valor_lider if lider_ganha else 0}]
                                df_comissao_detalhado_locados_agenciadores.loc[df_comissao_detalhado_locados_agenciadores['cod_crm'] == imovel, 'comissao_agenciadores'] = [{agenciador: round(valor_imovel * valor_agenciador / len(agenciadores), 3) for agenciador in agenciadores}]
                                # print([{agenciador: round(valor_imovel * valor_agenciador / len(agenciadores), 3) for agenciador in agenciadores}])
                        
                            else:
                                # pegando os valores de cada colaborador
                                if imovel_no_vista["ImoveisDesocupacao"].squeeze() == "Nao" and imovel_no_vista["CaptacaoPassiva"].squeeze() == "Nao":
                                    valor_agenciador = st.secrets['codigos_importantes']['agenciadores_valor_cheio'] + st.secrets['codigos_importantes']['lider_valor_cheio']
                                
                                elif imovel_no_vista["ImoveisDesocupacao"].squeeze() == "Nao" and imovel_no_vista["CaptacaoPassiva"].squeeze() == "Sim":
                                    valor_agenciador = st.secrets['codigos_importantes']['agenciadores_valor_passivo'] + st.secrets['codigos_importantes']['lider_valor_passivo']

                                else:
                                    valor_agenciador = st.secrets['codigos_importantes']['agenciadores_valor_desocupacao'] + st.secrets['codigos_importantes']['lider_valor_desocupacao']

                                # para cada agenciador, verifique se ele está no dict, se não, crie a chave e forneça o valor do agenciador
                                for agenciador in agenciadores:
                                    if not agenciador in comissao_agenciadores:
                                        comissao_agenciadores[agenciador] = valor_imovel * valor_agenciador / len(agenciadores)
                                    # caso já exista, incremente o valor do agenciador
                                    else:
                                        comissao_agenciadores[agenciador] += valor_imovel * valor_agenciador / len(agenciadores)

                                df_comissao_detalhado_locados_agenciadores.loc[df_comissao_detalhado_locados_agenciadores['cod_crm'] == imovel, 'comissao_agenciadores'] = [{agenciador: round(valor_imovel * valor_agenciador / len(agenciadores), 3) for agenciador in agenciadores}]
                                # print([{agenciador: round(valor_imovel * valor_agenciador / len(agenciadores), 3) for agenciador in agenciadores}])

                        # isso aqui fica visualmente feio pq aparentemente os dicionarios em uma coluna pandas tem que ter o mesmo tamanho, e fica nome de corretor: null
                        # df_comissao_detalhado_locados_agenciadores['comissao_agenciadores'] = df_comissao_detalhado_locados_agenciadores['comissao_agenciadores'].apply(change_dict_key)
                        # df_comissao_detalhado_locados_agenciadores['comissao_agenciadores'] = df_comissao_detalhado_locados_agenciadores['comissao_agenciadores'].apply(lambda x: {dict_replace_agenciadores.get(key): value for key, value in x.items()})

                        if sistema_lider:
                            for column in ['comissao_lider', 'comissao_agenciadores']:
                                # st.write(pd.concat([df_comissao_detalhado_locados_agenciadores.drop(['comissao_agenciadores'], axis=1), pd.json_normalize(df_comissao_detalhado_locados_agenciadores['comissao_agenciadores'])], axis=1))
                                df_comissao_detalhado_locados_agenciadores = df_comissao_detalhado_locados_agenciadores.join(pd.json_normalize(df_comissao_detalhado_locados_agenciadores[column]), lsuffix='_lider').drop([column], axis=1)
                                if column == 'comissao_lider':
                                    df_comissao_detalhado_locados_agenciadores = df_comissao_detalhado_locados_agenciadores.rename(columns={codigo_lider: "Lider"})
                                else:
                                    df_comissao_detalhado_locados_agenciadores = df_comissao_detalhado_locados_agenciadores.rename(columns=dict_replace_agenciadores)

                        else:
                            for column in ['comissao_agenciadores']:
                                # st.write(pd.concat([df_comissao_detalhado_locados_agenciadores.drop(['comissao_agenciadores'], axis=1), pd.json_normalize(df_comissao_detalhado_locados_agenciadores['comissao_agenciadores'])], axis=1))
                                df_comissao_detalhado_locados_agenciadores = df_comissao_detalhado_locados_agenciadores.join(pd.json_normalize(df_comissao_detalhado_locados_agenciadores[column]), lsuffix='_lider').drop([column], axis=1)
                                df_comissao_detalhado_locados_agenciadores = df_comissao_detalhado_locados_agenciadores.rename(columns=dict_replace_agenciadores)

                        for column in df_comissao_detalhado_locados_agenciadores.iloc[:, 5:].columns:
                            df_comissao_detalhado_locados_agenciadores[column] = df_comissao_detalhado_locados_agenciadores[column].apply(lambda x: real_br_money_mask(x).replace('nan', '0,00'))

                        comissao_agenciadores = change_dict_key(comissao_agenciadores)

                        df_comissao_compacto_locados_agenciadores = pd.DataFrame.from_dict(comissao_agenciadores, orient='index').reset_index().rename(columns={"index": "Agenciador", 0: "Valor de Comissão"})
                        df_comissao_compacto_locados_agenciadores['Valor de Comissão'] = df_comissao_compacto_locados_agenciadores['Valor de Comissão'].apply(real_br_money_mask)
                        if type_of_report == 'Detalhado':
                            st.write(df_comissao_detalhado_locados_agenciadores)
                            df_excel = to_excel(df_comissao_detalhado_locados_agenciadores.reset_index(drop=True))
                        elif type_of_report == 'Compacto':
                            st.write(df_comissao_compacto_locados_agenciadores)
                            df_excel = to_excel(df_comissao_compacto_locados_agenciadores.reset_index(drop=True))

                        st.download_button(
                        label="Pressione para Download",
                        data=df_excel,
                        file_name='extract.xlsx',
                        )

    # ------------- Criação Imóvel Sami/Vista ------------------------

    elif condition == 'Criação Imóvel Sami/Vista':
        if check_password("gerencia_password"):
            cod_imovel_vista = st.number_input("Digite o código do imóvel no Vista", help='Esse imóvel será criado no Sami e editado com os dados do Vista', step=1)
            opcao = st.selectbox("Escolha a opção", ["Criar Imóvel", "Editar Imóvel"])
            
            if opcao == 'Criar Imóvel':
                botao_criar = st.button("Criar Imóvel")
                if botao_criar:
                    cod_imovel_sami_criado = integrate.duplicate_imovel_sami()
                    st.write(f'O código do imóvel no Sami é {cod_imovel_sami_criado}')
            elif opcao == 'Editar Imóvel':
                cod_imovel_sami = st.number_input(f"Digite o código criado no Sami para confirmar a edição", help='Esse imóvel será criado no Sami e editado com os dados do Vista', step=1)
                botao_editar = st.button("Editar Imóvel")
                if botao_editar:
                    integrate.edit_imovel_sami(cod_imovel_vista=str(cod_imovel_vista), cod_imovel_sami=str(cod_imovel_sami))


    # ------------- Conferência de Seguros ------------------------

    elif condition == 'Conferência de Seguros':
        if check_password("gerencia_password"):
            tipo_seguro = st.selectbox("Escolha o tipo de seguro", ["Fiança", "Incêndio"])
            if tipo_seguro == 'Fiança':
                seguradora = st.selectbox("Escolha a seguradora", ["Pottencial", "Porto Seguro", "Tokio", "Liberty"], help="Cada seguradora possui uma estrutura diferente, por isso é necessário informar.")
            elif tipo_seguro == 'Incêndio':
                seguradora = "Alfa"
            now = datetime.datetime.now()
            data_referencia = st.date_input("Escolha o mês de referência desse boleto.", datetime.date(now.year, now.month-1, 1))

            pdf_seguro = st.file_uploader(label='Anexe o pdf do seguro.', accept_multiple_files=True)
            submitted = st.button("Enviar")
            if submitted:
                df_boleto_seguro = seguros.get_df_boleto_seguro(tipo_seguro=tipo_seguro, seguradora=seguradora, pdf_seguro=pdf_seguro)
                st.write(df_boleto_seguro)
                dict_dfs_lancamento = get_raw_data.get_df_lancamentos_sami(tipo_seguro=tipo_seguro, data_referencia=data_referencia, seguradora=seguradora)

                if seguradora in ["Pottencial", "Liberty", "Alfa"]:
                    df_to_check = df_boleto_seguro.loc[~(df_boleto_seguro["CPF_Inquilino"].isin(dict_dfs_lancamento["df_checar_lancamento"]["CPF"]))].merge(dict_dfs_lancamento["df_identificar_imovel"], left_on="CPF_Inquilino", right_on="CPF", how="left")
                elif seguradora in ["Porto Seguro"]:
                    df_to_check = df_boleto_seguro.loc[~(df_boleto_seguro["CPF_Proprietario"].isin(dict_dfs_lancamento["df_checar_lancamento"]["CPF"]))].merge(dict_dfs_lancamento["df_identificar_imovel"], left_on="CPF_Proprietario", right_on="CPF", how="left")
                elif seguradora in ["Tokio"]:
                    # para o tokio é o seguinte, não existe cpf do proprietário. então eu faço um merge na tabela de proprietario pelo nome mesmo
                    # e depois eu pesquiso se esse cpf está no df lancamentos
                    df_tokio_merged = df_boleto_seguro.merge(dict_dfs_lancamento["df_identificar_imovel"], left_on='Proprietario', right_on='Nome_Prop', how='left')
                    df_tokio_merged.rename(columns={"CPF": "CPF_Proprietario"}, inplace=True)
                    df_to_check = df_tokio_merged.loc[~(df_tokio_merged['CPF_Proprietario'].isin(dict_dfs_lancamento["df_checar_lancamento"]["CPF"]))]

                if df_to_check.shape[0] > 0:
                    st.error("Os seguros abaixo estão para pagamento mas não foram identificados!!! É necessário conferir um por um")
                    st.write(df_to_check)
                else:
                    st.success("Não foi identificado nenhum pagamento que não esteja lançado!")


    # ------------- Gerador de Assinatura de E-mail ------------------------

    elif condition == 'Gerador de Assinatura de E-mail':
        df_usuarios_vista = create_dataset.get_df_usuarios(only_vendas=False, all_users=True)
        # st.write(df_usuarios_vista.drop(columns=["Equipe"]))
        corretor = st.selectbox(
            "Selecione o Corretor",
            df_usuarios_vista["Nomecompleto"]
        )

        nome = st.text_input("Confira o nome", value=corretor)
        fone = st.text_input("Confira o telefone", value=df_usuarios_vista.loc[df_usuarios_vista['Nomecompleto'] == corretor]["Fone"].squeeze())
        fone_formatado = "55" + re.sub('[^0-9]', '', fone)
        cargo = st.text_input("Preencha o cargo")
        setor = st.selectbox(
            "Selecione o setor",
            ["Locações", "Vendas", "Marketing", "Administrativo", "RH", "Comercial"]
        )
        foto = df_usuarios_vista.loc[df_usuarios_vista['Nomecompleto'] == corretor]["Foto"].squeeze()
        botao = st.checkbox("Adicionar botão WhatsApp")


        if botao:
            html_string = f'''
            <table cellpadding="0" cellspacing="0" class="eQYmiW sc-gPEVay" style="font-family:Arial; font-size:small; vertical-align:-webkit-baseline-middle">
            <tbody>
                <tr>
                    <td>
                    <table cellpadding="0" cellspacing="0" class="eQYmiW sc-gPEVay" style="font-family:Arial; font-size:small; vertical-align:-webkit-baseline-middle">
                        <tbody>
                            <tr>
                                <td style="vertical-align:top">
                                <table cellpadding="0" cellspacing="0" class="eQYmiW sc-gPEVay" style="font-family:Arial; font-size:small; vertical-align:-webkit-baseline-middle">
                                    <tbody>
                                        <tr>
                                            <td style="text-align:center"><img class="bHiaRe sc-cHGsZl" src={foto} style="display:block; max-width:128px; width:130px" /></td>
                                        </tr>
                                        <tr>
                                            <td style="text-align:center">
                                            <table cellpadding="0" cellspacing="0" class="eQYmiW sc-gPEVay" style="display:inline-block; font-family:Arial; font-size:small; vertical-align:-webkit-baseline-middle">
                                                <tbody>
                                                    <tr>
                                                        <td><a class="sc-hzDkRC kpsoyz" href="https://www.facebook.com/OrionAssessoriaImobiliaria" style="display: inline-block; padding: 0px; background-color: rgb(25, 55, 107);"><img alt="facebook" class="ccSRck sc-bRBYWo" src="https://cdn2.hubspot.net/hubfs/53/tools/email-signature-generator/icons/facebook-icon-2x.png" style="background-color:#19376b; display:block; height:24px; max-width:135px" /></a></td>
                                                        <td>&nbsp;</td>
                                                        <td><a class="sc-hzDkRC kpsoyz" href="https://www.linkedin.com/company/%C3%B3rion-assessoria-imobili%C3%A1ria/" style="display: inline-block; padding: 0px; background-color: rgb(25, 55, 107);"><img alt="linkedin" class="ccSRck sc-bRBYWo" src="https://cdn2.hubspot.net/hubfs/53/tools/email-signature-generator/icons/linkedin-icon-2x.png" style="background-color:#19376b; display:block; height:24px; max-width:135px" /></a></td>
                                                        <td>&nbsp;</td>
                                                        <td><a class="sc-hzDkRC kpsoyz" href="https://www.instagram.com/imobiliariaorion/" style="display: inline-block; padding: 0px; background-color: rgb(25, 55, 107);"><img alt="instagram" class="ccSRck sc-bRBYWo" src="https://cdn2.hubspot.net/hubfs/53/tools/email-signature-generator/icons/instagram-icon-2x.png" style="background-color:#19376b; display:block; height:24px; max-width:135px" /></a></td>
                                                        <td>&nbsp;</td>
                                                    </tr>
                                                </tbody>
                                            </table>
                                            </td>
                                        </tr>
                                    </tbody>
                                </table>
                                </td>
                                <td>&nbsp;</td>
                                <td style="vertical-align:middle">
                                <h3 style="margin-left:0; margin-right:0"><strong>{nome}</strong></h3>

                                <p style="margin-left:0; margin-right:0">{cargo}</p>

                                <p style="margin-left:0; margin-right:0">{setor} | &nbsp;&Oacute;rion Assessoria Imobili&aacute;ria

                                <hr />
                                <table cellpadding="0" cellspacing="0" class="eQYmiW sc-gPEVay" style="font-family:Arial; font-size:small; vertical-align:-webkit-baseline-middle;">
                                    <tbody>
                                        <tr>
                                            <td style="vertical-align:middle">
                                            <table cellpadding="0" cellspacing="0" class="eQYmiW sc-gPEVay" style="font-family:Arial; font-size:small; vertical-align:-webkit-baseline-middle;">
                                                <tbody>
                                                    <tr>
                                                        <td style="vertical-align:bottom"><span style="background-color:#19376b"><img class="blSEcj sc-iRbamj" src="https://cdn2.hubspot.net/hubfs/53/tools/email-signature-generator/icons/phone-icon-2x.png" style="background-color:#19376b; display:block; width:13px" /></span></td>
                                                    </tr>
                                                </tbody>
                                            </table>
                                            </td>
                                            <td><a class="sc-gipzik iyhjGb" href="tel:{fone}" style="text-decoration: none; color: rgb(0, 0, 0); font-size: 12px;">{fone}</a></td>
                                        </tr>

                                        <tr style="display: block; margin-bottom: 5px;"><td></td></tr>

                                        <tr>
                                            <td style="vertical-align:middle">
                                            <table cellpadding="0" cellspacing="0" class="eQYmiW sc-gPEVay" style="font-family:Arial; font-size:small; vertical-align:-webkit-baseline-middle">
                                                <tbody>
                                                    <tr>
                                                        <td style="vertical-align:bottom"><span style="background-color:#19376b"><img class="blSEcj sc-iRbamj" src="https://cdn2.hubspot.net/hubfs/53/tools/email-signature-generator/icons/link-icon-2x.png" style="background-color:#19376b; display:block; width:13px" /></span></td>
                                                    </tr>
                                                </tbody>
                                            </table>
                                            </td>
                                            <td><a class="sc-gipzik iyhjGb" href="//orionsm.com.br" style="text-decoration: none; color: rgb(0, 0, 0); font-size: 12px;">orionsm.com.br</a></td>
                                        </tr>
                                    </tbody>
                                </table>

                                <table cellpadding="0" cellspacing="0" class="eQYmiW sc-gPEVay" style="font-family:Arial; font-size:small; vertical-align:-webkit-baseline-middle">
                                    <tbody>
                                    </tbody>
                                </table>
                                <br>
                                <a class="sc-fAjcbJ byigni" href="https://wa.me/{fone_formatado}" rel="noopener noreferrer" style="border-width: 6px 12px; border-style: solid; border-color: rgb(25, 55, 107); display: inline-block; background-color: rgb(25, 55, 107); color: rgb(255, 255, 255); font-weight: 700; text-decoration: none; text-align: center; line-height: 40px; font-size: 12px; border-radius: 3px;" target="_blank">Entre em contato comigo pelo whatsapp</a>

                                <table cellpadding="0" cellspacing="0" class="eQYmiW sc-gPEVay" style="font-family:Arial; font-size:small; vertical-align:-webkit-baseline-middle">
                                    <tbody>
                                    </tbody>
                                </table>
                                </td>
                            </tr>
                        </tbody>
                    </table>
                    </td>
                </tr>
            </tbody>
        </table>
            '''


        else:
            html_string = f'''
            <table cellpadding="0" cellspacing="0" class="eQYmiW sc-gPEVay" style="font-family:Arial; font-size:small; vertical-align:-webkit-baseline-middle">
            <tbody>
                <tr>
                    <td>
                    <table cellpadding="0" cellspacing="0" class="eQYmiW sc-gPEVay" style="font-family:Arial; font-size:small; vertical-align:-webkit-baseline-middle">
                        <tbody>
                            <tr>
                                <td style="vertical-align:top">
                                <table cellpadding="0" cellspacing="0" class="eQYmiW sc-gPEVay" style="font-family:Arial; font-size:small; vertical-align:-webkit-baseline-middle">
                                    <tbody>
                                        <tr>
                                            <td style="text-align:center"><img class="bHiaRe sc-cHGsZl" src={foto} style="display:block; max-width:128px; width:130px" /></td>
                                        </tr>
                                        <tr>
                                            <td style="text-align:center">
                                            <table cellpadding="0" cellspacing="0" class="eQYmiW sc-gPEVay" style="display:inline-block; font-family:Arial; font-size:small; vertical-align:-webkit-baseline-middle">
                                                <tbody>
                                                    <tr>
                                                        <td><a class="sc-hzDkRC kpsoyz" href="https://www.facebook.com/OrionAssessoriaImobiliaria" style="display: inline-block; padding: 0px; background-color: rgb(25, 55, 107);"><img alt="facebook" class="ccSRck sc-bRBYWo" src="https://cdn2.hubspot.net/hubfs/53/tools/email-signature-generator/icons/facebook-icon-2x.png" style="background-color:#19376b; display:block; height:24px; max-width:135px" /></a></td>
                                                        <td>&nbsp;</td>
                                                        <td><a class="sc-hzDkRC kpsoyz" href="https://www.linkedin.com/company/%C3%B3rion-assessoria-imobili%C3%A1ria/" style="display: inline-block; padding: 0px; background-color: rgb(25, 55, 107);"><img alt="linkedin" class="ccSRck sc-bRBYWo" src="https://cdn2.hubspot.net/hubfs/53/tools/email-signature-generator/icons/linkedin-icon-2x.png" style="background-color:#19376b; display:block; height:24px; max-width:135px" /></a></td>
                                                        <td>&nbsp;</td>
                                                        <td><a class="sc-hzDkRC kpsoyz" href="https://www.instagram.com/imobiliariaorion/" style="display: inline-block; padding: 0px; background-color: rgb(25, 55, 107);"><img alt="instagram" class="ccSRck sc-bRBYWo" src="https://cdn2.hubspot.net/hubfs/53/tools/email-signature-generator/icons/instagram-icon-2x.png" style="background-color:#19376b; display:block; height:24px; max-width:135px" /></a></td>
                                                        <td>&nbsp;</td>
                                                    </tr>
                                                </tbody>
                                            </table>
                                            </td>
                                        </tr>
                                    </tbody>
                                </table>
                                </td>
                                <td>&nbsp;</td>
                                <td style="vertical-align:middle">
                                <h3 style="margin-left:0; margin-right:0"><strong>{nome}</strong></h3>

                                <p style="margin-left:0; margin-right:0">{cargo}</p>

                                <p style="margin-left:0; margin-right:0">{setor} | &nbsp;&Oacute;rion Assessoria Imobili&aacute;ria

                                <hr />
                                <table cellpadding="0" cellspacing="0" class="eQYmiW sc-gPEVay" style="font-family:Arial; font-size:small; vertical-align:-webkit-baseline-middle;">
                                    <tbody>
                                        <tr>
                                            <td style="vertical-align:middle">
                                            <table cellpadding="0" cellspacing="0" class="eQYmiW sc-gPEVay" style="font-family:Arial; font-size:small; vertical-align:-webkit-baseline-middle;">
                                                <tbody>
                                                    <tr>
                                                        <td style="vertical-align:bottom"><span style="background-color:#19376b"><img class="blSEcj sc-iRbamj" src="https://cdn2.hubspot.net/hubfs/53/tools/email-signature-generator/icons/phone-icon-2x.png" style="background-color:#19376b; display:block; width:13px" /></span></td>
                                                    </tr>
                                                </tbody>
                                            </table>
                                            </td>
                                            <td><a class="sc-gipzik iyhjGb" href="tel:{fone}" style="text-decoration: none; color: rgb(0, 0, 0); font-size: 12px;">{fone}</a></td>
                                        </tr>

                                        <tr style="display: block; margin-bottom: 5px;"><td></td></tr>

                                        <tr>
                                            <td style="vertical-align:middle">
                                            <table cellpadding="0" cellspacing="0" class="eQYmiW sc-gPEVay" style="font-family:Arial; font-size:small; vertical-align:-webkit-baseline-middle">
                                                <tbody>
                                                    <tr>
                                                        <td style="vertical-align:bottom"><span style="background-color:#19376b"><img class="blSEcj sc-iRbamj" src="https://cdn2.hubspot.net/hubfs/53/tools/email-signature-generator/icons/link-icon-2x.png" style="background-color:#19376b; display:block; width:13px" /></span></td>
                                                    </tr>
                                                </tbody>
                                            </table>
                                            </td>
                                            <td><a class="sc-gipzik iyhjGb" href="//orionsm.com.br" style="text-decoration: none; color: rgb(0, 0, 0); font-size: 12px;">orionsm.com.br</a></td>
                                        </tr>
                                    </tbody>
                                </table>


                                </td>
                            </tr>
                        </tbody>
                    </table>
                    </td>
                </tr>
            </tbody>
        </table>
'''

        import streamlit.components.v1 as components
        components.html(html_string, height=600)
        # st.markdown(html_string, unsafe_allow_html=True)
        st.text(html_string)
