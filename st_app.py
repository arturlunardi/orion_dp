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
    a = '{:,.2f}'.format(float(my_value))
    b = a.replace(',','v')
    c = b.replace('.',',')
    return c.replace('v','.')


if check_password("password"):

    # ----------- Global Sidebar ---------------

    condition = st.sidebar.selectbox(
        "Selecione a Aba",
        ("Home", "Melhores Imóveis", "Previsão de Valor de Aluguel")
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

            df_orion = df_oferta.loc[df_oferta['imobiliaria'] == 'Órion'].drop(columns=['latitude', 'longitude', 'alugado'])
            df_outros = df_oferta.loc[df_oferta['imobiliaria'] != 'Órion'].drop(columns=['latitude', 'longitude', 'alugado'])
        
            return df_oferta, df_orion, df_outros

        # Get Datasets
        with st.spinner('Carregando os dados...'):
            df_oferta, df_orion, df_outros = get_datasets()

        # SideBar Filters
        st.sidebar.subheader("Filtros")

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
        
        filter_garagem = {'vagas_garagem': st.sidebar.text_input(
        label='Vagas Garagem',
        value="Todos",
        placeholder="Todos"
        # step=1 
        )}

        filter_button = st.sidebar.checkbox('Filtrar', help='Aperte o botão para filtrar os dados conforme os filtros selecionados')

        filters = [filter_bairro, filter_tipologia, filter_dormitorios, filter_garagem]

        # Display Datasets
        if filter_button:
            for filter in filters:
                key = [i for i in filter.keys()][0]
                value = filter.get(key)
                if value != 'Todos' and value != '':
                    if value.isdigit():
                        df_orion = df_orion.loc[df_orion[key] == int(value)]
                        df_oferta = df_oferta.loc[df_oferta[key] == int(value)]
                    else:
                        df_orion = df_orion.loc[df_orion[key] == value]
                        df_outros = df_outros.loc[df_outros[key] == value]
                elif value == '':
                    st.sidebar.warning(f"Por favor, selecione um valor válido no campo {key.title().replace('_', ' ')}")
                    # st.stop()
                    
            st.header('Imóveis da Órion')
            st.dataframe(df_orion.sort_values(by='predict_proba', ascending=False).reset_index(drop=True))

            st.header('Imóveis Concorrentes')
            st.dataframe(df_outros.sort_values(by='predict_proba', ascending=False).reset_index(drop=True))
        else:
            st.header('Imóveis da Órion')
            st.dataframe(df_orion.sort_values(by='predict_proba', ascending=False).reset_index(drop=True))

            st.header('Imóveis Concorrentes')
            st.dataframe(df_outros.sort_values(by='predict_proba', ascending=False).reset_index(drop=True))

    # ------------- Predict Rent ------------------------

    elif condition == "Previsão de Valor de Aluguel":
        if check_password("predict_aluguel_password"):
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


