import mysql.connector
import pandas as pd
import streamlit as st


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
        import datetime
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
        import datetime
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
    
