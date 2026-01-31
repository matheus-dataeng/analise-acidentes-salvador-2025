import pandas as pd
import os 
from dotenv import load_dotenv
from sqlalchemy import create_engine, VARCHAR, Integer, Date, Time, Float

def extract():
    caminho_arquivo = os.getenv("PATH_PRF")
    if not caminho_arquivo:
        raise ValueError("Caminho não reconhecido :/")
    df = pd.read_csv(caminho_arquivo, delimiter= ';', encoding = "latin1")
    return df

def transform(df):
    #DATAFRAME ACIDENTES
    df_acidentes = df[["id", "pessoas", "veiculos", "mortos", "feridos_leves",
                        "feridos_graves", "ilesos", "ignorados", "feridos"]].drop_duplicates()

    colunas_acidentes = {
        "id" : "Id_acidente", 
        "pessoas" : "Pessoas",
        "veiculos" : "Veiculos",
        "mortos" : "Mortos",
        "feridos_leves" : "Feridos_leves",
        "feridos_graves" : "Feridos_graves",
        "ilesos" : "Ilesos",
        "ignorados" : "Ignorados",
        "feridos" : "Feridos"

    }

    df_acidentes.rename(columns= colunas_acidentes, inplace= True)

    #DATAFRAME TEMPO
    df_tempo = df[["id", "data_inversa", "dia_semana", "horario", "fase_dia"]].drop_duplicates()

    df_tempo.rename(columns={ 
        "id" : "Id_acidente", 
        "data_inversa" : "Data",
        "dia_semana" : "Dia",
        "horario" : "Horario",
        "fase_dia" : "Fase_dia",
    }, inplace= True)

    df_tempo['Data'] = pd.to_datetime(df_tempo['Data'], errors = "coerce")
    df_tempo['Horario'] = pd.to_datetime(df_tempo['Horario'], errors = "coerce").dt.time
    df_tempo['Id_tempo'] = df_tempo.index +1

    def visibilidade(status_tempo):
        if status_tempo == "Amanhecer" or status_tempo == "Pleno dia":
            return "Boa visibilidade da pista"
        elif status_tempo == "Anoitecer":
            return "Visibilidade reduzida"
        else:
            return "Baixa visibilidade"

    df_tempo['Visibilidade'] = df_tempo['Fase_dia'].apply(visibilidade)
    df_tempo = df_tempo [["Id_tempo", "Id_acidente", "Data", "Dia", "Horario", "Fase_dia", "Visibilidade"]]

    #DATAFRAME LOCALIZAÇÃO
    df_localizacao = df[["id","uf", "municipio", "br", "km", "latitude", "longitude", "regional", "delegacia", "uop"]].drop_duplicates()

    colunas_localizacao = {
        "id" : "Id_acidente", 
        "uf" : "UF",
        "municipio" : "Municipio",
        "br" : "BR",
        "km" : "KM",
        "latitude" : "Latitude",
        "longitude" : "Longitude",
        "regional" : "Regional",
        "delegacia" : "Delegacia",
        "uop" : "UOP"
    }

    df_localizacao.rename(columns= colunas_localizacao, inplace= True)
    df_localizacao['KM'] = df_localizacao['KM'].str.replace(',', '.').astype(float)
    df_localizacao['Latitude'] = df_localizacao['Latitude'].str.replace(',', '.').astype(float)
    df_localizacao['Longitude'] = df_localizacao['Longitude'].str.replace(',', '.').astype(float)
    df_localizacao['Municipio'] = df_localizacao['Municipio'].str.title()
    df_localizacao['Id_localizacao'] = df_localizacao.index +1
    df_localizacao = df_localizacao[["Id_localizacao", "Id_acidente", "UF", "Municipio", "BR", "KM", "Latitude",
                                    "Longitude", "Regional", "Delegacia", "UOP" ]]

    #DATAFRAME ACIDENTE TIPO
    df_tipo_acidente = df[["id","tipo_acidente", "causa_acidente", "classificacao_acidente"]].drop_duplicates()
    df_tipo_acidente.rename(columns={
        "id" : "Id_acidente", 
        "tipo_acidente" : "Tipo_acidente", 
        "causa_acidente" : "Causa_acidente",
        "classificacao_acidente" : "Classificacao_acidente"
    }, inplace= True)
    df_tipo_acidente['Classificacao_acidente'] = df_tipo_acidente['Classificacao_acidente'].fillna("Sem registro")
    df_tipo_acidente["Id_tipo_acidente"] = df_tipo_acidente.index +1
    df_tipo_acidente = df_tipo_acidente[["Id_tipo_acidente", "Id_acidente", "Tipo_acidente", "Causa_acidente", "Classificacao_acidente"]]

    #DATAFRAME VIA
    df_via = df[["id","tipo_pista", "tracado_via", "sentido_via", "uso_solo"]].drop_duplicates()
    colunas_via = {
        "id" : "Id_acidente", 
        "tipo_pista" : "Pista",
        "tracado_via" : "Tracado_via",
        "sentido_via" : "Sentido_via",
        "uso_solo" : "Uso_solo"
    }
    df_via.rename(columns= colunas_via, inplace=True)
    df_via["Id_via"] = df_via.index +1
    df_via = df_via[["Id_via", "Id_acidente",  "Pista", "Tracado_via", "Sentido_via", "Uso_solo"]]

    #DATAFRAME METEOROLOGIA
    df_tempo_meteorologico = df[["id","condicao_metereologica"]].drop_duplicates()
    df_tempo_meteorologico.rename(columns= {"condicao_metereologica": "Condicao_meteorologica", "id": "Id_acidente", }, inplace= True)
    df_tempo_meteorologico["Id_tempo_condicao"] = df_tempo_meteorologico.index +1

    def probabilidade_acidente(condicao_meteorologica):
        if condicao_meteorologica == "Chuva":
            return "Alta probabilidade de acidente"
        elif condicao_meteorologica == "Garoa/Chuvisco":    
            return "Média probabilidade de acidente"
        else:
            return "Baixa probabilidade de acidente"

    df_tempo_meteorologico['Probabilidade_acidente'] = df_tempo_meteorologico['Condicao_meteorologica'].apply(probabilidade_acidente)
    df_tempo_meteorologico = df_tempo_meteorologico[["Id_tempo_condicao", "Id_acidente", "Condicao_meteorologica", "Probabilidade_acidente"]]

    #DATAFRAME FATO
    df_fato = (
        df_acidentes
        .merge(df_tempo[['Id_acidente', 'Id_tempo']], on= 'Id_acidente', how= "left")
        .merge(df_localizacao[['Id_acidente', 'Id_localizacao']], on= 'Id_acidente', how= "left")
        .merge(df_tipo_acidente[['Id_acidente', 'Id_tipo_acidente']], on= "Id_acidente", how= "left")
        .merge(df_via[['Id_acidente', 'Id_via']], on= "Id_acidente", how= "left")
        .merge(df_tempo_meteorologico[['Id_acidente', 'Id_tempo_condicao']], on= "Id_acidente", how= "left")

    )

    df_fato_acidentes = df_fato[['Id_tempo', 'Id_localizacao', 'Id_tipo_acidente', 'Id_via', 'Id_tempo_condicao', 
                                    "Pessoas","Veiculos","Mortos","Feridos_leves","Feridos_graves","Ilesos","Ignorados","Feridos" ]]

    return (df_acidentes, df_tempo, df_localizacao, df_tipo_acidente, df_via, df_tempo_meteorologico, df_fato_acidentes)

def load(dataframes):
    (df_acidentes, df_tempo, df_localizacao, df_tipo_acidente, df_via, df_tempo_meteorologico, df_fato_acidentes) = dataframes
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT")
    database_name = os.getenv("DB_NAME")

    conexao_banco = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database_name}"
    engine = create_engine(conexao_banco)

    tabela_acidentes = "acidentes"
    tabela_tempo = "tempo"
    tabela_localizacao = "localizacao"
    tabela_tipo_acidente = "tipo_acidente"
    tabela_via = "via"
    tabela_tempo_meteorologico = "tempo_meteorologico"
    tabela_fato_acidentes = "fato_acidentes"

    df_acidentes.to_sql(name= tabela_acidentes, con= engine, index= False, if_exists = 'replace', dtype = {
        "Id_acidente" : Integer,
        "Pessoas" : Integer,
        "Veiculos" : Integer,
        "Mortos" : Integer,
        "Feridos_leves" : Integer,
        "Feridos_graves" : Integer,
        "Ilesos" : Integer,
        "Ignorados" : Integer,
        "Feridos" : Integer
    })

    df_tempo.to_sql(name= tabela_tempo, con= engine, index= False, if_exists= 'replace', dtype= {
        "Id_tempo" : Integer,
        "Id_acidente" : Integer,
        "Data" : Date,
        "Dia" : VARCHAR(100),
        "Horario" : Time,
        "Fase_dia" : VARCHAR(100),
        "Visibilidade" : VARCHAR(100)
    })

    df_localizacao.to_sql(name = tabela_localizacao, con= engine, index= False, if_exists = 'replace', dtype= {
        "Id_localizacao" : Integer,
        "Id_acidente" : Integer,
        "UF" : VARCHAR(5),
        "Municipio" : VARCHAR(100),
        "BR" : Integer,
        "KM" : Float,
        "Latitude" : Float,
        "Longitude" : Float,
        "Regional" : VARCHAR(250),
        "Delegacia" : VARCHAR(150),
        "UOP" : VARCHAR(150)
    })

    df_tipo_acidente.to_sql(name= tabela_tipo_acidente, con = engine, index= False, if_exists= 'replace', dtype={
        "Id_tipo_acidente" : Integer,
        "Id_acidente" : Integer,
        "Tipo_acidente" : VARCHAR(250),
        "Causa_acidente" : VARCHAR(250),
        "Classificacao_acidente" : VARCHAR(250)
    })

    df_via.to_sql(name= tabela_via, con= engine, index = False, if_exists= 'replace', dtype={
        "Id_via" : Integer,
        "Id_acidente" : Integer,
        "Pista" : VARCHAR(230),
        "Tracado_via" : VARCHAR(250),
        "Sentido_via" :VARCHAR(220),
        "Uso_solo" : VARCHAR(210)    
    })

    df_tempo_meteorologico.to_sql(name= tabela_tempo_meteorologico, con= engine, index= False, if_exists= 'replace', dtype={
        "Id_tempo_condicao" : Integer,
        "Id_acidente" : Integer,
        "Condicao_meteorologica" : VARCHAR(150),
        "Probabilidade_acidente" : VARCHAR(150)
    })

    df_fato_acidentes.to_sql(name= tabela_fato_acidentes, con= engine, index= False, if_exists= 'replace', dtype={
        'Id_tempo' : Integer,
        'Id_localizacao' : Integer,
        'Id_tipo_acidente' : Integer,
        'Id_via' : Integer,
        'Id_tempo_condicao' : Integer,
        "Pessoas" : Integer,
        "Veiculos" : Integer,
        "Mortos" : Integer,
        "Feridos_leves" : Integer,
        "Feridos_graves" : Integer,
        "Ilesos" : Integer,
        "Ignorados" : Integer,
        "Feridos" : Integer,
    })

    print("Carga realizada!")




