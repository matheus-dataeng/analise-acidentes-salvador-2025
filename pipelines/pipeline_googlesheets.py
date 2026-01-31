import pandas as pd
import os
from dotenv import load_dotenv
import psycopg2
import gspread
from gspread_dataframe import set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials

def extract():
	load_dotenv()
	conexao_banco = psycopg2.connect(
		user = os.getenv("DB_USER"),
		password = os.getenv("DB_PASSWORD"),
		host = os.getenv("DB_HOST"),
		port = os.getenv("DB_PORT"),
		database = os.getenv("DB_NAME")
	)
	
	query_sql = '''

		SELECT 
		loc."Id_localizacao",
		loc."UF",
		loc."Municipio",
		temp."Data",
		temp."Horario",
		temp."Visibilidade",
		tip."Tipo_acidente",
		tip."Causa_acidente",
		meteo."Condicao_meteorologica",
		meteo."Probabilidade_acidente",
		via."Pista",
		aci."Pessoas",
		aci."Veiculos",
		aci."Feridos",
		aci."Mortos"
	FROM acidentes AS aci
	LEFT JOIN localizacao AS loc
		ON aci."Id_acidente" = loc."Id_acidente"
	LEFT JOIN tempo AS temp
		ON aci."Id_acidente" = temp."Id_acidente"
	LEFT JOIN tempo_meteorologico AS meteo
		ON aci."Id_acidente" = meteo."Id_acidente"
	LEFT JOIN tipo_acidente as tip
		ON aci."Id_acidente" = tip."Id_acidente"  
	LEFT JOIN  via
		ON aci."Id_acidente" = via."Id_acidente"
	WHERE loc."UF" = 'BA' AND loc."Municipio" = 'Salvador'
	ORDER BY loc."Id_localizacao" ASC, temp."Data" ASC

	'''

	df = pd.read_sql(query_sql, conexao_banco)
	conexao_banco.close()
	return df.to_json(orient="records", date_format="iso")

def load(**context):
	ti = context["ti"]
	data_json = ti.xcom_pull(task_ids="Extract")
	if not data_json:
		raise ValueError ("Xcom vazio, não obteve retorno!")
	df = pd.read_json(data_json, orient="records")
	credential_json = os.getenv("GOOGLE_SHEETS_CREDENTIALS")
	if not credential_json:
		raise ValueError ("Varivel não definida ou incorreta")
	scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive.file"]
	credentials = ServiceAccountCredentials.from_json_keyfile_name(credential_json, scope)
	google_sheets = gspread.authorize(credentials)
	planilha = google_sheets.open_by_key("1gJKI2fWWpC8GjXdlOsuGyUAkeyDZevcEvOtzNyhafoo")
	planilha_guia = planilha.worksheet("Registros")

	set_with_dataframe(
		planilha_guia,
		df,
		include_column_header= True,
		resize=True,
		include_index=False
	)

	print("Carga realizada!")