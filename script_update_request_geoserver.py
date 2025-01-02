import zipfile
import os
import requests
from requests.adapters import HTTPAdapter
import ssl
import geopandas as gpd
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

import logging

logging.basicConfig()
logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)

# Diretórios de entrada e saída
zip_files_dir = "zip_files_dir"  # Diretório onde os arquivos ZIP baixados serão armazenados
extract_dir = "extract_dir"  # Diretório onde os arquivos shapefiles serão extraídos do ZIP
output_dir = "output_dir"  # Diretório onde o shapefile combinado final será salvo

# Configuração de saída
output_filename = "sicar_imoveis_es"  # Nome base do arquivo shapefile combinado
output_shp_filename = f"{output_filename}.shp"  # Nome completo do shapefile combinado (com extensão .shp)
shapefile_path = f"{output_dir}/{output_shp_filename}"  # Caminho completo para o arquivo shapefile combinado

# Configuração do banco de dados
db_uri = "postgresql://postgres:<nome banco de dados>@<host>:<porta>/<usuário>"  # URI de conexão com o banco de dados PostgreSQL
table_name = "imoveis_rurais"  # Nome da tabela no banco de dados onde os dados serão armazenados

# Configuração do GeoServer
geoserver_url = "https://geoserver.car.gov.br/geoserver/sicar/wfs"  # URL para o endpoint WFS do GeoServer

# Configuração da tabela de informações
info_table_name = "informationDatabases"  # Nome da tabela que armazena informações sobre as bases de dados
field_info_update = "LastUpdate"  # Nome da coluna que será atualizada para registrar a última modificação
field_where = "DatabaseName"  # Nome da coluna usada como critério de filtragem para identificar a base de dados
value_field_where = "SICAR"  # Valor de filtragem para a coluna "field_where" (identifica a base de dados específica)

#parametros da requisição WFS
params = {
    "service": "WFS",
    "version": "1.0.0",
    "request": "GetFeature",
    "typename": f"sicar:sicar_imoveis_es",
    "pagingEnabled": "true",
    "preferCoordinatesForWfsT11": "false",
    "outputFormat": "SHAPE-ZIP",
    "sortBy": "cod_imovel",
    "maxFeatures": "1000"
}

class TLSAdapter(HTTPAdapter):
    def init_poolmanager(self, *args, **kwargs):
        # Define o contexto SSL com suporte a TLSv1.2
        context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
        context.set_ciphers("AES256-GCM-SHA384")
        kwargs['ssl_context'] = context
        return super(TLSAdapter, self).init_poolmanager(*args, **kwargs)

# Função para baixar dados usando paginação, descompactar e contar as linhas do shapefile
def download_wfs_data(url, params, zip_files_dir, output_filename, extract_dir):
    # Garante que o diretório zip_files_dir exista
    if not os.path.exists(zip_files_dir):
        os.makedirs(zip_files_dir)

    # Garante que o diretório extract_dir exista
    if not os.path.exists(extract_dir):
        os.makedirs(extract_dir)

    # Configuração de sessão
    session = requests.Session()
    session.mount('https://', TLSAdapter())

    start_index = 0
    total_features = 0

    while True:
        # Atualiza o parâmetro startIndex
        params["startIndex"] = str(start_index)

        try:
            response = session.get(url, params=params, verify=False)
            response.raise_for_status()  # Levanta exceção para erros HTTP

            file_output = f"{zip_files_dir}/{output_filename}_{start_index}.zip"

            # Salva os dados da página atual
            with open(file_output, "wb") as file:
                file.write(response.content)
            print(f"Página com dados de {start_index} salva como '{file_output}'.")

            # Extrai o arquivo ZIP e conta as linhas do shapefile
            lines_count = extract_shapefiles_and_count(file_output, extract_dir, f"{output_filename}_{start_index}")
            total_features += lines_count
            print(f"Arquivo {file_output} contém {lines_count} linhas.")

            # Se a quantidade de dados extraídos for menor que maxFeatures, significa que é a última página
            if lines_count < int(params["maxFeatures"]):
                print("Todos os dados foram baixados.")
                break  # Saímos do loop se a última página for retornada

            # Atualiza o start_index para a próxima página
            start_index += int(params["maxFeatures"])

        except requests.exceptions.RequestException as e:
            print(f"Erro ao acessar o GeoServer: {e}")
            break  # Se ocorrer erro, interrompe a requisição

    print(f"Total de linhas baixadas: {total_features}")

# Função para extrair shapefiles de arquivos ZIP e contar as linhas usando GeoPandas
def extract_shapefiles_and_count(zip_file, extract_dir, output_filename):
    with zipfile.ZipFile(zip_file, 'r') as zip_ref:
        # Cria o diretório base onde os shapefiles serão extraídos
        zip_extract_dir = os.path.join(extract_dir, output_filename)
        if not os.path.exists(zip_extract_dir):
            os.makedirs(zip_extract_dir)

        # Extrai todos os arquivos no diretório desejado
        zip_ref.extractall(zip_extract_dir)
        print(f"Extraído: {zip_file} para {zip_extract_dir}")

        # Encontra os arquivos shapefile (.shp)
        shapefile = None
        for file in zip_ref.namelist():
            if file.endswith('.shp'):
                shapefile = file
                break

        if shapefile:
            shapefile_path = os.path.join(zip_extract_dir, shapefile)
            # Usa GeoPandas para ler o shapefile e contar as linhas
            gdf = gpd.read_file(shapefile_path)
            return len(gdf)  # Retorna o número de linhas (features) do shapefile
        else:
            print(f"Shapefile não encontrado no arquivo {zip_file}")
            return 0

# Função para combinar shapefiles extraídos em um único diretório
def combine_shapefiles(extract_dir, output_dir, output_shp_filename):
    # Garante que o diretório de saída exite
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Lista de GeoDataFrames para armazenar os shapefiles carregados
    gdf_list = []

    # Itera por todas as subpastas no diretório de extração
    for root, dirs, files in os.walk(extract_dir):
        for file in files:
            # Verifica se o arquivo é um shapefile
            if file.endswith(".shp"):
                file_path = os.path.join(root, file)
                try:
                    # Carrega o shapefile como um GeoDataFrame
                    gdf = gpd.read_file(file_path)
                    gdf_list.append(gdf)
                    print(f"Carregado: {file_path}")
                except Exception as e:
                    print(f"Erro ao carregar {file_path}: {e}")

    # Combina todos os GeoDataFrames em um único
    if gdf_list:
        combined_gdf = gpd.GeoDataFrame(pd.concat(gdf_list, ignore_index=True))
        combined_gdf.to_file(f"{output_dir}/{output_shp_filename}")
        print(f"Shapefiles combinados salvos em: {output_shp_filename}")
    else:
        print("Nenhum shapefile encontrado para combinar.")


def save_shapefile_to_postgres(
        shapefile_path, db_uri, table_name, info_table_name, field_info_update, field_where, value_field_where):
    # Cria uma conexão com o banco de dados PostgreSQL
    engine = create_engine(db_uri)

    try:
        # Verifica se a tabela já existe e, se existir, a descarta
        with engine.connect() as connection:
            drop_table_query = text(f'DROP TABLE IF EXISTS "{table_name}";')
            connection.execute(drop_table_query)
            connection.commit()
            print(f"Tabela '{table_name}' descartada (se existia).")

        # Carrega o shapefile usando GeoPandas
        gdf = gpd.read_file(shapefile_path)

        # Adiciona o campo `gid` ao GeoDataFrame para ser usado como chave primária
        gdf["gid"] = range(1, len(gdf) + 1)

        # Salva o GeoDataFrame como uma tabela no banco de dados PostgreSQL (PostGIS)
        gdf.to_postgis(name=table_name, con=engine, if_exists='replace', index=False)
        print(f"Shapefile '{shapefile_path}' salvo na tabela '{table_name}' no banco de dados.")

        # Atualiza a tabela de informações sobre as bases de dados
        with engine.connect() as connection:
            update_info_table = text(
                f"""
                UPDATE public."{info_table_name}"
                SET "{field_info_update}" = NOW()
                WHERE "{field_where}" ilike '{value_field_where}';
                """
            )
            connection.execute(update_info_table, )
            connection.commit()
            print(f"Tabela '{info_table_name}' atualizada com sucesso.")

    except SQLAlchemyError as e:
        print(f"Erro ao interagir com o banco de dados: {e}")
    except Exception as e:
        print(f"Erro inesperado: {e}")

download_wfs_data(geoserver_url, params, zip_files_dir, output_filename, extract_dir)
combined_shapefile = combine_shapefiles(extract_dir, output_dir, output_shp_filename)  # Combina os shapefiles
save_shapefile_to_postgres(shapefile_path, db_uri, table_name, info_table_name, field_info_update, field_where, value_field_where)
