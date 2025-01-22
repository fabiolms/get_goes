#### Bibliotecas #########################################################
import os
from goes2go import GOES
from datetime import datetime, timedelta
import re
import xarray as xr
##########################################################################

def download_goes16(start_date, end_date):

    """

    Inicializa todo o processo de download e compactação dos dados

    Args:
        start_date (datetime): Data inicial.
        end_date (datetime): Data final.

    Returns:
        arquivo concatenado .nc
    """

    download_goes16_data(start_date, end_date)

    concat_netcdf_in_chunks()

    concat_netcdf_final()





def download_goes16_data(start_date, end_date, download_dir='files'):

    """

    Retorna todos os arquivos .nc da data escolhida

    Args:
        start_date (datetime): Data inicial.
        end_date (datetime): Data final.

    Returns:
        arquivos .nc
    """
    
    # Obtém o caminho absoluto do diretório onde o script está localizado
    script_dir = os.path.dirname(os.path.abspath(__file__))

    # Define o diretório de trabalho para o diretório do script
    download_dir = os.path.join(script_dir, download_dir)

    # Criar o diretório de download se não existir
    os.makedirs(download_dir, exist_ok=True)

    # Especificações para o download
    G = GOES(satellite=16, product="ABI-L2-DSRF", domain='F')

    start = start_date
    mid = start_date
    end = end_date

    while start != end:

        try:
            df = G.timerange(start, end, save_dir=download_dir)
            start = end

        except FileNotFoundError as e:
            list = re.split(r'/', str(e))
            base = datetime(int(list[2]), 1, 1, int(list[4]))
            mid = base + timedelta(days=int(list[3]) - 1)
        
            print(f"O horário {mid} não contêm dados!!")

            if start == mid:
                start = mid + timedelta(hours=1)

            else:
                mid_1 = mid - timedelta(hours=1)

                # Verifica se o intervalo entre start e mid_1 é válido
                if start < mid_1:  # Apenas tenta se o intervalo for válido
                    try:
                        df = G.timerange(start, mid_1, save_dir=download_dir)
                        print(f"Download entre {start} e {mid_1} foi concluído!")
                    except FileNotFoundError:
                        print(f"Nenhum dado disponível entre {start} e {mid_1}. Pulando intervalo.")
                else:
                    print(f"Intervalo inválido entre {start} e {mid_1}. Pulando.")

                # Avance para evitar loops infinitos
                start = mid + timedelta(hours=1)
        
    print("Download finalizado com sucesso.")





def concat_netcdf_in_chunks(input_dir=r'files\noaa-goes16\ABI-L2-DSRF', bloco_tamanho=50, output_dir='chunk_goes'):

    """

    Realiza a primeira etapa de concatenação em pacotes

    Args:

    Returns:
        arquivos agrupados .nc
    """

    # Obtém o caminho absoluto do diretório onde o script está localizado
    script_dir = os.path.dirname(os.path.abspath(__file__))

    # Define o diretório de trabalho para o diretório do script
    input_dir = os.path.join(script_dir, input_dir)
    output_dir = os.path.join(script_dir, output_dir)

    os.makedirs(output_dir, exist_ok=True)

    # Listar todos os arquivos NetCDF no diretório e subdiretórios
    list = []
    for root, dirs, files in os.walk(input_dir):
        for file in files:
            if file.endswith(".nc"):
                list.append(os.path.join(root, file))
    
    # Ordenar os arquivos para garantir a ordem temporal
    list.sort()
    print(list)

    # Concatenar em blocos
    chunk = []
    count = 0
    
    for i, file in enumerate(list):
        
        # Abrir dataset e adicionar ao bloco
        ds = xr.open_mfdataset(file)
        chunk.append(ds)

        # Se atingir o tamanho do bloco ou for o último arquivo, concatenar e salvar
        if (i + 1) % bloco_tamanho == 0 or i == len(list) - 1:
            # Concatenar os datasets do bloco
            print(f"Concatenando e salvando bloco {count}...")
            ds_concat = xr.concat(chunk, dim='time')
            
            # Gerar o caminho de saída para o bloco
            chunk_output_path = os.path.join(output_dir, f"output_parte_{count}.nc")
            ds_concat.to_netcdf(chunk_output_path)
            print(f"Bloco {count} salvo em: {chunk_output_path}")
            
            # Resetar o bloco
            chunk = []
            count += 1

    print("Primeira concatenação realizada com sucesso.")





def concat_netcdf_final(input_dir='chunk_goes', output_path='output_final.nc'):

    """

    Realiza a concatenação final dos arquivos

    Args:

    Returns:
        arquivo final .nc
    """

    # Obtém o caminho absoluto do diretório onde o script está localizado
    script_dir = os.path.dirname(os.path.abspath(__file__))

    # Define o diretório de trabalho para o diretório do script
    input_dir = os.path.join(script_dir, input_dir)
    output_path = os.path.join(script_dir, output_path)

    # Listar todos os arquivos intermediários no diretório 'chunks_goes'
    arquivos_intermediarios = [f for f in os.listdir(input_dir) if f.startswith('output_parte_') and f.endswith('.nc')]
    
    # Função para extrair o número da parte do arquivo
    def extrair_numero(file_name):
        return int(re.search(r'output_parte_(\d+)\.nc', file_name).group(1))
    
    # Ordenar os arquivos intermediários numericamente
    arquivos_intermediarios.sort(key=extrair_numero)

    # Carregar e concatenar todos os arquivos intermediários com Dask
    list = []
    
    for i, file in enumerate(arquivos_intermediarios):
        print(f"Concatenando arquivo intermediário {i + 1}/{len(arquivos_intermediarios)}: {file}")
        ds = xr.open_mfdataset(os.path.join(input_dir, file), chunks={"time": 100})  # Carregar com chunks
        list.append(ds)
    
    # Concatenar os arquivos usando Dask, com compat='no_conflicts' para evitar incompatibilidades
    ds_final = xr.concat(list, dim='time', compat='no_conflicts')
    
    # Salvar o arquivo final concatenado com Dask
    ds_final.to_netcdf(output_path, engine="h5netcdf", compute=True)

    print(f"Arquivo final concatenado salvo como: {output_path}")
