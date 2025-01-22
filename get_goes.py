##########################################################################
#    Script para obter dados do GOES-16 tratados                         #
#    Autor: Fábio Lucas Miranda de Sá                                    #
#    Ano: 2024                                                           #
##########################################################################

#### Bibliotecas #########################################################
from datetime import datetime
from module1 import download_goes16

# Função que inicializa o código
download_goes16(datetime(2024,1,1,0), datetime(2024,1,2,0))