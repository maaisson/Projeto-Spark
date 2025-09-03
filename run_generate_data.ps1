# Caminho até a raiz do projeto
cd "E:\Projeto-Spark"

# Ativa o ambiente virtual (ajustado para PowerShell no Windows)
. .\.venv\Scripts\Activate.ps1

# Define o PYTHONPATH como raiz
$env:PYTHONPATH = "."

# Executa o script Python
python .\generator\generate_data.py >> .\logs\ga4_cron_windows.log 2>&1
