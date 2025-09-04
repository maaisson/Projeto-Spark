from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, LongType

# Mapeia todos os nomes possíveis para um nome padrão (canônico)
COLUMN_MAPPING = {
    "id_sessao": "session_id", "sessionId": "session_id", "session_id": "session_id",
    "id_usuario": "user_id", "userId": "user_id", "user_id": "user_id",
    "origem": "source", "fonte": "source", "source": "source",
    "meio": "medium", "canal": "medium", "medium": "medium",
    "campanha": "campaign", "ad_campaign": "campaign", "campaign": "campaign",
    "dispositivo": "device_category", "deviceCategory": "device_category", "device_category": "device_category",
    "inicioSessao": "session_start", "startTime": "session_start", "session_start": "session_start",
    "fimSessao": "session_end", "endTime": "session_end", "session_end": "session_end",
    "visualizacoesPagina": "page_views", "views": "page_views", "page_views": "page_views",
    "transacoes": "transactions", "compras": "transactions", "transactions": "transactions",
    "receita": "revenue", "valor": "revenue", "revenue": "revenue"
}

# Define o schema final da camada Bronze com os tipos corretos
BRONZE_SCHEMA = StructType([
    StructField("session_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("source", StringType(), True),
    StructField("medium", StringType(), True),
    StructField("campaign", StringType(), True),
    StructField("device_category", StringType(), True),
    StructField("session_start", LongType(), True), # Lendo como Long para tratar depois
    StructField("session_end", LongType(), True),   # Lendo como Long para tratar depois
    StructField("page_views", IntegerType(), True),
    StructField("transactions", IntegerType(), True),
    StructField("revenue", DoubleType(), True),
    StructField("source_file", StringType(), True)
])