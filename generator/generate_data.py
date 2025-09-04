from faker import Faker
import pandas as pd
import random
from datetime import timedelta
from config import settings

fake = Faker()

RAW_COLUMNS_VARIANTS = {
        "session_id": ["id_sessao", "sessionId"],
        "user_id": ["id_usuario", "userId"],
        "source": ["origem", "fonte"],
        "medium": ["meio", "canal"],
        "campaign": ["campanha", "ad_campaign"],
        "device_category": ["dispositivo", "deviceCategory"],
        "session_start": ["inicioSessao", "startTime"],
        "session_end": ["fimSessao", "endTime"],
        "page_views": ["visualizacoesPagina", "views"],
        "transactions": ["transacoes", "compras"],
        "revenue": ["receita", "valor"]
}


def generate_fake_ga4_data(n):
    data = []
    for _ in range(n):
        session_start = fake.date_time_between(start_date=settings.DATE_START, end_date=settings.DATE_END)
        session_duration = random.randint(10, 600)  # seconds
        
        row = {
            "session_id": fake.uuid4(),
            "user_id": fake.uuid4(),
            "source": fake.random_element(elements=('google', 'facebook', 'instagram', 'direct', 'email')),
            "medium": fake.random_element(elements=('organic', 'cpc', 'referral', 'none')),
            "campaign": fake.bs(),
            "device_category": fake.random_element(elements=('desktop', 'mobile', 'tablet')),
            "session_start": session_start,
            "session_end": session_start + timedelta(seconds=session_duration),
            "page_views": random.randint(1, 10),
            "transactions": random.randint(0, 2),
            "revenue": round(random.uniform(0, 500), 2)            
        }
        
        if random.random() < 0.1:
            col = random.choice(list(row.keys()))
            row[col] = None
        
        data.append(row)
        
    return pd.DataFrame(data)

if __name__ == "__main__":
    n = random.randint(settings.N_RECORDS_MIN, settings.N_RECORDS_MAX)
    df = generate_fake_ga4_data(n)
    
    file_datetime = fake.date_time_between(start_date=settings.DATE_START, end_date=settings.DATE_END)
    timestamp = file_datetime.strftime("%Y%m%d%H%M%S")
    
    year = file_datetime.strftime("%Y")
    month = file_datetime.strftime("%m")
    day = file_datetime.strftime("%d")   

    parquet_dir = settings.RAW_PATH / "parquet" / year / month / day
    csv_dir = settings.RAW_PATH / "csv" / year / month / day
    json_dir = settings.RAW_PATH / "json" / year / month / day
    
    parquet_dir.mkdir(parents=True, exist_ok=True)
    csv_dir.mkdir(parents=True, exist_ok=True)
    json_dir.mkdir(parents=True, exist_ok=True)

    #parquet
    parquet_path = parquet_dir / f"ga_data_{timestamp}.parquet"
    df.to_parquet(parquet_path, index=False)
    
    #CSV
    df_csv = df.rename(columns={col: random.choice(syns) for col, syns in RAW_COLUMNS_VARIANTS.items()})
    csv_path = csv_dir / f"ga_data_{timestamp}.csv"
    df_csv.to_csv(csv_path, index=False, sep=";")

    #JSON
    json_path = json_dir / f"ga_data_{timestamp}.json"
    df.to_json(json_path, orient="records", lines=True, force_ascii=False)

    print(f"[OK] Gerado {n} registros em:")
    print(f" - Parquet: {parquet_path}")
    print(f" - CSV:     {csv_path}")
    print(f" - JSON:    {json_path}")