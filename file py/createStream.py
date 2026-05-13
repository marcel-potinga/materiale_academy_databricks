

from ast import main
import json
import time
import io
import pandas as pd
from azure.eventhub import EventHubProducerClient, EventData
from azure.eventhub import EventData
from azure.storage.filedatalake import DataLakeServiceClient
# INSTALLATE LE LIBRERIE NECESSARIARIE PRIMA DI ESEGUIRE QUESTO SCRIPT:
# pip install azure-eventhub azure-storage-file-datalake pandas
#NON PUSHATE QUESTO SCRIPT SU GITHUB, TENETELO IN LOCALE E PROTEGGETE LE VOSTRE CREDENZIALI!!!

#Config
STORAGE_ACCOUNT   = "" #nome dell'account di archiviazione ADLS
ACCESS_KEY        = "" #chiave di accesso all'account di archiviazione ADLS
CONTAINER         = "" #nome del container ADLS dove è presente
CSV_PATH          = "transactions/Fraud.csv" #percorso del file CSV all'interno del container ADLS (es: "cartella/file.csv")

CONNECTION_STRING = "" #stringa di connessione all'Event Hub (puoi trovarla nel portale Azure, nella sezione "Chiavi di accesso" dell'Event Hub) AGGIUNGETE EntityPath=transactions alla fine della stringa di connessione se non è già presente, es: "Endpoint=sb://myeventhub.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=abc123;EntityPath=transactions"
EVENT_HUB_NAME    = "transactions" #nome dell'Event Hub (deve corrispondere al nome dell'Event Hub creato in Azure, es: "transactions")

DELAY_SECONDS     = 2  # intervallo tra un evento e l'altro
BATCH_SIZE        = 1  # quante transazioni per batch

def load_csv_from_adls() -> pd.DataFrame: 
    """Legge il CSV delle transazioni direttamente da ADLS."""
    service = DataLakeServiceClient(
        account_url=f"https://{STORAGE_ACCOUNT}.dfs.core.windows.net",
        credential=ACCESS_KEY
    )
    file_client = (
        service
        .get_file_system_client(CONTAINER)
        .get_file_client(CSV_PATH)
    )
    content = file_client.download_file().readall()
    df = pd.read_csv(io.BytesIO(content))
    print(f"CSV caricato da ADLS: {len(df)} righe, colonne: {list(df.columns)}")
    return df


def build_payload(row: dict) -> str:
    """Arricchisce la riga con metadata di invio e serializza in JSON."""
    row["event_timestamp"] = time.strftime("%Y-%m-%dT%H:%M:%SZ")
    row["producer"]        = "bank_transactions_simulator"
    # Converti eventuali nan in none
    cleaned = {k: (None if pd.isna(v) else v) for k, v in row.items()}
    return json.dumps(cleaned)


def send_transactions(df):
    producer = EventHubProducerClient.from_connection_string(
        conn_str=CONNECTION_STRING,
        eventhub_name=EVENT_HUB_NAME
    )

    with producer:
        total = len(df)
        sent = 0
        errors = 0

        for start in range(0, total, BATCH_SIZE):
            chunk = df.iloc[start : start + BATCH_SIZE]
            try:
                batch = producer.create_batch()
                for _, row in chunk.iterrows():
                    payload = build_payload(row.to_dict())
                    batch.add(EventData(payload))
                producer.send_batch(batch)
                sent += len(chunk)
            except Exception as e:
                errors += len(chunk)
                print(f"  [ERRORE] batch {start}-{start+BATCH_SIZE}: {e}")

            if sent % 100 == 0:
                print(f"  → {sent}/{total} inviate | errori: {errors}")

            time.sleep(DELAY_SECONDS)

    print(f"\nCompletato! Inviati: {sent} | Errori: {errors}")

df = load_csv_from_adls()
send_transactions(df)


