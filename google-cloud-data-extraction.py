# Google BigQuery
from google.cloud import bigquery

def query_bigquery(project_id, query):
    client = bigquery.Client(project=project_id)
    query_job = client.query(query)
    results = query_job.result()
    return [dict(row) for row in results]

# Exemple d'utilisation
bigquery_data = query_bigquery('your-project-id', '''
    SELECT date, total_sales, product_category
    FROM `your-dataset.your-table`
    WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
''')

# Google Cloud Storage
from google.cloud import storage

def list_blobs_with_prefix(bucket_name, prefix):
    storage_client = storage.Client()
    blobs = storage_client.list_blobs(bucket_name, prefix=prefix)
    return [blob.name for blob in blobs]

# Exemple d'utilisation
gcs_files = list_blobs_with_prefix('your-bucket-name', 'sales_data/')

# Google Pub/Sub
from google.cloud import pubsub_v1

def pull_messages(project_id, subscription_id, max_messages):
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(project_id, subscription_id)
    response = subscriber.pull(
        request={"subscription": subscription_path, "max_messages": max_messages}
    )
    return [msg.message.data for msg in response.received_messages]

# Exemple d'utilisation
pubsub_messages = pull_messages('your-project-id', 'your-subscription-id', 100)

# Google Cloud Firestore
from google.cloud import firestore

def query_firestore(collection_name, field, operator, value):
    db = firestore.Client()
    docs = db.collection(collection_name).where(field, operator, value).stream()
    return [doc.to_dict() for doc in docs]

# Exemple d'utilisation
firestore_data = query_firestore('products', 'category', '==', 'electronics')

# Google Analytics Data API (GA4)
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    DateRange,
    Dimension,
    Metric,
    RunReportRequest,
)

def get_ga4_report(property_id):
    client = BetaAnalyticsDataClient()
    request = RunReportRequest(
        property=f"properties/{property_id}",
        dimensions=[Dimension(name="date")],
        metrics=[Metric(name="activeUsers"), Metric(name="sessions")],
        date_ranges=[DateRange(start_date="7daysAgo", end_date="today")],
    )
    response = client.run_report(request)
    return [
        {
            "date": row.dimension_values[0].value,
            "active_users": row.metric_values[0].value,
            "sessions": row.metric_values[1].value,
        }
        for row in response.rows
    ]

# Exemple d'utilisation
ga4_data = get_ga4_report('your-ga4-property-id')
