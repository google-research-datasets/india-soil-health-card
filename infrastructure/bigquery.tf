resource "google_bigquery_dataset" "shc_cards" {
  dataset_id                  = "soilhealth"
  friendly_name               = "soilhealth"
  description                 = "Soil Health"
  location                    = "EU"
}

resource "google_bigquery_dataset_iam_member" "workflow_role" {
  dataset_id = google_bigquery_dataset.shc_cards.dataset_id
  role       = "roles/bigquery.dataEditor"
  member   = "serviceAccount:${google_service_account.workflow_sa.email}"
}
resource "google_bigquery_dataset_iam_member" "cr_role" {
  dataset_id = google_bigquery_dataset.shc_cards.dataset_id
  role       = "roles/bigquery.dataEditor"
  member   = "serviceAccount:${google_service_account.run_sa.email}"
}

resource "google_bigquery_table" "default" {
  dataset_id = google_bigquery_dataset.shc_cards.dataset_id
  table_id   = "soil_health_cards"

  schema = <<EOF
[
    {
        "name": "id",
        "type": "STRING",
        "mode": "NULLABLE"
    },
    {
        "name": "sample",
        "type": "STRING",
        "mode": "REQUIRED"
    },
    {
        "name": "sr_no",
        "type": "INT64",
        "mode": "REQUIRED"
    },
    {
        "name": "state_id",
        "type": "INT64",
        "mode": "REQUIRED"
    },
    {
        "name": "district_id",
        "type": "INT64",
        "mode": "REQUIRED"
    },
    {
        "name": "subdistrict_id",
        "type": "INT64",
        "mode": "REQUIRED"
    },
    {
        "name": "village_id",
        "type": "INT64",
        "mode": "REQUIRED"
    },
    {
        "name": "soil_tests",
        "type": "RECORD",
        "mode": "REPEATED",
        "fields": [
            {
                "name": "sr_no",
                "type": "STRING",
                "mode": "NULLABLE"
            },
            {
                "name": "parameter",
                "type": "STRING",
                "mode": "NULLABLE"
            },
            {
                "name": "value",
                "type": "STRING",
                "mode": "NULLABLE"
            },
            {
                "name": "unit",
                "type": "STRING",
                "mode": "NULLABLE"
            },
            {
                "name": "rating",
                "type": "STRING",
                "mode": "NULLABLE"
            },
            {
                "name": "normal_level",
                "type": "STRING",
                "mode": "NULLABLE"
            }
        ]
    },
    {
        "name": "soil_health_card_number",
        "type": "STRING",
        "mode": "NULLABLE"
    },
    {
        "name": "validity",
        "type": "STRING",
        "mode": "NULLABLE"
    },
    {
        "name": "soil_sample_details",
        "type": "RECORD",
        "mode": "NULLABLE",
        "fields": [
            {
                "name": "date_of_sample_collection",
                "type": "DATE",
                "mode": "NULLABLE"
            },
            {
                "name": "survey_no",
                "type": "STRING",
                "mode": "NULLABLE"
            },
            {
                "name": "farm_size",
                "type": "STRING",
                "mode": "NULLABLE"
            },
            {
                "name": "geo_position_string",
                "type": "STRING",
                "mode": "NULLABLE"
            },
            {
                "name": "geo_position",
                "type": "GEOGRAPHY",
                "mode": "NULLABLE"
            }
        ]
    }
]
EOF

}

resource "google_bigquery_table" "states" {
  dataset_id = google_bigquery_dataset.shc_cards.dataset_id
  table_id   = "states"

  schema = <<EOF
[
    {
        "name": "id",
        "type": "INTEGER",
        "mode": "NULLABLE"
    },
    {
        "name": "name",
        "type": "STRING",
        "mode": "REQUIRED"
    }
]
EOF
}

resource "google_bigquery_table" "districts" {
  dataset_id = google_bigquery_dataset.shc_cards.dataset_id
  table_id   = "districts"

  schema = <<EOF
[
    {
        "name": "id",
        "type": "INTEGER",
        "mode": "NULLABLE"
    },
    {
        "name": "state_id",
        "type": "INTEGER",
        "mode": "NULLABLE"
    },
    {
        "name": "name",
        "type": "STRING",
        "mode": "REQUIRED"
    }
]
EOF
}

resource "google_bigquery_table" "subdistricts" {
  dataset_id = google_bigquery_dataset.shc_cards.dataset_id
  table_id   = "subdistricts"

  schema = <<EOF
[
    {
        "name": "id",
        "type": "INTEGER",
        "mode": "NULLABLE"
    },
    {
        "name": "district_id",
        "type": "INTEGER",
        "mode": "NULLABLE"
    },
    {
        "name": "name",
        "type": "STRING",
        "mode": "REQUIRED"
    }
]
EOF
}

resource "google_bigquery_table" "villages" {
  dataset_id = google_bigquery_dataset.shc_cards.dataset_id
  table_id   = "villages"

  schema = <<EOF
[
    {
        "name": "id",
        "type": "INTEGER",
        "mode": "NULLABLE"
    },
    {
        "name": "subdistrict_id",
        "type": "INTEGER",
        "mode": "NULLABLE"
    },
    {
        "name": "name",
        "type": "STRING",
        "mode": "REQUIRED"
    }
]
EOF
}

/*
resource "google_eventarc_trigger" "primary" {
    name = "gcs-ingestion"
    location = var.region
    matching_criteria {
        attribute = "type"
        value = "google.cloud.pubsub.topic.v1.messagePublished"
    }
    service_account = google_service_account.workflow_sa.email
    destination {
        cloud_run_service {
            service = google_cloud_run_service.anthrokrishi-scraper.name
            region = var.region
        }
    }
}*/