locals {
  apis = ["iam.googleapis.com", "run.googleapis.com", "workflows.googleapis.com", "sqladmin.googleapis.com"]
}

data "google_project" "project" {
  project_id = var.project_id
}

resource "google_project_service" "project" {
  for_each = toset(local.apis)
  project  = data.google_project.project.project_id
  service  = each.key

  disable_on_destroy = false
}

resource "google_service_account" "run_sa" {
  account_id = "anthro-run"
}

resource "google_service_account" "workflow_sa" {
  account_id = "anthro-workflows"
}

resource "google_storage_bucket" "shc-bucket" {
  name          = "grotz-anthrokrishi-shcs"
  location      = "EU"
  force_destroy = true

  uniform_bucket_level_access = true
}

resource "google_project_iam_member" "logWriterCloudRun" {
  project = var.project_id
  role    = "roles/logging.logWriter"
  member  = "serviceAccount:${google_service_account.run_sa.email}"
}

resource "google_project_iam_member" "logWriterWorkflow" {
  project = var.project_id
  role    = "roles/logging.logWriter"
  member  = "serviceAccount:${google_service_account.workflow_sa.email}"
}

resource "google_project_iam_member" "workflowEnqueuer" {
  project = var.project_id
  role    = "roles/cloudtasks.enqueuer"
  member  = "serviceAccount:${google_service_account.workflow_sa.email}"
}

resource "google_service_account_iam_member" "workflowCanUseItself" {
  service_account_id = google_service_account.workflow_sa.name
  role               = "roles/iam.serviceAccountUser"
  member  = "serviceAccount:${google_service_account.workflow_sa.email}"
}

resource "google_service_account_iam_member" "workflowCanUseItselfToken" {
  service_account_id = google_service_account.workflow_sa.name
  role               = "roles/iam.serviceAccountTokenCreator"
  member  = "serviceAccount:${google_service_account.workflow_sa.email}"
}

resource "google_storage_bucket_iam_member" "member" {
  bucket = google_storage_bucket.shc-bucket.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.run_sa.email}"
}

resource "google_storage_bucket_iam_member" "member_reader" {
  bucket = google_storage_bucket.shc-bucket.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.workflow_sa.email}"
}

resource "google_workflows_workflow" "shc_scraping" {
  name            = "shc-scraping"
  region          = var.region
  description     = "SHC Data Scraping Workflow"
  service_account = google_service_account.workflow_sa.id
  source_contents = templatefile("${path.module}/workflow.yaml",
    {
      cloud_run_url = google_cloud_run_service.anthrokrishi-scraper.status[0].url,
      task_sa = google_service_account.workflow_sa.email,
      queue_name = google_cloud_tasks_queue.states.id
    }
  )

  depends_on = [
    google_cloud_run_service.anthrokrishi-scraper,
    google_project_service.project,
    google_storage_bucket.shc-bucket
  ]
}

resource "google_workflows_workflow" "shc_extracting" {
  name            = "shc-extracting"
  region          = var.region
  description     = "Extract data from SHCs and send to BigQuery"
  service_account = google_service_account.workflow_sa.id
  source_contents = templatefile("${path.module}/workflow-extract-all-files.yaml",
    {
      cloud_run_url = google_cloud_run_service.anthrokrishi-scraper.status[0].url,
      shc_bucket = google_storage_bucket.shc-bucket.name
    }
  )

  depends_on = [
    google_cloud_run_service.anthrokrishi-scraper,
    google_project_service.project,
    google_storage_bucket.shc-bucket
  ]
}

resource "google_workflows_workflow" "shc_scraping_village" {
  name            = "shc-scraping-village"
  region          = var.region
  description     = "SHC Data Scraping Workflow for Village"
  service_account = google_service_account.workflow_sa.id
  source_contents = templatefile("${path.module}/workflow-village.yaml",
    {
      cloud_run_url = google_cloud_run_service.anthrokrishi-scraper.status[0].url,
      cloud_run_url_async = google_cloud_run_service.anthrokrishi-scraper-pubsub.status[0].url,
      task_sa = google_service_account.workflow_sa.email,
      queue_name = google_cloud_tasks_queue.default.id
    }
  )

  depends_on = [
    google_cloud_run_service.anthrokrishi-scraper,
    google_project_service.project,
    google_storage_bucket.shc-bucket
  ]
}

resource "google_workflows_workflow" "shc_scraping_states" {
  name            = "shc-scraping-states"
  region          = var.region
  description     = "SHC Data Scraping Workflow for Scraping each state"
  service_account = google_service_account.workflow_sa.id
  source_contents = templatefile("${path.module}/workflow-ingest-states.yaml",
    {
      cloud_run_url = google_cloud_run_service.anthrokrishi-scraper.status[0].url,
      task_sa = google_service_account.workflow_sa.email,
      queue_name = google_cloud_tasks_queue.villages.id
    }
  )

  depends_on = [
    google_cloud_run_service.anthrokrishi-scraper,
    google_project_service.project,
    google_storage_bucket.shc-bucket
  ]
}


resource "google_project_iam_member" "wfExecutor" {
  project = var.project_id
  role    = "roles/workflows.invoker"
  member  = "serviceAccount:${google_service_account.workflow_sa.email}"
}


locals {
  container_folder_hash = sha1(join("", [for f in fileset("../container", "*") : filesha1("../container/${f}")]))
}

resource "null_resource" "docker_image" {
  provisioner "local-exec" {
    command = <<EOT
gcloud auth configure-docker
docker buildx build --push -t gcr.io/${var.project_id}/scraper:${local.container_folder_hash} ../container
EOT
  }

  triggers = {
    dir_sha1 = local.container_folder_hash
  }
}

resource "google_cloud_run_service" "anthrokrishi-scraper" {
  provider = google-beta
  name     = "scraper"
  location = var.region
  project  = var.project_id

  metadata {
    annotations = {
      "run.googleapis.com/ingress" : "all"
    }
  }

  template {
    spec {
      service_account_name  = google_service_account.run_sa.email
      timeout_seconds       = 900
      container_concurrency = 1
      containers {
        image = "gcr.io/${var.project_id}/scraper:${local.container_folder_hash}"
        ports {
          name           = "http1"
          container_port = 8080
        }
        resources {
          limits = {
            cpu    = "4000m"
            memory = "8Gi"
          }
        }
        env {
          name  = "GCS_BUCKET"
          value = google_storage_bucket.shc-bucket.name
        }
        env {
          name  = "BIGQUERY_DATASET"
          value = google_bigquery_dataset.shc_cards.dataset_id
        }
        env {
          name  = "POSTGRES_IAM_USER"
          value = "${google_service_account.run_sa.account_id}@${data.google_project.project.project_id}.iam" //google_service_account.run_sa.email
        }
        env {
          name  = "DATABASE_NAME"
          value = google_sql_database.database.name
        }
        env {
          name  = "DATABASE_CONNECTION_NAME"
          value = google_sql_database_instance.instance.connection_name
        }
        env {
          name  = "SPANNER_INSTANCE_ID"
          value = google_spanner_instance.main.name
        }
        env {
          name  = "SPANNER_DATABASE_ID"
          value = google_spanner_database.database.name
        }
      }
    }
    metadata {
      annotations = {
        "autoscaling.knative.dev/maxScale"      = "100"
        "run.googleapis.com/cloudsql-instances" = google_sql_database_instance.instance.connection_name
        "run.googleapis.com/client-name"        = "shc-scraper"
      }
    }
  }

  traffic {
    percent         = 100
    latest_revision = true
  }

  depends_on = [
    google_project_service.project,
    null_resource.docker_image
  ]
}


resource "google_cloud_run_service" "anthrokrishi-scraper-pubsub" {
  provider = google-beta
  name     = "scraper-pubsub"
  location = var.region
  project  = var.project_id

  metadata {
    annotations = {
      "run.googleapis.com/ingress" : "all"
    }
  }

  template {
    spec {
      service_account_name  = google_service_account.run_sa.email
      timeout_seconds       = 3600
      container_concurrency = 10
      containers {
        image = "gcr.io/${var.project_id}/scraper:${local.container_folder_hash}"
        ports {
          name           = "http1"
          container_port = 8080
        }
        resources {
          limits = {
            cpu    = "4000m"
            memory = "4Gi"
          }
        }
        env {
          name  = "GCS_BUCKET"
          value = google_storage_bucket.shc-bucket.name
        }
        env {
          name  = "BIGQUERY_DATASET"
          value = google_bigquery_dataset.shc_cards.dataset_id
        }
        env {
          name  = "POSTGRES_IAM_USER"
          value = "${google_service_account.run_sa.account_id}@${data.google_project.project.project_id}.iam" //google_service_account.run_sa.email
        }
        env {
          name  = "DATABASE_NAME"
          value = google_sql_database.database.name
        }
        env {
          name  = "DATABASE_CONNECTION_NAME"
          value = google_sql_database_instance.instance.connection_name
        }
        env {
          name  = "SPANNER_INSTANCE_ID"
          value = google_spanner_instance.main.name
        }
        env {
          name  = "SPANNER_DATABASE_ID"
          value = google_spanner_database.database.name
        }
      }
    }
    metadata {
      annotations = {
        "autoscaling.knative.dev/maxScale"      = "100"
        "run.googleapis.com/cloudsql-instances" = google_sql_database_instance.instance.connection_name
        "run.googleapis.com/client-name"        = "shc-scraper-pubsub"
      }
    }
  }

  traffic {
    percent         = 100
    latest_revision = true
  }

  depends_on = [
    google_project_service.project,
    null_resource.docker_image
  ]
}

resource "google_cloud_run_service_iam_member" "member" {
  location = google_cloud_run_service.anthrokrishi-scraper.location
  project  = google_cloud_run_service.anthrokrishi-scraper.project
  service  = google_cloud_run_service.anthrokrishi-scraper.name
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.workflow_sa.email}"
}

resource "google_cloud_run_service_iam_member" "member-invoker-pubsub" {
  location = google_cloud_run_service.anthrokrishi-scraper-pubsub.location
  project  = google_cloud_run_service.anthrokrishi-scraper-pubsub.project
  service  = google_cloud_run_service.anthrokrishi-scraper-pubsub.name
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.workflow_sa.email}"
}

resource "google_cloud_tasks_queue" "states" {
  name = "shc-states-queue"
  location = var.region
  project = var.project_id
  rate_limits {
    max_concurrent_dispatches = 1
    max_dispatches_per_second = 10
  }
}

resource "google_cloud_tasks_queue" "villages" {
  name = "shc-villages-queue"
  location = var.region
  project = var.project_id
  rate_limits {
    max_concurrent_dispatches = 5
    max_dispatches_per_second = 10
  }
}

resource "google_cloud_tasks_queue" "default" {
  name = "shc-card-queue"
  location = var.region
  project = var.project_id
  rate_limits {
    max_concurrent_dispatches = 100
    max_dispatches_per_second = 500
  }
}
