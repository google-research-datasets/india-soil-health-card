resource "google_pubsub_topic" "shcs" {
  name = "shcs"
  project = data.google_project.project.project_id

  message_retention_duration = "86600s"
}

resource "google_pubsub_topic_iam_member" "binding" {
  project = data.google_project.project.project_id
  topic = google_pubsub_topic.shcs.name
  role = "roles/pubsub.editor" # maybe .publisher is sufficient
  member = "serviceAccount:${google_service_account.workflow_sa.email}"
}

resource "google_service_account" "pubsub_pusher_sa" {
  account_id = "pubsub-pusher"
}

resource "google_cloud_run_service_iam_member" "member-pubsub" {
  location = google_cloud_run_service.anthrokrishi-scraper-pubsub.location
  project  = google_cloud_run_service.anthrokrishi-scraper-pubsub.project
  service  = google_cloud_run_service.anthrokrishi-scraper-pubsub.name
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.pubsub_pusher_sa.email}"
}

resource "google_pubsub_subscription" "scraper" {
  name  = "scraper-subscription"
  project = data.google_project.project.project_id

  topic = google_pubsub_topic.shcs.name

  ack_deadline_seconds = 600
/*
  push_config {
    push_endpoint = "${google_cloud_run_service.anthrokrishi-scraper-pubsub.status[0].url}/push"
    oidc_token {
      service_account_email = google_service_account.pubsub_pusher_sa.email
    }
  }*/

  dead_letter_policy {
    dead_letter_topic = google_pubsub_topic.shc_dead_letter.id
    max_delivery_attempts = 50
  }
}

resource "google_pubsub_topic" "shc_dead_letter" {
  name = "shc-scraper-dead-letter"
}

resource "google_pubsub_topic_iam_member" "binding_dead_letter" {
  project = data.google_project.project.project_id
  topic = google_pubsub_topic.shcs.name
  role = "roles/pubsub.publisher"
  member = "serviceAccount:service-${data.google_project.project.number}@gcp-sa-pubsub.iam.gserviceaccount.com"
}

resource "google_pubsub_subscription" "shc_dead_letter_sub" {
  name  = "shc_dead_letter_sub"
  topic = google_pubsub_topic.shc_dead_letter.name
}