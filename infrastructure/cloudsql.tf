resource "google_sql_database_instance" "instance" {
  name             = "shc-scraper-mysql"
  database_version = "POSTGRES_14"
  region           = var.region
  project          = data.google_project.project.project_id

  settings {
    tier = "db-f1-micro"
    database_flags {
      name  = "cloudsql.iam_authentication"
      value = "on"
    }
  }

  deletion_protection = "true"
}

resource "google_sql_database" "database" {
  name     = "shc-scraper"
  project  = data.google_project.project.project_id
  instance = google_sql_database_instance.instance.name
}

resource "google_sql_user" "iam_user_sa" {
  project  = data.google_project.project.project_id
  name     = "${google_service_account.run_sa.account_id}@${data.google_project.project.project_id}.iam"
  instance = google_sql_database_instance.instance.name
  type     = "CLOUD_IAM_SERVICE_ACCOUNT"
}

resource "google_project_iam_member" "instance_user" {
  project = data.google_project.project.project_id
  role    = "roles/cloudsql.instanceUser"
  member  = format("serviceAccount:%s", google_service_account.run_sa.email)
}

resource "google_project_iam_member" "sql_client" {
  project = data.google_project.project.project_id
  role    = "roles/cloudsql.client"
  member  = format("serviceAccount:%s", google_service_account.run_sa.email)
}

resource "google_sql_user" "iam_user" {
  project  = data.google_project.project.project_id
  name     = "grotz@google.com"
  instance = google_sql_database_instance.instance.name
  type     = "CLOUD_IAM_USER"
}

resource "google_project_iam_member" "iam_user_cloudsql_instance_user" {
  project  = data.google_project.project.project_id
  role   = "roles/cloudsql.instanceUser"
  member = format("user:%s", google_sql_user.iam_user.name)
}

resource "google_project_iam_member" "iam_user_cloudsql_client" {
  project  = data.google_project.project.project_id
  role   = "roles/cloudsql.client"
  member = format("user:%s", google_sql_user.iam_user.name)
}