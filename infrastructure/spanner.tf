resource "google_spanner_instance" "main" {
  config       = "regional-europe-west1"
  project = var.project_id
  display_name = "AnthroKrishi"
  processing_units    = 1000
}

resource "google_spanner_instance_iam_member" "database" {
  instance = google_spanner_instance.main.name
  role     = "roles/spanner.viewer"
  member  = format("serviceAccount:%s", google_service_account.run_sa.email)
}

resource "google_spanner_database" "database" {
  instance = google_spanner_instance.main.name
  name     = "metadata"
  ddl = [
    "CREATE TABLE States (StateId INT64 NOT NULL, Name STRING(MAX)) PRIMARY KEY(StateId)",
    "CREATE TABLE Districts (StateId INT64 NOT NULL,DistrictId INT64 NOT NULL, Name STRING(MAX)) PRIMARY KEY(StateId, DistrictId)",
    "CREATE TABLE SubDistricts (DistrictId INT64 NOT NULL,SubDistrictId INT64 NOT NULL, Name STRING(MAX)) PRIMARY KEY(DistrictId, SubDistrictId)",
    "CREATE TABLE Villages (SubDistrictId INT64 NOT NULL,VillageId INT64 NOT NULL, Name STRING(MAX)) PRIMARY KEY(SubDistrictId, VillageId)",
    "CREATE TABLE Cards (VillageId INT64 NOT NULL, Sample STRING(MAX) NOT NULL, VillageGrid STRING(MAX), SrNo INT64 NOT NULL, Ingested BOOL) PRIMARY KEY(VillageId, Sample, SrNo)",
    "CREATE TABLE Checkpoints (Id INT64 NOT NULL, StateId INT64 NOT NULL, DistrictId INT64 NOT NULL, SubDistrictId INT64 NOT NULL) PRIMARY KEY(Id)"
  ]
  deletion_protection = false
}

resource "google_spanner_database_iam_member" "database" {
  instance = google_spanner_instance.main.name
  database = google_spanner_database.database.name
  role     = "roles/spanner.databaseUser"
  member  = format("serviceAccount:%s", google_service_account.run_sa.email)
}