/**
 * Copyright 2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

resource "google_spanner_instance" "main" {
  config           = "regional-europe-west1"
  project          = var.project_id
  display_name     = "SoilHealhCards"
  processing_units = 5000

  depends_on = [
    google_project_service.project
  ]
}

resource "google_spanner_instance_iam_member" "database" {
  instance = google_spanner_instance.main.name
  role     = "roles/spanner.viewer"
  member   = format("serviceAccount:%s", google_service_account.scraper.email)
}

resource "google_spanner_database" "database" {
  instance = google_spanner_instance.main.name
  name     = "metadata"
  ddl = [
    "CREATE TABLE Cards ( VillageId INT64 NOT NULL, Sample STRING(MAX) NOT NULL, VillageGrid STRING(MAX), SrNo INT64 NOT NULL, Ingested BOOL, Extracted BOOL ) PRIMARY KEY(VillageId, Sample, SrNo)",
    "CREATE INDEX CARDS_FULL_INDEX ON Cards(VillageId, Ingested)",
    "CREATE TABLE Checkpoints ( Id INT64 NOT NULL, StateId INT64 NOT NULL, DistrictId INT64 NOT NULL, SubDistrictId INT64 NOT NULL ) PRIMARY KEY(Id)",
    "CREATE TABLE Districts ( StateId INT64 NOT NULL, DistrictId INT64 NOT NULL, Name STRING(MAX) ) PRIMARY KEY(StateId, DistrictId)",
    "CREATE TABLE States ( StateId INT64 NOT NULL, Name STRING(MAX) ) PRIMARY KEY(StateId)",
    "CREATE TABLE SubDistricts ( DistrictId INT64 NOT NULL, SubDistrictId INT64 NOT NULL, Name STRING(MAX) ) PRIMARY KEY(DistrictId, SubDistrictId)",
    "CREATE TABLE Villages ( SubDistrictId INT64 NOT NULL, VillageId INT64 NOT NULL, Name STRING(MAX), CardsLoaded BOOL ) PRIMARY KEY(SubDistrictId, VillageId)",
    "CREATE VIEW VILLAGES_VIEW SQL SECURITY INVOKER AS SELECT v.VillageId as VillageId, v.Name as VillageName, v.CardsLoaded as CardsLoaded, sd.SubDistrictId as SubDistrictId, sd.Name as SubDistrictName, d.DistrictId as DistrictId, d.Name as DistrictName, s.StateId as StateId, s.Name as StateName FROM Villages v INNER JOIN SubDistricts sd ON sd.SubDistrictId = v.SubDistrictId INNER JOIN Districts d ON d.DistrictId = sd.DistrictId INNER JOIN States s ON s.StateId = d.StateId ORDER BY v.VillageId"
  ]
  deletion_protection = false
}

resource "google_spanner_database_iam_member" "database" {
  instance = google_spanner_instance.main.name
  database = google_spanner_database.database.name
  role     = "roles/spanner.databaseUser"
  member   = format("serviceAccount:%s", google_service_account.scraper.email)
}