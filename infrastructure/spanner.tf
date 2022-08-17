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
    "CREATE TABLE Cards (VillageId INT64 NOT NULL, Sample STRING(MAX) NOT NULL, VillageGrid STRING(MAX), SrNo INT64 NOT NULL, Ingested BOOL, Extracted BOOL, StateId INT64, DistrictId INT64, SubDistrictId INT64, extract_attempt INT64 DEFAULT (0), data_comparison_mismatch STRING(MAX)) PRIMARY KEY(VillageId, Sample, SrNo);",
    "CREATE INDEX CARDS_FULL_INDEX ON Cards(VillageId, Ingested);",
    "CREATE TABLE Cards_info (SampleNo STRING(MAX) NOT NULL, SrNo INT64 NOT NULL, VillageId INT64 NOT NULL, SubDistrictId INT64 NOT NULL, DistrictId INT64 NOT NULL, StateId INT64 NOT NULL, soil_health_card_number STRING(MAX), validity STRING(MAX), survey_no STRING(MAX), farm_size FLOAT64, farm_size_unit STRING(MAX), irrigation_method STRING(MAX), latitude FLOAT64, longitude FLOAT64, soil_test_lab STRING(MAX), soil_type STRING(MAX), pH_parameter STRING(MAX), pH_value FLOAT64, pH_unit STRING(MAX), pH_rating STRING(MAX), pH_min_normal_level FLOAT64, pH_max_normal_level FLOAT64, pH_unit_normal_level STRING(MAX), EC_parameter STRING(MAX), EC_value FLOAT64, EC_unit STRING(MAX), EC_rating STRING(MAX), EC_min_normal_level FLOAT64, EC_max_normal_level FLOAT64, EC_unit_normal_level STRING(MAX), OC_parameter STRING(MAX), OC_value FLOAT64, OC_unit STRING(MAX), OC_rating STRING(MAX), OC_min_normal_level FLOAT64, OC_max_normal_level FLOAT64, OC_unit_normal_level STRING(MAX), N_parameter STRING(MAX), N_value FLOAT64, N_unit STRING(MAX), N_rating STRING(MAX), N_min_normal_level FLOAT64, N_max_normal_level FLOAT64, N_unit_normal_level STRING(MAX), P_parameter STRING(MAX), P_value FLOAT64, P_unit STRING(MAX), P_rating STRING(MAX), P_min_normal_level FLOAT64, P_max_normal_level FLOAT64, P_unit_normal_level STRING(MAX), K_parameter STRING(MAX), K_value FLOAT64, K_unit STRING(MAX), K_rating STRING(MAX), K_min_normal_level FLOAT64, K_max_normal_level FLOAT64, K_unit_normal_level STRING(MAX), S_parameter STRING(MAX), S_value FLOAT64, S_unit STRING(MAX), S_rating STRING(MAX), S_min_normal_level FLOAT64, S_max_normal_level FLOAT64, S_unit_normal_level STRING(MAX), Zn_parameter STRING(MAX), Zn_value FLOAT64, Zn_unit STRING(MAX), Zn_rating STRING(MAX), Zn_min_normal_level FLOAT64, Zn_max_normal_level FLOAT64, Zn_unit_normal_level STRING(MAX), B_parameter STRING(MAX), B_value FLOAT64, B_unit STRING(MAX), B_rating STRING(MAX), B_min_normal_level FLOAT64, B_max_normal_level FLOAT64, B_unit_normal_level STRING(MAX), Fe_parameter STRING(MAX), Fe_value FLOAT64, Fe_unit STRING(MAX), Fe_rating STRING(MAX), Fe_min_normal_level FLOAT64, Fe_max_normal_level FLOAT64, Fe_unit_normal_level STRING(MAX), Mn_parameter STRING(MAX), Mn_value FLOAT64, Mn_unit STRING(MAX), Mn_rating STRING(MAX), Mn_min_normal_level FLOAT64, Mn_max_normal_level FLOAT64, Mn_unit_normal_level STRING(MAX), Cu_parameter STRING(MAX), Cu_value FLOAT64, Cu_unit STRING(MAX), Cu_rating STRING(MAX), Cu_min_normal_level FLOAT64, Cu_max_normal_level FLOAT64, Cu_unit_normal_level STRING(MAX), error_log STRING(MAX), sample_collection_date DATE, recommendations JSON, fertilizer_combinations JSON) PRIMARY KEY(SampleNo, SrNo, VillageId);",
    "CREATE TABLE Checkpoints (Id INT64 NOT NULL, StateId INT64 NOT NULL, DistrictId INT64 NOT NULL, SubDistrictId INT64 NOT NULL) PRIMARY KEY(Id);"
    "CREATE TABLE Districts (StateId INT64 NOT NULL, DistrictId INT64 NOT NULL, Name STRING(MAX)) PRIMARY KEY(StateId, DistrictId);",
    "CREATE TABLE States (StateId INT64 NOT NULL, Name STRING(MAX)) PRIMARY KEY(StateId);",
    "CREATE TABLE SubDistricts (DistrictId INT64 NOT NULL, SubDistrictId INT64 NOT NULL, Name STRING(MAX)) PRIMARY KEY(DistrictId, SubDistrictId);",
    "CREATE TABLE Villages (SubDistrictId INT64 NOT NULL, VillageId INT64 NOT NULL, Name STRING(MAX), CardsLoaded BOOL) PRIMARY KEY(SubDistrictId, VillageId);",
    "CREATE VIEW UNIGESTED_CARDS SQL SECURITY INVOKER AS SELECT c.Sample as Sample, c.VillageGrid as VillageGrid, c.SrNo as SrNo, c.VillageId as VillageId, v.Name as VillageName, sd.SubDistrictId as SubDistrictId, sd.Name as SubDistrictName, d.DistrictId as DistrictId, d.Name as DistrictName, s.StateId as StateId, s.Name as StateName FROM Cards c  INNER JOIN Villages v ON c.VillageId = v.VillageId INNER JOIN SubDistricts sd ON v.SubDistrictId = sd.SubDistrictId INNER JOIN Districts d ON sd.DistrictId = d.DistrictId INNER JOIN States s ON d.StateId = s.StateId WHERE c.Ingested IS NULL ORDER BY c.VillageId;",
    "CREATE VIEW VILLAGES_VIEW SQL SECURITY INVOKER AS SELECT v.VillageId as VillageId, v.Name as VillageName, v.CardsLoaded as CardsLoaded, sd.SubDistrictId as SubDistrictId, sd.Name as SubDistrictName, d.DistrictId as DistrictId, d.Name as DistrictName, s.StateId as StateId, s.Name as StateName FROM Villages v INNER JOIN SubDistricts sd ON sd.SubDistrictId = v.SubDistrictId INNER JOIN Districts d ON d.DistrictId = sd.DistrictId INNER JOIN States s ON s.StateId = d.StateId ORDER BY v.VillageId;",  
  ]
  deletion_protection = false
}

resource "google_spanner_database_iam_member" "database" {
  instance = google_spanner_instance.main.name
  database = google_spanner_database.database.name
  role     = "roles/spanner.databaseUser"
  member   = format("serviceAccount:%s", google_service_account.scraper.email)
}
