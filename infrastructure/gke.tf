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

resource "google_service_account" "default" {
  account_id   = "gke-nodes"
  display_name = "GKE Nodes"
}

resource "google_container_cluster" "cluster" {
  name     = "scraper-cluster"
  location = var.region
  project = var.project_id

  remove_default_node_pool = true
  initial_node_count       = 1

  workload_identity_config { 
  }

  depends_on = [
    google_project_service.project
  ]
}

resource "google_container_node_pool" "spot_pool" {
  provider = google-beta

  name       = "spot-node-pool"
  location   = var.region
  project    = var.project_id
  cluster    = google_container_cluster.cluster.name 
  node_count = 1
  autoscaling {
    min_node_count = 0
    max_node_count = 20
  }

  node_config {
    service_account = google_service_account.scraper.email
    oauth_scopes = [
      "https://www.googleapis.com/auth/cloud-platform"
    ]

    labels = {
      env = var.project_id
    }

    # enabling usage of spot nodes for this pool
    spot = true

    machine_type = "n2-standard-4"
    tags         = ["gke-spot-node"]
    metadata = {
      disable-legacy-endpoints = "true"
    }
  }

  lifecycle {
    ignore_changes = [
      node_count
    ]
  }
}


resource "google_container_node_pool" "ondemand_pool" {
  provider = google-beta

  name       = "ondemand-node-pool"
  location   = var.region
  project    = var.project_id
  cluster    = google_container_cluster.cluster.name 
  node_count = 1
  autoscaling {
    min_node_count = 0
    max_node_count = 20
  }

  node_config {
    service_account = google_service_account.scraper.email
    oauth_scopes = [
      "https://www.googleapis.com/auth/cloud-platform"
    ]

    labels = {
      env = var.project_id
    }

    machine_type = "e2-standard-4"
    tags         = ["gke-ondemand-node"]
    metadata = {
      disable-legacy-endpoints = "true"
    }
  }

  lifecycle {
    ignore_changes = [
      node_count
    ]
  }
}