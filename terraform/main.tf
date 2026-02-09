terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "7.18.0"
    }
  }
}

provider "google" {
  # Configuration options
  credentials = "./keys.json"
  project     = "project-88010cf5-939d-4e44-92f"
  region      = "us-central1"

}
resource "google_storage_bucket" "demo-bucket" {
  name          = var.gcs_bucket_name
  location      = var.location
  force_destroy = true

  uniform_bucket_level_access = true

  lifecycle_rule {
    condition {
      age = 5
    }
    action {
      type = "Delete"
    }
  }
}
resource "google_bigquery_dataset" "nytaxi" {
  dataset_id = var.dataset_name
}