terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "5.6.0"
    }
  }
}

provider "google" {
  credentials = file(var.credentials)
  project     = var.project
  region      = var.region
}


resource "google_storage_bucket" "weather_lake" {
  name          = var.gcs_bucket_name
  location      = var.location
  force_destroy = true


  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}



resource "google_bigquery_dataset" "weather_dataset" {
  dataset_id = var.bq_dataset_name
  location   = var.location
}

# In the future create a dataproc cluster
resource "google_dataproc_cluster" "spark_cluster" {
  name   = var.dataproc_cluster_name
  region = var.region

  cluster_config {
    gce_cluster_config {
      zone = var.zone
    }

    software_config {
      override_properties = {
        "dataproc:dataproc.allow.zero.workers" = "true"
      }
    }
  }

}
