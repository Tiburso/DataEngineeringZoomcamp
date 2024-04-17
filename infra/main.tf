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

resource "google_compute_network" "vpc_network" {
  name                    = "my-custom-mode-network"
  auto_create_subnetworks = false
  mtu                     = 1460
}

resource "google_compute_subnetwork" "default" {
  name          = "my-custom-subnet"
  ip_cidr_range = "10.0.1.0/24"
  region        = var.region
  network       = google_compute_network.vpc_network.id
}

# Add firewall rule for port 3000
resource "google_compute_firewall" "metabase_firewall" {
  name    = "allow-metabase"
  network = google_compute_network.vpc_network.id

  allow {
    protocol = "tcp"
    ports    = ["3000"]
  }

  source_ranges = ["0.0.0.0/0"]
}

resource "google_compute_firewall" "metabase_ssh" {
  name    = "allow-ssh"
  network = google_compute_network.vpc_network.id

  allow {
    protocol = "tcp"
    ports    = ["22"]
  }

  source_ranges = ["0.0.0.0/0"]

  target_tags = ["ssh"]
}

resource "google_compute_address" "metabase_address" {
  name = "metabase-address-de"
}

# Google compute engine instance to host metabase
resource "google_compute_instance" "metabase_instance" {
  name         = "metabase-instance"
  machine_type = "e2-micro"
  zone         = var.zone
  tags         = ["ssh"]

  boot_disk {
    initialize_params {
      image = "debian-cloud/debian-11"
    }
  }

  network_interface {
    subnetwork = google_compute_subnetwork.default.id

    access_config {
      nat_ip = google_compute_address.metabase_address.address
    }
  }

  metadata = {
    user-data = file("scripts/metabase-config.yaml")
  }
}

output "Metabase-URL" {
  value = join("", ["http://", google_compute_instance.metabase_instance.network_interface.0.access_config.0.nat_ip, ":3000"])
}
