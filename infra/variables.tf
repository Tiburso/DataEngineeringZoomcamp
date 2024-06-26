variable "credentials" {
  description = "My Credentials"
  default     = "creds.json"
  #ex: if you have a directory where this file is called keys with your service account json file
  #saved there as my-creds.json you could use default = "./keys/my-creds.json"
}


variable "project" {
  description = "Project"
  default     = "dataengineeringbootcamp-419022"
}

variable "region" {
  description = "Region"
  #Update the below to your desired region
  default = "europe-west1"
}

variable "zone" {
  description = "Zone"
  #Update the below to your desired zone
  default = "europe-west1-b"
}

variable "location" {
  description = "Project Location"
  #Update the below to your desired location
  default = "EU"
}

variable "bq_dataset_name" {
  description = "My BigQuery Dataset Name"
  #Update the below to what you want your dataset to be called
  default = "weather_data_de"
}

variable "gcs_bucket_name" {
  description = "My Storage Bucket Name"
  #Update the below to a unique bucket name
  default = "weather_data_de_bucket"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}

variable "dataproc_cluster_name" {
  description = "My Dataproc Cluster Name"
  #Update the below to what you want your cluster to be called
  default = "weather-data-de-cluster"
}
