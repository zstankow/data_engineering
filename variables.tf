variable "credentials" {
    description = "My Credentials"
    default     = "./keys/my-creds.json"
}

variable "project" {
    description = "Project"
    default     = "seismic-helper-431214-n4"
}

variable "location" {
    description = "project location"
    default = "US"
}

variable "region" {
    description = "Region"
    default     = "us-central1"
}

variable "bq_dataset_name" {
    description = "My BigQuery dataset name"
    default = "demo_dataset"
}

variable "gcs_storage_class" {
    description = "Bucket Storage Class"
    default = "STANDARD"
}

variable "gcs_bucket_name" {
    description = "my storage bucket name"
    default = "seismic-helper-431214-n4-terra-bucket"
}