variable "credentials" {
  description = "GCS credentials"
  default     = "./key.json"
}

variable "project_name" {
  description = "Project Name"
  default     = "ny-taxi-rides-485210"
}

variable "region" {
  description = "Region of Paris (if users are in France or Europe)"
  default     = "europe-west9"
}


variable "location" {
  description = "Project location"
  default     = "EU"
}

variable "bq_dataset_name" {
  description = "My BigQuery Dataset Name"
  default     = "demo_dataset"
}

variable "gcs_bucket_name" {
  description = "Bucket Storage Name"
  default     = "ny-taxi-rides-485210-bucket"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}