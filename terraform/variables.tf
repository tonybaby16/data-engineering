variable "dataset_name" {
  description = "my big query dataset name"
  default = "nytaxi"
}
variable "gcs_storage_class" {
  description = "bucket storage class"
  default = "STANDARD"
}
variable "gcs_bucket_name" {
  description = "storage bucket name"
  default = "demo_bucket_project-88010cf5-939d-4e44-92f"
}
variable "location" {
  description = "project lcoation"
  default = "US"
}