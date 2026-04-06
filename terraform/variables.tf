variable "environment" {
  description = "Environment name used to label generated deployment scaffolding."
  type        = string
  default     = "dev"
}

variable "project_name" {
  description = "Project identifier written into generated deployment assets."
  type        = string
  default     = "airflow-ecommerce-pipeline"
}

variable "deployment_target" {
  description = "Current runtime target for the pipeline."
  type        = string
  default     = "local"
}

variable "future_cloud" {
  description = "Placeholder for the cloud provider that will eventually host the production runtime."
  type        = string
  default     = "unassigned"
}

variable "airflow_schedule" {
  description = "Schedule expression mirrored into generated deployment metadata."
  type        = string
  default     = "0 8 * * *"
}

variable "raw_event_count" {
  description = "Default event volume used by the scheduled pipeline run."
  type        = number
  default     = 25
}

variable "dirty_rate" {
  description = "Default dirty-data rate used by the event generator."
  type        = number
  default     = 0.15
}
