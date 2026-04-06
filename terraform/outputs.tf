output "runtime_env_file" {
  description = "Generated environment file for the current deployment target."
  value       = local_file.runtime_env.filename
}

output "deployment_manifest_file" {
  description = "Generated deployment manifest describing the pipeline runtime contract."
  value       = local_file.deployment_manifest.filename
}

output "deployment_summary" {
  description = "High-level summary of the current Terraform-managed deployment scaffold."
  value = {
    project_name      = var.project_name
    environment       = var.environment
    deployment_target = var.deployment_target
    future_cloud      = var.future_cloud
    apply_mode        = "manual-only"
  }
}
