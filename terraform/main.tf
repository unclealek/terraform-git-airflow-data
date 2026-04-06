terraform {
  required_version = ">= 1.0.0"

  required_providers {
    local = {
      source  = "hashicorp/local"
      version = "~> 2.5"
    }
  }
}

resource "local_file" "runtime_env" {
  filename = local.runtime_env_filename
  content  = join("\n", local.runtime_env_lines)
}

resource "local_file" "deployment_manifest" {
  filename = local.deployment_manifest_filename
  content  = jsonencode(local.deployment_manifest)
}
