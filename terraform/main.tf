terraform {
  required_version = ">= 1.0.0"

  required_providers {
    local = {
      source  = "hashicorp/local"
      version = "~> 2.5"
    }
  }
}

resource "local_file" "gitops_demo" {
  filename = local.generated_filename
  content  = <<-EOT
    environment=${var.environment}
    message=Terraform ran successfully via GitHub Actions
  EOT
}
