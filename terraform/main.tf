
provider "aws" {
  region = "us-east-1"

  default_tags {
    tags = {
      Project     = "meditrack360-data-lake"
      Environment = "dev"
      ManagedBy   = "Terraform"
    }
  }
}