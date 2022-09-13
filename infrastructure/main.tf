# Hashicorp Configuration Language 
# Linguagem Declarativa 

resource "aws_s3_bucket" "datalake" {
  #Parâmetros de configuração do recurso escolhido
  bucket = "${var.base_bucket_name}-${var.ambiente}-${var.numero_conta}"
  acl    = "private"

  server_side_encryption_configuration {
    rule {
      apply_server_side_encryption_by_default {
        sse_algorithm = "AES256"
      }
    }
  }

  tags = {
    IES   = "IGTI"
    CURSO = "EDC"
  }
}


resource "aws_s3_bucket_object" "codigo_spark" {
  bucket = aws_s3_bucket.datalake.id
  key    = "emr-code/pyspark/job_spark_from_tf.py"
  acl    = "private"
  source = "scripts/job_spark.py"
  etag   = filemd5("scripts/job_spark.py")
}

provider "aws" {
  region = "us-east-2"
}

# Centralizar o arquivo de controle de estado do terraform 
terraform {
  backend "s3" {
    bucket = "terraform-state-igti-clo"
    key    = "state/igti/edc/mod1/terraform.tfstate"
    region = "us-east-2"
  }
}