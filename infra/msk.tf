data "aws_vpc" "default" {
  count = var.enable_msk ? 1 : 0

  default = true
}

data "aws_subnets" "default" {
  count = var.enable_msk ? 1 : 0

  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default[0].id]
  }
}

resource "aws_kms_key" "msk_scram" {
  count = var.enable_msk ? 1 : 0

  description         = "Encrypt MSK SCRAM secret for ${var.project_name}"
  enable_key_rotation = true

  tags = {
    Project = var.project_name
  }
}

resource "random_password" "kafka" {
  count = var.enable_msk ? 1 : 0

  length  = 24
  special = true
}

resource "aws_secretsmanager_secret" "msk_scram" {
  count = var.enable_msk ? 1 : 0

  name       = "AmazonMSK_${var.project_name}"
  kms_key_id = aws_kms_key.msk_scram[0].arn

  tags = {
    Project = var.project_name
  }
}

resource "aws_secretsmanager_secret_version" "msk_scram" {
  count = var.enable_msk ? 1 : 0

  secret_id = aws_secretsmanager_secret.msk_scram[0].id
  secret_string = jsonencode({
    username = var.kafka_username
    password = random_password.kafka[0].result
  })
}

resource "aws_security_group" "msk" {
  count = var.enable_msk ? 1 : 0

  name_prefix = "${var.project_name}-msk-"
  description = "MSK brokers for ${var.project_name}"
  vpc_id      = data.aws_vpc.default[0].id

  ingress {
    description = "Broker-to-broker"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    self        = true
  }

  # ponytail: open for dev laptops; tighten to your IP in prod
  ingress {
    description = "Public SASL/SCRAM clients"
    from_port   = 9196
    to_port     = 9196
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Project = var.project_name
  }
}

resource "aws_msk_configuration" "main" {
  count = var.enable_msk ? 1 : 0

  kafka_versions = [var.kafka_version]
  name           = "${var.project_name}-msk-config"

  server_properties = <<PROPERTIES
auto.create.topics.enable=true
num.partitions=3
default.replication.factor=2
PROPERTIES
}

resource "aws_msk_cluster" "main" {
  count = var.enable_msk ? 1 : 0

  cluster_name           = "${var.project_name}-kafka"
  kafka_version          = var.kafka_version
  number_of_broker_nodes = var.msk_broker_count

  broker_node_group_info {
    instance_type   = var.msk_instance_type
    client_subnets  = data.aws_subnets.default[0].ids
    security_groups = [aws_security_group.msk[0].id]

    connectivity_info {
      public_access {
        type = "SERVICE_PROVIDED_EIPS"
      }
    }
  }

  client_authentication {
    sasl {
      scram = true
    }

    unauthenticated = false
  }

  configuration_info {
    arn      = aws_msk_configuration.main[0].arn
    revision = aws_msk_configuration.main[0].latest_revision
  }

  encryption_info {
    encryption_in_transit {
      client_broker = "TLS"
    }
  }

  tags = {
    Project = var.project_name
  }

  depends_on = [aws_secretsmanager_secret_version.msk_scram]
}

resource "aws_msk_scram_secret_association" "main" {
  count = var.enable_msk ? 1 : 0

  cluster_arn     = aws_msk_cluster.main[0].arn
  secret_arn_list = [aws_secretsmanager_secret.msk_scram[0].arn]

  depends_on = [aws_secretsmanager_secret_version.msk_scram]
}

data "aws_msk_bootstrap_brokers" "main" {
  count = var.enable_msk ? 1 : 0

  cluster_arn = aws_msk_cluster.main[0].arn

  depends_on = [aws_msk_scram_secret_association.main]
}
