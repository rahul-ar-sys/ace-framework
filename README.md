# ACE Assessment Framework

A course-agnostic system that automates evaluation of learners' submissions across Analysis, Communication, and Evaluation (ACE) dimensions.

## System Architecture

The framework consists of six core layers:

1. **Ingestion & Normalization Layer** - CSV processing and data normalization
2. **Interpreter & Routing Layer** - Task routing to specialized processors
3. **Processing Layer** - MCQ, Text, and Audio evaluation processors
4. **Aggregation & Reporting Layer** - Score aggregation and report generation
5. **Analytics & Querying Layer** - Data querying and dashboard support
6. **Fault Tolerance & Monitoring Layer** - Error handling and observability

## Technology Stack

- **Infrastructure**: AWS (S3, Lambda, Athena, SQS, ECS/Fargate, DynamoDB, CloudWatch)
- **IaC**: AWS CDK (TypeScript)
- **Processing**: Python with AI/ML libraries
- **Reporting**: JSON, PDF, CSV outputs
- **Monitoring**: CloudWatch metrics and dashboards

## Project Structure

```
ace_framework/
├── infrastructure/          # AWS CDK infrastructure code
├── services/              # Core service implementations
├── processors/            # Specialized evaluation processors
├── config/               # Configuration files and schemas
├── tests/               # Unit and integration tests
├── docs/                # Documentation
└── scripts/             # Deployment and utility scripts
```

## Getting Started

See the [Deployment Guide](docs/deployment.md) for detailed setup instructions.
