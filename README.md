# Exchange Rate Pipeline

This project automates the process of fetching exchange rates, transforming the data using **AWS Glue**, and storing it in a structured format for efficient analysis. The pipeline utilizes a combination of AWS services such as **AWS Lambda**, **AWS Glue**, **AWS Step Functions**, and **AWS EventBridge**.

## Overview

The **Exchange Rate Pipeline** project fetches exchange rates from an external API, processes the data, and stores it in a Parquet format on **Amazon S3**. Here's how the components work together:

- **AWS Lambda**: Fetches exchange rates from an external API and stores the data in an S3 bucket.
- **AWS Glue**: Performs ETL (Extract, Transform, Load) to convert the data into Parquet format.
- **AWS Step Functions**: Orchestrates the flow between Lambda and Glue jobs, ensuring a seamless process.
- **AWS EventBridge**: Triggers the Lambda function every 24 hours to fetch the latest exchange rates.

---

## Architecture Diagram

![Research and design](https://github.com/user-attachments/assets/10374ad5-838d-42f2-a47c-d23ccd78e67f)


---

## Prerequisites

Before you begin, make sure you have the following tools installed and configured:

- **AWS CLI**: AWS Command Line Interface configured with the necessary permissions.
- **Python 3.x**: Installed on your local machine.
- **AWS SDK for Python (Boto3)**: Installable via `pip install boto3`.
- **Git**: For version control and managing the project.

Ensure your AWS account has the necessary permissions to interact with the services involved (Lambda, Glue, Step Functions, S3, and EventBridge).

---

Additional Resources
If you are new to any of the AWS services used in this project, you might find the following resources helpful:

AWS Lambda Documentation
AWS Glue Documentation
AWS Step Functions Documentation
AWS EventBridge Documentation
