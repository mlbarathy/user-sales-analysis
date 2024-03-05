# User Sales Analysis Using Spark

This project uses Spark(Scala) for building a comprehensive sales data pipeline for a retail company. The pipeline generated 
combines sales data with data from external sources (api end points). Data transformations and aggregations are performed, and store the final dataset in a database. The aim this project is to enable analysis and derive insights into customer behaviour and sales performance.

## Contents
- [Project Overview](#project-overview)
- [Requirements](#requirements)
- [Getting Started](#getting-started)
  - [Jenkins Pipeline](#jenkins-pipeline)
  - [Airflow DAG Deployment](#airflow-dag-deployment)
- [Code Base](#code-base)
- [Transformations And Aggregations](#transformation-and-aggregations)
  - [2. Temporary Table Creation](#2-temporary-table-creation)
  - [3. Queries](#3-queries)
  - [4. Caching](#4-caching)
  - [4. Visualization](#4-caching)
 
<br>
<br>
 
## Project Overview

  The aim for this project is to analyze and derive insights into customer behaviour and sales performance using Spark & Visualization through transformations and aggregations using SPark APIs and Spark SQL and Notebook for visualization

## Requirements

  For deploying the entire pipeline , below requirements must be avaiable.
  
  - Scala (2.13 ) with Spark (3.3.1)
  - Jenkins
  - Maven
  - Airflow for Scheduling.
  - AWS S3 & EMR
  - Notebook for Visualization
  - DataBase Viewer
 
## Getting Started

### Jenkins Pipeline
  
  Jenkins pipeline must be deployed for building the artifacts and moving to S3. The pipleline has 5 stages:

  - Initialization
  - Checkout Source Code
  - Maven Build
  - Deploy Scripts to EMR Airflow
  - Deploy Scripts to S3.

<img width="615" alt="image" src="https://github.com/mlbarathy/user-sales-analysis/assets/43374951/00459af1-8e4c-4e81-a5b9-a0393ca0ae98">



### Airflow DAG Deployment

Part of Jenkins pipeline , airflow dag will deployed and will be ready for trigeering.The DAG script is under airfloeDAG from parent directory

<img width="1574" alt="image" src="https://github.com/mlbarathy/user-sales-analysis/assets/43374951/d698d75a-927e-4e5d-b918-4bd20ab9318e">

Once the DAG is deployed sucessfully , create a Variable (USER_SALES_ANALYSIS_SPARK_CONFIG) in Airlfow with below values and can be modified

<img width="1061" alt="image" src="https://github.com/mlbarathy/user-sales-analysis/assets/43374951/4ed98f1b-8ac8-408e-b6b5-dcbabf861680">





## Code Base

The code base framework contains below artifacts,

  - airflowDag  : Python File for DAG deployment
  - saleDate    : Sample Sales Data Provided
  - schemaJson  : Configuration file for providing schema, transformation query and config details
  - spark_emr   : Scala Spark Code.

## Transformations And Aggregations.

The following transformations & aggregations are perfornmed ,

  - Finding the total sales amount per customer.
  - Identifing the top-selling products or customers.
  - Analyzing sales trends over time (e.g., monthly or quarterly sales).
  - Analysis over weather data in to find the average sales amount per weather condition)
  - Find the top product selling in terms of quantity and price , etc







    
  

  
