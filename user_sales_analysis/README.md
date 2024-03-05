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
- [visualizations](#visualizations)
  
 
<br>
<br>
 
## Project Overview

  The aim for this project is to analyze and derive insights into customer behaviour and sales performance using Spark & Visualization through transformations and aggregations using SPark APIs and Spark SQL and Notebook for visualization
<br>
<br>  

## Requirements

  For deploying the entire pipeline , below requirements must be avaiable.
  
  - Scala (2.13 ) with Spark (3.3.1)
  - Jenkins
  - Maven
  - Airflow for Scheduling.
  - AWS S3 & EMR
  - Notebook for Visualization
  - DataBase Viewer

<br>
<br>
 
## Getting Started
<br>

### Jenkins Pipeline
  
  Jenkins pipeline must be deployed for building the artifacts and moving to S3. The pipleline has 5 stages:

  - Initialization
  - Checkout Source Code
  - Maven Build
  - Deploy Scripts to EMR Airflow
  - Deploy Scripts to S3.

<img width="615" alt="image" src="https://github.com/mlbarathy/user-sales-analysis/assets/43374951/00459af1-8e4c-4e81-a5b9-a0393ca0ae98">
<br>
<br>


### Airflow DAG Deployment

Part of Jenkins pipeline , airflow dag will deployed and will be ready for trigeering.The DAG script is under airfloeDAG from parent directory

<img width="1574" alt="image" src="https://github.com/mlbarathy/user-sales-analysis/assets/43374951/d698d75a-927e-4e5d-b918-4bd20ab9318e">
<br>
<br>


Once the DAG is deployed sucessfully , create a Variable (USER_SALES_ANALYSIS_SPARK_CONFIG) in Airlfow with below values and can be modified

<img width="1061" alt="image" src="https://github.com/mlbarathy/user-sales-analysis/assets/43374951/4ed98f1b-8ac8-408e-b6b5-dcbabf861680">

<br>
<br>

## Code Base

The code base framework contains below artifacts,

  - airflowDag  : Python File for DAG deployment
  - saleDate    : Sample Sales Data Provided
  - schemaJson  : Configuration file for providing schema, transformation query and config details
  - spark_emr   : Scala Spark Code.
    - Has 2 Items :
      - Controller - Main Class for Execution
      - Utils - Contains Reusable Functions like , table creation,data insertion,transformation, etc 
                   
    

<br>
<br>

## Transformations And Aggregations.

The following transformations & aggregations are perfornmed ,

  - Finding the total sales amount per customer.
  - Identifing the top-selling products or customers.
  - Analyzing sales trends over time (e.g., monthly or quarterly sales).
  - Analysis over weather data in to find the average sales amount per weather condition)
  - Find the top product selling in terms of quantity and price , etc

<br>
<br>

## Visualizations

The Analysis and Trend of the user sales are captured through NoteBook visualizations.
<br>
### Total Sales Per Customer:
<img width="1557" alt="image" src="https://github.com/mlbarathy/user-sales-analysis/assets/43374951/2047a0ad-2f01-42ab-897a-8265356c8e1e">
<br>

### Average Quantity Per Order
<img width="1476" alt="image" src="https://github.com/mlbarathy/user-sales-analysis/assets/43374951/109f2c6d-9d2a-4b72-9a76-42a0464e2018">
<br>

### Top Selling Products - Quantity
<img width="1533" alt="image" src="https://github.com/mlbarathy/user-sales-analysis/assets/43374951/0e23af7d-1e16-45d5-98c8-731d20f64884">
<br>

### Top Selling Products - Price
<img width="1533" alt="image" src="https://github.com/mlbarathy/user-sales-analysis/assets/43374951/a6eff004-aba4-464d-aff8-390b6a10e823">
<br>

### Sales Trend - Monthly
<img width="1533" alt="image" src="https://github.com/mlbarathy/user-sales-analysis/assets/43374951/17b4c4c8-4f0a-4a74-b7c0-f4d593f68dc2">
<br>

### Sales Trend - Quarterly
<img width="1297" alt="image" src="https://github.com/mlbarathy/user-sales-analysis/assets/43374951/172aebe8-91a4-4145-a208-1407583c91d1">
<br>

### Sales Trend - Weather
<img width="1524" alt="image" src="https://github.com/mlbarathy/user-sales-analysis/assets/43374951/25900d0e-7fe6-4f58-be10-c19aa34f95fc">
<br>
<br>
















    
  

  
