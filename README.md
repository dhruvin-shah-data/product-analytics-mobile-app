# SciPlay Game Analytics Project


# Overview

This repository contains an end-to-end analytics project simulating a mobile gaming studio workflow. The goal is to transform raw gameplay and transaction data into actionable insights addressing game health, player engagement, LiveOps effectiveness, and monetization performance.

# Business Context

Mobile game teams require reliable metrics to monitor stability, understand player behavior, evaluate LiveOps initiatives, and optimize revenue. This project answers those needs through clearly defined stakeholder questions and KPI-driven dashboards.

# Stakeholder Questions

Is the game stable and healthy over time?
How deeply and frequently are players engaging with the game?
Which LiveOps cohorts drive higher player engagement?
Which regions and user segments generate the most revenue?
Which offers and gameplay mechanics contribute most to monetization and activity?

# Data Modeling Approach

Implemented a layered analytics medallion architecture:
**Bronze:** Raw ingested data
**Silver:** Cleaned and standardized datasets
**Gold:** Business-ready metric tables aligned to KPIs

This approach ensures data quality, scalability, and clear metric ownership.

# Dashboards & Insights

**1. Game Health Overview**

**Business Question:** 
a. Is the game stable and healthy over time?

Key KPIs:

Crashed Sessions, 
Installs vs DAU, 
Session Volume, 
Daily Revenue, 

<img width="1863" height="687" alt="image" src="https://github.com/user-attachments/assets/d15c13b6-4631-4569-9932-3c27083ba33b" />



**2. Player Engagement & LiveOps Impact**

**Business Questions:**

b. How deeply and frequently are players engaging with the game?

c. Which LiveOps cohorts drive higher player engagement?

Key KPIs:

Average Session Duration, 
Average Sessions per User, 
Engagement by LiveOps Cohort

<img width="1863" height="754" alt="image" src="https://github.com/user-attachments/assets/6d330b9d-accb-4a11-a722-7dd4ea85d451" />



**3. Monetization & Gameplay Performance**

**Business Questions**
d. Which regions and user segments generate the most revenue?

e. Which offers and gameplay mechanics contribute most to monetization and activity?

Key KPIs:

Revenue by Country
ARPU vs Payer Revenue, 
Successful Purchases by Offer, 
Spins by Machine

<img width="1859" height="757" alt="image" src="https://github.com/user-attachments/assets/cccad43a-40a2-46dc-b18c-46850f47aeeb" />



# Key Learnings

a. Translating stakeholder questions into well-defined KPIs is more impactful than starting from available data or visuals.

b. Early alignment with stakeholders on metric definitions prevents misinterpretation and rework later in the analytics process.

c. A layered data model (Bronze → Silver → Gold) improves data reliability and makes business logic easier to maintain and explain.

d. Clean, business-friendly metric naming significantly improves dashboard usability and stakeholder trust.

e. Consistent visual design choices (color, layout, labeling) help stakeholders focus on insights rather than chart mechanics.

f. Documenting assumptions and decisions is as important as writing correct SQL when building analytics projects.

# Tools & Technologies

Databricks
SQL

Notion

Python (for checks)

Tableau

GitHub

# Notes
This is an academic project using fictional or simulated data. No confidential or proprietary information is included
