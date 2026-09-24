# 🚀 Reddit Airflow ETL

An Apache Airflow ETL pipeline that collects posts from Reddit,
categorizes them based on keywords, stores useful posts in SQLite,
and optionally sends notifications to Slack.

## 📌 Project Overview

This project demonstrates a simple ETL workflow using Apache Airflow.

The pipeline:

1. Connects to Reddit using the Reddit API
2. Fetches new posts from `r/dataengineering`
3. Categorizes posts based on their content
4. Identifies high and medium priority posts
5. Stores useful posts in SQLite
6. Sends optional Slack notifications
7. Runs automatically every hour

## 🏗️ Architecture

```text
              Reddit API
                  │
                  ▼
          Fetch Reddit Posts
                  │
                  ▼
           Check Posts
             /       \
            /         \
       No Posts      Posts
          │             │
          ▼             ▼
       Logging      Categorization
                         │
                         ▼
                    Filter Posts
                         │
                         ▼
                    SQLite DB
                         │
                         ▼
                  Slack Notification
