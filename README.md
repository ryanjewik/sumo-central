# Sumopedia — A Full-Stack Sumo Community Platform

Sumopedia is a modern full-stack platform built for sumo wrestling fans, analysts, and data enthusiasts. It provides real-time match experiences, predictive analytics, community discussion features, and rich informational pages on rikishi and basho history — all backed by a scalable, cloud-first architecture.

## 🚀 Features

### Community
- Discussion boards for basho, rikishi, and general topics
- User profiles with activity and voting history
- SEO-optimized dynamic routing via Next.js App Router

### Real-Time Match Experience
- Live match updates and scoring
- Real-time match voting powered by Redis Pub/Sub and WebSockets
- XGBoost model provides predictive insights for match outcomes

### Rikishi & Basho Explorer
- Detailed rikishi profiles (career statistics, stable, rank history)
- Basho pages with torikumi, daily results, historical records, and stats
- MongoDB-hosted documents enable fast, flexible data queries

### Data Engineering & Orchestration
- Apache Airflow orchestrates ingestion, ETL, and ML pipelines
- Apache Spark performs distributed processing for:
  - Cleaning raw Sumo-API batches
  - Feature engineering
  - Statistical rollups and summaries
- ML scoring and data outputs stored in S3, MongoDB, and PostgreSQL

## 🧠 Machine Learning
- XGBoost model predicts the probability of "westWin" for upcoming matches
- Data pipeline:
  1. Raw webhook events ingested via Gin backend
  2. Airflow DAG triggers a Spark ETL job
  3. Spark generates features and runs the model
  4. Predictions stored in MongoDB/Postgres for frontend consumption

## 🏗️ Tech Stack

### Frontend
- Next.js 16 with App Router
- SSR for SEO and dynamic content
- SSG for static/archival pages

### Backend
- Go + Gin
- Webhooks, REST API, WebSocket gateway
- Integrates with Postgres, MongoDB, S3, and Redis

### Data Layer
- PostgreSQL, MongoDB Atlas, Redis, AWS S3

### Orchestration
- Airflow with Dockerized tasks
- Spark ETL + ML pipelines

### Infrastructure
- Docker & Docker Compose
- Nginx reverse proxy
- AWS EC2 with Cloudflare proxy
- HTTPS via Let’s Encrypt

## 📚 Architecture Overview

```
              Cloudflare Proxy
                     │
                     ▼
         ┌─────────────────────────┐
         │       Nginx Proxy       │
         └─────────────▲──────────┘
                       │
                       │ Internal Docker Network
                       │
     ┌─────────────────┴─────────────────────┐
     │                                       │
┌────▼────┐                         ┌────────▼────────┐
│ Next.js │                         │     Gin API     │
│Frontend │                         │   (Go Backend)  │
└────▲────┘                         └────────▲────────┘
     │ API calls                               │
     │                                          │ DB queries
┌────┴──────────┐                    ┌──────────┴─────────┐
│   PostgreSQL   │                    │   MongoDB Atlas    │
└────────────────┘                    └────────────────────┘
```

## 📈 Roadmap
- Prediction accuracy dashboard
- Improved SEO metadata
- Leaderboards & achievements
- MongoDB Atlas Search
- Additional ML models
- Mobile app wrapper

## 📝 License
MIT License

## 🙌 Acknowledgments
- Sumo API community
- Next.js, Gin, Spark, Redis, Airflow communities
