# Project Context: GPHub Backend (F1 API)

Architecture Note: This workspace is completely isolated. It only contains the backend API and data ingestion logic. The consumer of this API is an external Android application (GPHub) developed in a separate repository. Your ONLY job is to fetch raw F1 data, store it locally via SQLite, and expose it via clean, fast REST endpoints (JSON) for the mobile client to consume.

## 1. Project Overview
This project is the backend infrastructure for a mobile application (currently Android). The goal is to build a self-hosted, real-time F1 API that ingests official Formula 1 telemetry/timing data, processes it, and serves it to the mobile client via REST APIs and WebSockets.

## 2. Client Requirements (The Frontend)
- The mobile app requires highly optimized JSON responses.
- The app needs historical data (e.g., driver stats, season standings) and live session data (e.g., lap times, driver positions).
- Data must be easily parseable. Avoid deep, overly complex nested JSON objects where a flat structure is sufficient.

## 3. Future Scaling (Freemium Model)
The application will eventually have two tiers:
- **Free Tier:** Access to basic endpoints (news RSS, standings, historical stats) with potential rate-limiting.
- **Premium Tier:** Access to real-time WebSockets, push notifications, and high-frequency data updates. 
*Note for the AI: Keep this architecture in mind. Endpoint routing and database models should eventually support user roles or API key validation.*

## 4. Current Phase Focus
For this initial phase, we are strictly focusing on:
1. Setting up the SQLite database to store basic session data.
2. Building the FastAPI endpoints to serve static data.
3. Establishing the SignalR ingestion script to read live F1 data (without focusing on authentication yet).