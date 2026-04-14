Fonda Backend

Fonda is a unified backend platform that integrates multiple food delivery services and POS systems into a single, centralized system. It allows merchants to manage orders, payments, analytics, and operations without switching between different apps.

📌 Overview

Fonda is designed as an isolation-oriented integration layer, enabling seamless communication between:

Delivery platforms (Uber Eats, Grubhub, DoorDash)
POS systems (Square, Clover)
Payment systems (Stripe)
Communication services (Twilio)

👉 Merchants do not need to change their existing systems — Fonda acts as a central hub to manage everything.

✨ Key Features
🔄 Multi-platform Integration
Uber Eats
Grubhub
DoorDash
🧾 POS Integration
Square
Clover
💳 Payment Processing
Powered by Stripe
📲 Communication
SMS & notifications via Twilio
📦 Order Management
View all orders from multiple platforms
Accept / Reject orders
Real-time order updates
📊 Dashboard & Analytics
Unified transaction view
Financial insights & reporting
Automated payout tracking
💸 Finance Automation
Centralized payment reconciliation
Automated payouts to merchants
🏗️ Architecture

Fonda backend is built using a scalable microservices-friendly architecture:

Backend
Python + Flask framework
RESTful APIs
Dockerized services
Deployment
AWS Lambda (serverless compute)
AWS ECS (container orchestration)
Docker containers
☁️ AWS Services Used
Database: Amazon RDS
Messaging: Amazon SQS, Amazon SNS
Storage: Amazon S3
Analytics: Amazon QuickSight
Scheduling: AWS EventBridge (cron jobs)
Email: Amazon SES
