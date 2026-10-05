# Microservices CLI Cheatsheet: Docker, Redis, PostgreSQL & Kafka

A comprehensive reference guide for running, inspecting, and debugging our microservices architecture.

---

## 1. Active Service Ports & Endpoints

| Service / Container | Container Name | Host Port | Purpose | Verification URL / Command |
| :--- | :--- | :---: | :--- | :--- |
| **Eureka Server** | `discover-service` | `8761` | Service Registry & Discovery | [http://localhost:8761](http://localhost:8761) |
| **API Gateway** | `api-gateway` | `8090` | Single Entry Point & Routing | [http://localhost:8090](http://localhost:8090) |
| **User Service** | `user-service` | `8081` | Authentication & User Management | `http://localhost:8081/api/users/get/1` |
| **Product Service** | `product-service` | `8082` | Catalog & Product Cache | `http://localhost:8082/api/products` |
| **Order Service** | `order-service` | `8083` | Order Management & Kafka Producer | `http://localhost:8083/api/orders` |
| **Payment Service** | `payment-service` | `8084` | Payment Processing & Idempotency | `http://localhost:8084/api/payments` |
| **Notification Service**| `notification-service` | `8085` | Kafka Event Consumer (Email/SMS) | Check logs: `docker logs -f notification-service` |
| **Redis Cache** | `redis` | `6379` | In-Memory RAM Cache & TTL | `docker exec redis redis-cli ping` |
| **PostgreSQL** | `postgres` | `5432` | Relational Persistence (Multi-DB) | `docker exec postgres psql -U postgres` |
| **Apache Kafka** | `kafka` | `9092` | KRaft Event Streaming Broker | `docker exec kafka ...` |

---

## 2. Docker & Docker Compose Commands

### General Container Management
```powershell
# View all currently running containers
docker ps

# View all containers including stopped/crashed ones
docker ps -a

# Start all containers in the background (run from parent Microservice directory)
docker compose up -d

# Rebuild code changes and restart all containers
docker compose up --build -d

# Restart only one specific service (e.g. after code update)
docker compose up -d --build --no-deps user-service

# Stop all containers safely without losing database data
docker compose down

# Follow live logs of a specific service
docker logs -f user-service
docker logs -f api-gateway
docker logs -f redis
docker logs -f postgres
```

---

## 3. Redis Commands (`redis-cli`)

Our running Redis container is named **`redis`**.

### One-Liner Quick Commands (Fast Inspection from PowerShell)
```powershell
# 1. Test if Redis is alive
docker exec redis redis-cli ping

# 2. List all stored keys in Redis
docker exec redis redis-cli keys "*"

# 3. Read cached JSON object for user 1
docker exec redis redis-cli get "users::1"

# 4. Check remaining TTL (time-to-live) in seconds
docker exec redis redis-cli ttl "users::1"

# 5. Delete / Evict a specific cache key manually
docker exec redis redis-cli del "users::1"

# 6. Flush / Delete all cached entries across all namespaces
docker exec redis redis-cli flushall
```

### Interactive Redis Terminal Mode
```powershell
# Open live redis-cli shell inside container
docker exec -it redis redis-cli

# Commands inside redis-cli:
KEYS *
GET "users::1"
TTL "users::1"
DEL "users::1"
MONITOR     # Streams every command sent to Redis live as you click in Postman!
exit        # Exit back to Windows PowerShell
```

---

## 4. PostgreSQL Database Commands (`psql`)

Our running PostgreSQL container is named **`postgres`**.

### One-Liner Quick Queries (From PowerShell)
```powershell
# View all users registered in user_db
docker exec postgres psql -U postgres -d user_db -c "SELECT id, name, email, role FROM users;"

# View product records in product_db
docker exec postgres psql -U postgres -d product_db -c "SELECT * FROM products;"

# View orders in order_db
docker exec postgres psql -U postgres -d order_db -c "SELECT * FROM orders;"

# View payment records in payment_db
docker exec postgres psql -U postgres -d payment_db -c "SELECT * FROM payments;"
```

### Interactive `psql` Terminal Mode
```powershell
# Open interactive SQL shell inside user_db
docker exec -it postgres psql -U postgres -d user_db

# Useful psql meta-commands:
\l                  # List all databases (user_db, product_db, order_db, etc.)
\c <database_name>  # Switch database (e.g., \c product_db)
\dt                 # List all tables in the active database
\d <table_name>     # Describe table structure and columns (e.g., \d users)
SELECT * FROM users;
\q                  # Exit back to Windows PowerShell
```

---

## 5. Apache Kafka Commands

Our running Kafka broker container is named **`kafka`**.

```powershell
# 1. List all active Kafka topics
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

# 2. Create a test topic manually
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic order-created-events --partitions 3 --replication-factor 1

# 3. Listen to live messages published to a topic (from beginning)
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic order-created-events --from-beginning

# 4. View Consumer Groups and Partition Offsets (Lag monitoring)
docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list
```

---

## 6. Windows PowerShell Port & Network Debugging

```powershell
# Check if ports are active on Windows
Get-NetTCPConnection -LocalPort 8081 -ErrorAction SilentlyContinue   # User Service
Get-NetTCPConnection -LocalPort 8090 -ErrorAction SilentlyContinue   # API Gateway
Get-NetTCPConnection -LocalPort 8761 -ErrorAction SilentlyContinue   # Eureka Dashboard
Get-NetTCPConnection -LocalPort 6379 -ErrorAction SilentlyContinue   # Redis
Get-NetTCPConnection -LocalPort 5432 -ErrorAction SilentlyContinue   # PostgreSQL
Get-NetTCPConnection -LocalPort 9092 -ErrorAction SilentlyContinue   # Kafka

# Find which PID is holding a port
netstat -ano | findstr :8081

# Kill a frozen process occupying a port
taskkill /F /PID <PID>
```
