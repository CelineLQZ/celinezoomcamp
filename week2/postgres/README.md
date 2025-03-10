# **PostgreSQL, pgAdmin, and Kestra Deployment**

## **1. Start PostgreSQL and pgAdmin Using Docker-Compose**  
Include **PostgreSQL** and **pgAdmin** in `docker-compose.yaml`, then start them using the following command:  
```sh
docker-compose up -d
```

## **2. Start Kestra Separately**  
Since `docker-compose.yml` does not include Kestra, you need to run it manually:  
```sh
docker run -d --name kestra \
  --network pg-network \
  -p 8081:8080 \
  --user=root \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -v /tmp:/tmp \
  kestra/kestra:latest server local
```

## **3. Access the Services**  
| Service  | URL                          |
|----------|------------------------------|
| pgAdmin  | [http://localhost:8082](http://localhost:8082) |
| Kestra   | [http://localhost:8081](http://localhost:8081) |

## **4. Connect to PostgreSQL**  
In **pgAdmin**, add a new server with the following configuration:  
- **Host:** `postgres`  
- **Port:** `5432`  
- **Database:** `postgres-zoomcamp`  
- **Username:** `kestra`  
- **Password:** `k3str4`  
