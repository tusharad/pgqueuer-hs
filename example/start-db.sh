docker stop queue-db
docker rm queue-db
docker run \
  --name queue-db -d \
  -e POSTGRES_USER=queue_user \
  -e POSTGRES_PASSWORD=queue_pass \
  -e POSTGRES_DB=queue_db \
  -p 5432:5432 \
  postgres:16
