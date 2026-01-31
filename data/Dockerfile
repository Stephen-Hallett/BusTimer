# Dockerfile for a Postgres DB that runs any SQL in init.sql on first start.
# Note: run pgAdmin as a separate container (example shown below).
# Build: docker build -t bustimer-db .
# Run Postgres: docker run -d --name bustimer-db -p 5432:5432 -e POSTGRES_USER=bususer -e POSTGRES_PASSWORD=buspass -e POSTGRES_DB=busdb bustimer-db
# Run pgAdmin: docker run -d --name pgadmin -p 8080:80 \
#   -e PGADMIN_DEFAULT_EMAIL=admin@example.com -e PGADMIN_DEFAULT_PASSWORD=admin \
#   --link bustimer-db:postgres dpage/pgadmin4

FROM postgres:15

# Default DB credentials (can be overridden at runtime)
ENV POSTGRES_USER=bususer \
    POSTGRES_PASSWORD=buspass \
    POSTGRES_DB=busdb

COPY init.sql /docker-entrypoint-initdb.d/

EXPOSE 5432

HEALTHCHECK --interval=10s --timeout=5s --start-period=10s --retries=5 \
  CMD pg_isready -U "$POSTGRES_USER" || exit 1

# The official postgres image provides the correct ENTRYPOINT/CMD.