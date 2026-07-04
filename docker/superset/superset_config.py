import os

# Use the Postgres database defined in docker-compose
SQLALCHEMY_DATABASE_URI = os.environ.get("DATABASE_URL")

ROW_LIMIT = 100000
SQL_MAX_ROW = 100000
VIZ_ROW_LIMIT = 100000
