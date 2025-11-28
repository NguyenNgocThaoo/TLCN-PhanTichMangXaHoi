FROM apache/superset:4.1.4

USER root

RUN mkdir -p /app/drivers/dremio
COPY drivers/dremio-jdbc-driver-25.0.5.jar /app/drivers/dremio/

RUN pip install jaydebeapi JPype1
RUN pip install sqlalchemy_dremio sqlalchemy_dremio[flight]
# ENABLE FEATURE FLAGS
ENV SUPERSET_FEATURE_FLAGS='{"ENABLE_FILTERS": true, "DASHBOARD_NATIVE_FILTERS": true}'


USER superset
