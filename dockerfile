FROM bitnami/spark:3.5.5


# Custom logging
COPY log4j2.properties /opt/bitnami/spark/conf/log4j2.properties

# any files/libraries you need on the cluster, install here ie:
# RUN pip install scipy
