FROM apache/spark:3.5.1

USER root
RUN python3 -m pip install --no-cache-dir numpy==1.24.4 \
    && mkdir -p /home/spark/.ivy2/cache /home/spark/.ivy2/jars \
    && chown -R spark:spark /home/spark/.ivy2
USER spark
