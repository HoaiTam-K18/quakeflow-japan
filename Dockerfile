FROM bitnami/spark:latest

# Bitnami đã có user 1001, chỉ cần tạo thư mục làm việc
USER root
RUN mkdir -p /opt/spark/work-dir && \
    chown -R 1001:1001 /opt/spark/work-dir

# Thiết lập môi trường
ENV HOME=/opt/spark/work-dir
ENV SPARK_HOME=/opt/bitnami/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

WORKDIR /opt/spark/work-dir
USER 1001