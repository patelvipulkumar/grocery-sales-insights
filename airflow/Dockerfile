FROM apache/airflow:3.0.0

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY entrypoint.sh /entrypoint.sh
USER root
RUN chmod +x /entrypoint.sh
ARG TERRAFORM_VERSION=1.8.5
RUN apt-get update \
	&& apt-get install -y --no-install-recommends wget unzip openjdk-17-jre-headless \
	&& wget -q https://releases.hashicorp.com/terraform/${TERRAFORM_VERSION}/terraform_${TERRAFORM_VERSION}_linux_amd64.zip -O /tmp/terraform.zip \
	&& unzip /tmp/terraform.zip -d /usr/local/bin \
	&& chmod +x /usr/local/bin/terraform \
	&& rm -f /tmp/terraform.zip \
	&& rm -rf /var/lib/apt/lists/*
USER airflow

ENV AIRFLOW__CORE__LOAD_EXAMPLES=False
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

ENTRYPOINT ["/entrypoint.sh"]
