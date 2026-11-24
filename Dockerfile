FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /srv/app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY app ./app
COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

# 轮换状态（当前业务账号与服务端口令）落盘目录，由 compose 命名卷挂载；
# 目录预先创建，容器内以非 root 运行时也可写
RUN mkdir -p /var/lib/order-demo

EXPOSE 8000
ENTRYPOINT ["docker-entrypoint.sh"]
