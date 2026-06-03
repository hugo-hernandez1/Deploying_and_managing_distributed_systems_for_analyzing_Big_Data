FROM hhlh1/ubuntu-fastdds-suite:v3.2.2

RUN apt-get update && apt-get install -y \
    xvfb \
    x11vnc \
    && rm -rf /var/lib/apt/lists/*

COPY start.sh /start.sh
RUN chmod +x /start.sh

EXPOSE 5900

CMD ["/start.sh"]