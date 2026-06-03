FROM hhlh1/ubuntu-fastdds-suite:v3.2.2 AS stats-provider

FROM hhlh1/ubuntu-fastdds-suite:v3.2.2-secure-gen AS builder

WORKDIR /app

RUN apt-get update && \
    apt-get install -y libcurl4-openssl-dev && \
    rm -rf /var/lib/apt/lists/*

RUN rm -rf /usr/local/eprosima/fastdds
COPY --from=stats-provider /usr/local/eprosima/fastdds /usr/local/eprosima/fastdds
COPY --from=stats-provider /usr/local/eprosima/fastdds_statistics_backend /usr/local/eprosima/fastdds_statistics_backend

ENV CMAKE_PREFIX_PATH="/usr/local/eprosima/fastdds:/usr/local/eprosima/fastdds_statistics_backend"
ENV LD_LIBRARY_PATH="/usr/local/eprosima/fastdds/lib:/usr/local/eprosima/fastdds_statistics_backend/lib:${LD_LIBRARY_PATH}"

COPY . /app

RUN cmake -S /app -B /app/build && \
    cmake --build /app/build --parallel "$(nproc)"


FROM hhlh1/ubuntu-fastdds-suite:v3.2.2-secure-gen

WORKDIR /app

RUN apt-get update && \
    apt-get install -y libcurl4-openssl-dev && \
    rm -rf /var/lib/apt/lists/*

RUN rm -rf /usr/local/eprosima/fastdds
COPY --from=stats-provider /usr/local/eprosima/fastdds /usr/local/eprosima/fastdds
COPY --from=stats-provider /usr/local/eprosima/fastdds_statistics_backend /usr/local/eprosima/fastdds_statistics_backend

ENV LD_LIBRARY_PATH="/usr/local/eprosima/fastdds/lib:/usr/local/eprosima/fastdds_statistics_backend/lib:${LD_LIBRARY_PATH}"

COPY --from=builder /app/build/csv_publisher .
COPY --from=builder /app/build/csv_subscriber .
COPY --from=builder /app/build/csv_intermediate_store .
COPY --from=builder /app/build/csv_analysis_aggregator .
COPY --from=builder /app/build/csv_final_agg_subscriber .
COPY --from=builder /app/build/monitor .
COPY data/ /app/data/

RUN chmod +x csv_publisher \
             csv_subscriber \
             csv_intermediate_store \
             csv_analysis_aggregator \
             csv_final_agg_subscriber \
             monitor