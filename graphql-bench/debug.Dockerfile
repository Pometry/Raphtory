FROM debian:stable-slim
# USER root

RUN apt-get update && apt-get -y install curl xz-utils
RUN curl --proto '=https' --tlsv1.2 -LsSf https://github.com/mstange/samply/releases/download/samply-v0.13.1/samply-installer.sh | sh
# RUN echo '1' > /proc/sys/kernel/perf_event_paranoid
# RUN sh -c '1 >/proc/sys/kernel/perf_event_paranoid'

COPY --from=raphtory:debug-base /raphtory-graphql /raphtory-graphql

ENTRYPOINT ["sh", "-c", "/root/.cargo/bin/samply record --save-only --output /profiles/profile.json.gz /raphtory-graphql"]
