FROM debian:bookworm-slim

ARG REHLDS_VERSION=3.15.0.896
ARG REGAMEDLL_VERSION=5.30.0.814
ARG METAMOD_VERSION=1.3.0.149
ARG AMXX_BUILD=5467

ENV DEBIAN_FRONTEND=noninteractive

RUN dpkg --add-architecture i386 \
    && apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates curl lib32gcc-s1 libstdc++6:i386 libc6:i386 \
        procps tar unzip \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /opt/steamcmd /opt/hlds \
    && curl -fsSL https://steamcdn-a.akamaihd.net/client/installer/steamcmd_linux.tar.gz \
        | tar -xz -C /opt/steamcmd \
    && for attempt in 1 2 3; do \
        /opt/steamcmd/steamcmd.sh \
          +force_install_dir /opt/hlds \
          +login anonymous \
          +app_set_config 90 mod cstrike \
          +app_update 90 validate \
          +quit && break; \
      done \
    && test -x /opt/hlds/hlds_run

RUN curl -fsSL \
      "https://github.com/dreamstalker/rehlds/releases/download/${REHLDS_VERSION}/rehlds-bin-${REHLDS_VERSION}.zip" \
      -o /tmp/rehlds.zip \
    && unzip -q /tmp/rehlds.zip -d /tmp/rehlds \
    && cp -a /tmp/rehlds/bin/linux32/. /opt/hlds/ \
    && curl -fsSL \
      "https://github.com/rehlds/ReGameDLL_CS/releases/download/${REGAMEDLL_VERSION}/regamedll-bin-${REGAMEDLL_VERSION}.zip" \
      -o /tmp/regamedll.zip \
    && unzip -q /tmp/regamedll.zip -d /tmp/regamedll \
    && cp -a /tmp/regamedll/bin/linux32/cstrike/. /opt/hlds/cstrike/ \
    && curl -fsSL \
      "https://github.com/rehlds/Metamod-R/releases/download/${METAMOD_VERSION}/metamod-bin-${METAMOD_VERSION}.zip" \
      -o /tmp/metamod.zip \
    && unzip -q /tmp/metamod.zip -d /tmp/metamod \
    && mkdir -p /opt/hlds/cstrike/addons \
    && cp -a /tmp/metamod/addons/metamod /opt/hlds/cstrike/addons/ \
    && curl -fsSL \
      "https://www.amxmodx.org/amxxdrop/1.10/amxmodx-1.10.0-git${AMXX_BUILD}-base-linux.tar.gz" \
      -o /tmp/amxx-base.tar.gz \
    && tar -xzf /tmp/amxx-base.tar.gz -C /opt/hlds/cstrike \
    && curl -fsSL \
      "https://www.amxmodx.org/amxxdrop/1.10/amxmodx-1.10.0-git${AMXX_BUILD}-cstrike-linux.tar.gz" \
      -o /tmp/amxx-cstrike.tar.gz \
    && tar -xzf /tmp/amxx-cstrike.tar.gz -C /opt/hlds/cstrike \
    && chmod +x /opt/hlds/hlds_linux /opt/hlds/hlds_run \
    && rm -rf /tmp/rehlds* /tmp/regamedll* /tmp/metamod* /tmp/amxx-*

WORKDIR /opt/hlds

COPY redis.inc cstrike/addons/amxmodx/scripting/include/redis.inc
COPY redis_xadd_validation_test.sma cstrike/addons/amxmodx/scripting/redis_xadd_validation_test.sma
COPY tests/runtime/redis_runtime_test.sma cstrike/addons/amxmodx/scripting/redis_runtime_test.sma
COPY tests/runtime/entrypoint.sh /usr/local/bin/redis-runtime-entrypoint

RUN chmod +x /usr/local/bin/redis-runtime-entrypoint \
    && chmod +x cstrike/addons/amxmodx/scripting/amxxpc \
    && cd cstrike/addons/amxmodx/scripting \
    && ./amxxpc redis_xadd_validation_test.sma \
      -o../plugins/redis_xadd_validation_test.amxx \
    && ./amxxpc redis_runtime_test.sma \
      -o../plugins/redis_runtime_test.amxx

ENTRYPOINT ["/usr/local/bin/redis-runtime-entrypoint"]
