#!/bin/sh
set -eu

module=/module/redis_amxx_i386.so
if [ ! -r "$module" ]; then
    echo "Redis runtime gate: module artifact is missing or unreadable: $module" >&2
    exit 64
fi

install -m 0755 "$module" cstrike/addons/amxmodx/modules/redis_amxx_i386.so

cat > cstrike/liblist.gam <<'EOF'
game "Counter-Strike"
gamedll "dlls\\mp.dll"
gamedll_linux "addons/metamod/metamod_i386.so"
type "multiplayer_only"
secure "1"
EOF

mkdir -p cstrike/addons/metamod cstrike/addons/amxmodx/configs
cat > cstrike/addons/metamod/plugins.ini <<'EOF'
linux addons/amxmodx/dlls/amxmodx_mm_i386.so
EOF

cat > cstrike/addons/amxmodx/configs/modules.ini <<'EOF'
redis
EOF

cat > cstrike/addons/amxmodx/configs/plugins.ini <<'EOF'
redis_xadd_validation_test.amxx
redis_runtime_test.amxx
EOF

cat > cstrike/server.cfg <<EOF
hostname "Redis module runtime gate"
sv_lan 1
rcon_password "${RCON_PASSWORD:?RCON_PASSWORD is required}"
redis_xadd_validation_host "redis"
redis_xadd_validation_port "6379"
redis_runtime_host "redis"
redis_runtime_port "6379"
EOF

exec ./hlds_run -game cstrike -console -port 27015 \
    +map de_dust2 +maxplayers 4 +exec server.cfg
