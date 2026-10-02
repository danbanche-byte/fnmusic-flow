#!/bin/sh
set -eu

cd /app/app/integrated/pockettune-server
./node_modules/.bin/tsx index.ts &
gateway_pid=$!

# lx-music 自定义源宿主：让用户导入的「lx-music 自定义源脚本」在服务端原样运行，
# 不需要额外部署中转服务，也不需要用户开着 lx-music 客户端。
# 只监听回环地址，日志落到数据卷，方便在「音源管理 → 查看脚本日志」之外直接翻看。
export LX_HOST_BIND="${LX_HOST_BIND:-127.0.0.1}"
export LX_HOST_PORT="${LX_HOST_PORT:-18432}"
# 日志优先写数据卷；万一数据卷不可写就退回 /tmp，避免重定向失败拖垮整个启动脚本
lx_log_dir=/config
if [ ! -d "$lx_log_dir" ] || [ ! -w "$lx_log_dir" ]; then
  lx_log_dir=/tmp
fi

# 宿主看护循环：进程退出（崩溃/被杀）后 5 秒自动重启，不再需要重启容器才能恢复。
# 每轮启动前把超过 10MB 的日志轮转为 .1，避免无限增长吃满数据卷。
lx_rotate_log() {
  lx_log="$lx_log_dir/lxnode.log"
  lx_size=$(stat -c %s "$lx_log" 2>/dev/null || echo 0)
  if [ "$lx_size" -gt 10485760 ]; then
    mv "$lx_log" "$lx_log.1" 2>/dev/null || true
  fi
}
lx_supervise() {
  while :; do
    lx_rotate_log
    node /app/app/lxnode/host.mjs >> "$lx_log_dir/lxnode.log" 2>&1 &
    echo $! > /tmp/lxnode.pid
    wait $!
    lx_code=$?
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] lxnode host exited (code=$lx_code); restarting in 5s" >> "$lx_log_dir/lxnode.log" 2>/dev/null || true
    sleep 5
  done
}
lx_supervise &
lx_host_pid=$!

fnos_proxy_pid=""
if [ -S /var/run/trim_cgi.socket ]; then
  socat TCP-LISTEN:5666,bind=127.0.0.1,reuseaddr,fork UNIX-CONNECT:/var/run/trim_cgi.socket &
  fnos_proxy_pid=$!
fi

cleanup() {
  kill "$gateway_pid" 2>/dev/null || true
  kill "$lx_host_pid" 2>/dev/null || true
  kill "${gw_socat_pid:-}" 2>/dev/null || true
  # 看护循环是子 shell，还要杀掉它名下的 node 宿主，避免残留占用端口
  if [ -f /tmp/lxnode.pid ]; then
    kill "$(cat /tmp/lxnode.pid 2>/dev/null)" 2>/dev/null || true
  fi
  if [ -n "$fnos_proxy_pid" ]; then
    kill "$fnos_proxy_pid" 2>/dev/null || true
  fi
}
trap cleanup INT TERM EXIT

# 宿主是本地进程，正常几百毫秒内就绪；最多等 5 秒，等不到也照常启动（解析时会自动重试装载）
for _i in 1 2 3 4 5; do
  curl -fsS "http://127.0.0.1:${LX_HOST_PORT}/health" >/dev/null 2>&1 && break
  sleep 1
done

sleep 1
cd /app
# 2026-09-29（1.0.9）：统一网关接入 —— 官方约定 socket 放 TRIM_APPDEST 即
# /var/apps/<app>/target/ 下，文件名与 ui/config 的 gatewaySocket 一致，
# fnOS nginx 以 /app/<app>/ 路径（SSO 登录态）反代到此 socket，内外网桌面图标统一可用。
# 2026-10-01（1.3.1）：**架构修正** —— 此前 UDS 与 TCP 各起一个 uvicorn 进程，
# 两个实例内存各自独立：经网关导入的音源只存在于 UDS 实例，TCP 实例的下载
# worker 看不到（反之亦然），还意味着两个并发 worker 有重复下载风险。
# 现改为 socat 把网关 socket 转发到 TCP 8000：全应用只剩**一个** uvicorn 进程、
# 一份内存注册表、一个 worker，网关与直连端口看到的状态永远一致。
GW_SOCK="/gw-target/fnmusic-flow-community.sock"
rm -f "$GW_SOCK"
socat UNIX-LISTEN:"$GW_SOCK",fork TCP:127.0.0.1:8000 &
gw_socat_pid=$!
for _i in 1 2 3 4 5 6 7 8 9 10; do
  [ -S "$GW_SOCK" ] && break
  sleep 0.5
done
chmod 666 "$GW_SOCK" 2>/dev/null || true
exec uvicorn app.production:app --host 0.0.0.0 --port 8000
