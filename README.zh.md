# cfipd
[English](README.md)

cfipd 是面向 **Cloudflare 站点 HTTPS 连接**的 relay，具备自适应边缘 IP 优选能力。
客户端使用统一的连接入口，Cloudflare IP 探测、健康检查和入口替换由 daemon 自动完成。

客户端保留原始 HTTPS 域名和 TLS SNI，与 Cloudflare 建立端到端 TLS 会话。
cfipd 透传加密数据，不解密或改写 HTTP 请求。

支持 IPv4 与 IPv6，根据网络状态自适应调整探测频率，并提供只读 Web 面板。
发布产物包含静态二进制和带 systemd 服务的 Debian 安装包。默认接受东京、大阪、香港和
新加坡的入口，可通过配置调整目标地区。

## Quick Start

在使用 systemd 的 Debian 或 Ubuntu 上，从
[GitHub Releases](https://github.com/breakertt/cfipd/releases/latest) 下载匹配机器架构的
`.deb`，在下载目录执行：

```bash
sudo apt install ./cfipd_*_"$(dpkg --print-architecture)".deb
sudo cfipd status

# 可选：启用面板，地址为 http://127.0.0.1:8080
sudo systemctl enable --now cfipd-web
```

安装后 daemon 自动启动，默认在 **8443** 端口接受需要转发的 HTTPS 连接。
一个 daemon 只有一个共享监听入口：所有客户端连接都使用同一个 relay 地址和端口，
每条新连接再转发到某个 Active IP 的 443 端口。即使有 3 个 Active，也只需一个监听端口。
例如，三条独立的 HTTPS 连接可以这样分配：

```text
HTTPS 连接 A --> cfipd:8443 --> Active IP 1:443
HTTPS 连接 B --> cfipd:8443 --> Active IP 2:443
HTTPS 连接 C --> cfipd:8443 --> Active IP 1:443
```

增加连接或更换 Active IP 都不需要新开端口。只有再启动一个独立 daemon，且原监听地址
已被占用时，才需要使用不同的监听地址或端口，并为它配置独立的数据目录和控制 socket。

首次启动需要发现并验证入口；
通过 `sudo cfipd actives` 查看可用入口。下面的命令经本机 relay 发送 HTTPS 请求：

```bash
curl --noproxy '*' \
  --connect-to speed.cloudflare.com:443:127.0.0.1:8443 \
  https://speed.cloudflare.com/cdn-cgi/trace
```

像示例一样调整客户端的建连目标，同时保留原始 HTTPS 域名和 TLS SNI。
该入口直接透传 TLS 流，不进行 HTTP 代理的 `CONNECT` 握手。
如需其他目标地区，修改 `/etc/cfipd.json` 中的 `allowed_colos`，然后重启 daemon。

## 安装与服务管理

每个支持的 Linux 目标架构都有独立二进制和 `.deb`：

| CPU | 二进制后缀 | Debian 架构 |
|---|---|---|
| x86-64 | `linux_amd64` | `amd64`（基础 x86-64 / GOAMD64=v1） |
| ARM64 | `linux_arm64` | `arm64` |
| ARMv7 | `linux_armv7` | `armhf` |
| 32 位 x86 | `linux_386` | `i386` |
| RISC-V 64 | `linux_riscv64` | `riscv64` |

每次发布均提供 `SHA256SUMS`。独立二进制不需要 Go 运行时或 libc；下载后赋予执行权限，
使用其 `run` 子命令启动。HTTPS 探测需要系统 CA 证书包。

Debian 包以专用系统用户 `cfipd` 运行 daemon：

| 项目 | 位置 |
|---|---|
| 配置 | `/etc/cfipd.json` |
| 学习状态与探测历史 | `/var/lib/cfipd/` |
| 控制 socket | `/run/cfipd/ctl.sock` |
| daemon / 面板服务 | `cfipd.service` / `cfipd-web.service` |

```bash
sudo systemctl restart cfipd          # 应用配置修改
systemctl status cfipd
sudo journalctl -u cfipd -f
sudo systemctl disable --now cfipd-web
```

升级保留本地配置和管理员禁用的服务。卸载保留学习状态与服务用户；purge 还会移除打包的
配置文件。如果已有手工管理的实例，安装前先备份数据并停止它。两个 daemon 不能共用同一
数据目录。

## 配置

systemd 服务明确读取 `/etc/cfipd.json`。直接运行时，cfipd 依次查找当前目录的
`cfipd.json` 和 `/etc/cfipd.json`；也可以用
`cfipd run -config /path/to/config.json` 指定配置。

配置字段覆盖内置默认值。安装包的配置可以写成：

```json
{
  "allowed_colos": ["NRT", "KIX", "HKG", "SIN"],
  "active_count": 3,
  "relay_listen": ":8443",
  "data_dir": "/var/lib/cfipd",
  "socket": "/run/cfipd/ctl.sock"
}
```

| 字段 | 用途 | 默认值 |
|---|---|---|
| `allowed_colos` | 接受的 Cloudflare 机房代码 | `NRT`、`KIX`、`HKG`、`SIN` |
| `zone_probe_host` | 一个已开橙云的普通主机名，用来淘汰对普通 zone 返回 Cloudflare 1034 错误的入口 IP；留空则不检查 | 空 |
| `active_count` | 现役入口数量 | `3` |
| `relay_listen` | HTTPS relay 监听地址；空字符串关闭 relay | `:8443` |
| `health_interval` | 健康检查的起始间隔 | `"10s"` |
| `scan_v6_ratio` | 新地址发现中分配给 IPv6 的比例 | `0.2` |
| `promote_min_mbps` | 转正所需的最低吞吐量 | `30` |

完整配置见 [config.go](https://github.com/breakertt/cfipd/blob/main/config.go)。
直接运行默认使用 `/tmp/cfipd.sock`，Debian 包通过配置改为 `/run/cfipd/ctl.sock`。
直接运行需要对数据和 socket 位置有写权限；若调整打包服务的这些路径，还需同步调整
systemd 的目录设置和权限。

## 命令行接口

安装包的控制命令需要 `sudo`，通过 Unix socket 与 daemon 通信。

| 命令 | 用途 |
|---|---|
| `cfipd status` | 池规模、健康、调度和 PK 状态 |
| `cfipd actives` / `cfipd standbys` / `cfipd candidates` | 查看各入口池 |
| `cfipd prefixes` | 查看前缀学习统计 |
| `cfipd relay` | 各入口的连接与流量统计 |
| `cfipd scan` | 安排一次新地址探测 |
| `cfipd import FILE` | 将 IP 列表或 CloudflareSpeedTest CSV 导入为候选种子 |
| `cfipd promote IP` | 验证一个 Standby，尝试补入空缺的现役位置 |
| `cfipd quarantine IP` | 让一个地址冷却后重新验证 |
| `cfipd version` | 查看版本、源码提交和构建时间 |

也可直接执行 `cfipd web` 启动面板，默认监听 `127.0.0.1:8080`，用 `-listen` 调整地址。
只读 API 为 `/api/status`、`/api/standbys` 和 `/api/relay`。

## 工作机制

地址在三个角色之间流转：

| 角色 | 用途 | 默认容量 |
|---|---|---|
| Candidate | 对有价值的新发现进行短期验证 | 最多 256；每个 /24 或 /48 最多两个 |
| Standby | 已验证的备用入口，等待转正或参与比较 | 最多 8 |
| Active | 为新转发的 HTTPS 连接选择的现役入口 | 3 |

扫描器在内置 Cloudflare 网段中采样，根据前缀级探测结果学习。失败或不符合目标地区的
发现仍反馈给前缀学习，但不形成永久重试任务。候选验证和扫描强度取决于当前后备需求与
改进机会。

随着可用性稳定，Active 检查间隔从 10 秒逐步放慢到 30、60、120 秒，出现异常时加速复核。
两个优先后备约每五分钟检查，其余约每三十分钟检查。慢但仍可用的入口会继续服务，直到
有替代者通过完整验证。

默认配置下，择优替换采用同窗成对比较（PK）：双方都必须通过可用性检查，挑战者的 HTTPS 探测中位耗时
至少改善 20% 且 30ms，p90 不退步。至少五分钟后，再对同一对入口进行一次独立比较以确认
结果。之后进行带宽对比，挑战者至少达到 30Mbps，且不低于现役入口实测吞吐的 80%；
每天最多两次完整带宽对比。新状态的前 24 小时仅记录 PK 替换建议，不执行择优换位，
故障恢复照常工作；观察截止时间在重启后保留。

实现层通过 TCP 透传承载 HTTPS：每条新客户端连接按延迟权重选择一个 Active IP，
连接到该 Cloudflare 边缘入口（默认 443 端口），双向复制加密的 TLS 字节流。
TLS 握手和 HTTP 请求由客户端与 Cloudflare 处理。复用同一条 HTTPS 连接的请求继续
使用同一个上游；选 IP 发生在建连时，不是每个 HTTP 请求各选一次。
换位只影响新的 HTTPS 连接，已经建立的会话继续使用原上游。

## 数据与运行限制

- `state.json` 保存入口池、前缀反馈、调度时间和 PK 预算。`history.jsonl` 记录探测与事件，
  单段约 8MiB 时轮转，只保留最近三个归档，历史总共约 32MiB 以内；每删一个旧归档都写日志。
- 测量反映 daemon 所在机器到配置探测域名的路径。换网络后保留旧状态，重新学习；
  程序没有城市或运营商特判。
- 面板中的耗时是 HTTPS 请求耗时，不是纯 TCP RTT；带宽是历史验证结果，不是实时吞吐。
  默认下载测试上限为 99,999,999 字节和十秒。
- relay 承载基于 TCP 的 HTTPS，不支持基于 QUIC/UDP 的 HTTP/3。
  上游建连失败时会尝试其他 Active，已经建立的流若上游中断则无法挽救。
  在没有客户端流量且使用最慢默认检查间隔时，静默故障可能要约两分钟才被发现。
- relay 和面板均无认证。限制监听地址或网络访问范围；面板保持回环监听，或只开放给可信
  网络。

## 开发

使用 `go.mod` 要求的 Go 版本（当前为 Go 1.27 或更高）。项目使用 Go 标准库，不依赖额外的
运行时组件。

```bash
go build -o cfipd .
go vet ./...
go test -race -count=1 ./...
```

在 Linux 上执行 `bash scripts/release.sh VERSION dist` 可构建全部目标架构与 Debian 包。
`scripts/test-package.sh` 会安装和移除服务，必须在一次性 systemd 测试机器运行。

CI 验证源码、交叉构建，并实测安装包。新 `v*` 标签通过验收后发布 GitHub Release，分支与
手动运行生成 CI artifact。[Release notes](https://github.com/breakertt/cfipd/releases)
提供版本历史，[AGENTS.md](https://github.com/breakertt/cfipd/blob/main/AGENTS.md) 说明贡献约定。
