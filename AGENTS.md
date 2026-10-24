# AGENTS.md

本文件是 cfipd 仓库内代码 agent 的项目约定；用户的当前指令优先。
使用方式和设计说明见 [README.md](README.md)，默认参数以 `cmd/cfipd/config.go` 为准，构建矩阵以
`scripts/release.sh` 为准。修改行为时同步更新相关文档，不在这里复制整套参数表。

除文档（`README.md`、`README.zh.md`、本文件）外，仓库内的一切——代码、注释、日志和错误信息、脚本、提交信息——一律使用英文。唯一的例外是 `web.html` 里的 `I18N.zh` 翻译表：面板是中英双语的，英文是源语言，按浏览器语言自动选择，右上角可切换并记住选择。

`README.md` 使用英文，第二行链接 `README.zh.md`；中文副本保持相同结构与含义。
顺序为项目介绍、Quick Start，再到配置和实现细节。README 面向使用者，不放交接、实验或
修补过程记录；版本历史放 Release notes，工作进度放仓库外的进度文档。

## 项目结构

cfipd 是 Go 标准库实现的 Cloudflare HTTPS relay、IP 优选 daemon 和只读 Web 面板。
对使用者描述 HTTPS 用途；实现层是 L4 TCP 透传，TLS 在客户端与 Cloudflare 之间端到端
建立，不在 relay 终止，也不实现 HTTP CONNECT 代理协议。

Go 源码是 `cmd/cfipd/` 下的单个 main 包，下表中的源文件都在这个目录。测试按被测文件
命名为 `<源文件>_test.go`，共享 fixture 放在 `fixtures_test.go`。

| 文件 | 职责 |
|---|---|
| `main.go` / `config.go` | CLI、版本信息、配置查找、加载、检查（`cfipd test`）和默认值 |
| `daemon.go` | 单事件循环、任务调度、异步结果应用和转正 |
| `pk.go` | 择优 PK：挑选配对、同窗比较、独立确认和带宽对照 |
| `bandwidth.go` | Active 带宽定期复测（低于转正下限需连续两次确认），确认后寻找替换 |
| `ip.go` | 角色、健康状态、真实观测窗口、候选生命周期和分散约束 |
| `probe.go` / `scanner.go` | HTTPS 探测、限时下载测速、地址采样和前缀反馈 |
| `store.go` | 原子 JSON 快照（逐条流式写出）、旧格式迁移、JSONL 流水和轮转 |
| `relay.go` | TCP 透传（共享 16KiB 复制缓冲）、供新连接读取的原子快照、真实流量反馈、按客户端 IP 的用量表（最多 256 个） |
| `memory.go` | Go 软内存上限：固定值，或按主机剩余内存每分钟调整 |
| `ctl.go` / `web.go` / `web.html` | Unix socket 控制接口和独立只读仪表盘（可选单密码 Basic 认证） |
| `packaging/` / `scripts/` / `.github/workflows/ci.yml` | systemd、Debian 包、构建和发布验收 |

## 实现约束

- 保持单事件循环拥有可变 pool/前缀状态。worker 只读取派发时的不可变输入，结果回到事件
  循环后校验角色和代数；代数按结果涉及的每个 IP 校验，不用全池代数，代数 0 同样要校验。
  relay 通过原子快照选路，快照带上各 Active 的代数。
- 每次物理探测保存真实时间、成功或失败，并准确计数。角色迁移不得伪造观测时间或丢弃
  失败样本；客户端连接统计与合成探测统计分开。HTTP 探测总耗时不能标为纯 TCP RTT。
- 所有带宽下载（转正、PK、复测、替换、手动 promote）都经 `longJobBlocked` 串行，并保持
  `speed_gap` 间隔（Active 不足一半时补位和手动 promote 例外）；遇到 HTTP 429 一律暂停。PK 带宽阶段由发起 PK 时的
  判断覆盖（期间一直占着 pkRunning）。新增下载路径必须走同一个判断。
- 调度按实际需求、收益和每条 IP 的截止时间驱动；容量与预算是上限，不是必须填满的配额。
  前缀 key、资格判据和架构映射各自只有一个权威实现，不复制逻辑。
- 慢但仍可用的 Active 保持服务；带宽低于转正下限的 Active 同样保持服务，直到有后备
  通过完整转正测试后一换一替换；只有另有 Active 达到下限时才降权和尝试替换。relay 权重和
  PK 带宽阶段只用 `score`（rtt^-(1-w)·Mbps^w，w 为 `bandwidth_weight`）比较，底线先于分数：
  relay 对超过 `rttLimit` 或确认低于转正下限的 Active 降权；PK 挑战者必须更快且达到带宽下限。择优替换必须完成新鲜的同窗 PK、同一对的独立确认和
  有效带宽对照，提交前复核角色、分散约束和代数（换位一换一，不改变 Active 数量）；
  原子换位不影响已经建立的 relay 连接。
- 快照迁移和重启保留真实验证进度、调度时间、PK 观察截止时间及每日计数/预算。
  日志轮转只保留最近 `histKeep` 个旧段，删除更旧的段时写日志，文档写明总磁盘上限。
- 默认机房选择可配置；不要加入城市、运营商或机器地址特判。学习数据只代表探测机器的
  出口，换出口时保留旧数据并使用新的数据目录重新学习。
- 优先最窄的契约修复，保留 Go 标准库实现；新增依赖或结构要有具体收益。先用行为测试
  复现缺陷，再修正；测试构造数据必须明确标为 fixture，不能宣称是生产业务测量。

## 验证

使用符合 `go.mod` 的 Go 工具链。Go 改动只格式化涉及的文件，并按改动范围运行：

```bash
go vet ./...
go test -race -count=1 ./...
go build ./...
git diff --check
```

文档改动做内容核对和 `git diff --check` 即可；不用为了文档重新跑网络探测。
打包改动在 Linux 构建 `bash scripts/release.sh VERSION dist`，核对 ELF 架构、静态链接、
包元数据和 `SHA256SUMS`。普通交叉编译成功不等于已经在对应 ISA 上执行过。

`scripts/test-package.sh` 会实际安装、启停和卸载服务，**只在一次性 systemd 测试机器运行**。
它验证自动启动、可选 Web、配置及管理员禁用状态保留、卸载后学习数据保留；测试网络隔离
不代表真实 Cloudflare 业务验收。状态目录受权限保护，检查时用 `sudo`，不要放宽服务权限
来让测试通过。

## Git、发布与部署

- 提交前检查工作区，推送前 fetch 并核对目标分支；不覆盖他人的修改、不改写已有标签。
  沿用已配置的 Git 身份，设置/改变身份或作者前询问用户；不添加 agent 署名或 Co-Author。
- main/PR/手动 CI 生成构建 artifact；新 `v*` 标签通过验收后才发布真正的 GitHub Release。
  需要发布时使用新版本标签，不为了修复失败流水线 force-retag。仅写文档不创建新版本。
- 发布验收检查 CI 结论、实际 Release 资产、架构/版本/源码提交和校验和；区分编译、安装
  生命周期、连通性与真实业务性能这些证据，不能相互替代。
- 生成或发布包不等于获准迁移生产服务。部署前核对目标修改、进程、连接、端口和数据属主，
  备份二进制、配置与一致状态，保留回滚路径；不要让两个 daemon 共用数据目录。
- Debian 包使用专用 `cfipd` 用户，配置 `/etc/cfipd.json`，状态 `/var/lib/cfipd`，socket
  `/run/cfipd/ctl.sock`。保留配置、管理员禁用状态和学习数据；Web 默认只绑定回环。
- 不提交实际配置、学习状态、历史流水、构建产物或凭据，不把 token 写入命令参数或日志。
  删除和清理遵循用户的可恢复要求，不使用强制覆盖或破坏性清理作为捷径。
- 长任务放在执行机器的 tmux 中；先确认会话状态，再发送输入。基础设施故障先报告，
  不顺手改变机器网络、认证或服务布局。

进度记录在 `~/.agents/projects/cfipd/PROGRESS.md`；计划在 `~/.agents/plans/cfipd/`。
它们属于本机工作状态，不提交进仓库；如有共享项目记忆，先读
`~/.agents/memory/cfipd/MEMORY.md` 及其索引。
