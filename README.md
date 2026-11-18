# 制品下载切流代理（Artifact Relay）

单进程 HTTP GET 代理 + 切换 API：值班员把新下载切到新版上游，同时给旧版上游已受理的下载
留出收尾时间。源站更换访问密钥时，还可以按目标在进程内轮换回源凭据，代理始终只发送
自己绑定的凭据、不透传客户端的 `Authorization`。FastAPI 提供控制面，aiohttp 提供上游
连接，uvicorn 单 worker。

```
客户端 ──GET──▶ relay (FastAPI/uvicorn, 单进程) ──GET(绑定凭据)──▶ blue  (候选 A)
                       │  POST /api/switch                           green (候选 B)
                       │  POST /api/credentials  (本地管理密钥授权)
                       └─ 探活 GET <target>/ready（切流用当前凭据，轮换用候选凭据）
```

## 关键语义

| 需求 | 实现 |
| --- | --- |
| 候选探活 200 才切流 | `POST /api/switch` 先向候选的 `RELAY_PROBE_PATH`（默认 `/ready`）发 GET，仅 200 提交；其余状态码、超时、连接错误都算失败 |
| 失败保留现行目标 | 探活失败只写一条 failed 操作记录，不改 `current`，也不产生新代 |
| 后受理的切换废止先前未完成候选 | 新切换被受理时，旧候选立即标为 `superseded` 并取消探活任务。提交路径会再次校验“我仍是当前待定候选”，因此**探活已经返回 200 但尚未提交**的旧候选也无法翻流 |
| 迟到探活不得切回 | 同上：`_commit_if_pending` 在状态锁内比较 `self.pending is op`，不匹配直接丢弃 |
| 每个 GET 固定受理时的目标 | 请求进入路由函数后，**第一次 await 之前**同步读取 `state.current` 并在该代上登记；之后即使切流也不会改变这条请求的上游 |
| 长连接后续请求按新目标 | 目标解析发生在每个请求（而非每条连接）上，keep-alive/pipeline 的下一个请求重新读取当前代 |
| 切流后旧代可继续 5 秒 | 提交时给旧代开 drain 窗口（`RELAY_DRAIN_SECONDS`，默认 5s，状态里带剩余秒数）。窗口内旧代请求照常转发 |
| 到期只中断该代未结束请求 | 定时器只调用**那一代**的 `abort_all()`；每条代理请求只监听自己代的 abort 事件，响应体在下一个 chunk 边界停止发送并让 uvicorn 关闭该连接（客户端看到截断）。其他代（含新代）完全不受影响 |
| 连续切换、回切同址不得误断新代 | 每次成功切流都会新建**代**（epoch），即使目标 URL 与历史某代相同。drain 定时器绑定的是代对象而不是 target 名，所以“蓝→绿→蓝→绿”里第 2 代的到期绝不会影响第 4 代的连接 |
| 响应逐块转发、不整份缓存 | `upstream.content.iter_chunked(RELAY_CHUNK_SIZE)` 逐块 `send`；有测试断言首字节在整份响应结束前很早就到达客户端 |
| 慢探活不挡切换/查询 | 探活是独立任务，`POST /api/switch` 默认立刻返回 202（`wait=true` 才阻塞，且可设超时）；`/api/status` 是纯同步快照，不取锁、不 await 任何网络 |
| 慢下载不挡切换/查询 | 探活与代理使用**两个独立的 aiohttp session/连接池**；控制面不等待任何下载 |
| `/docs` 可切换、查目标与各代在途数 | OpenAPI 见 `/docs`；`GET /api/status` 返回当前目标/代、各代状态与 `in_flight`、进行中的请求明细；`/demo` 是可视化演示页 |

补充约定：

* **同址切换是 no-op**：候选目标就是当前目标时，探活仍需 200，但明确返回 `status: "noop"`，
  不新建代、不 drain —— 所以重复点“切到现在这版”不会误伤正在跑的下载。
* 探活只决定提交与否，**不读取探活响应体**：探活端点返回超慢/无限长的 body 也不会拖住切换。
* 旧代 drain 到期时，若该代已无在途请求，会立即标为 `closed`，状态快照保持如实。
* 只代理 GET/HEAD；WebSocket / `CONNECT` 不支持。上游返回的 3xx `Location` 原样透传
  （不重写 Location，所以跨代的绝对跳转不会自动改写到新目标）。
* 切流不重放任何请求：已经发出并失败的请求不会被自动重试，由客户端自行重试。

## 回源凭据轮换

源站更换访问密钥时，值班员通过管理 API 把新凭据提交给代理；代理先拿候选凭据请求目标的
`RELAY_PROBE_PATH`（默认 `/ready`），**只有 HTTP 200 才更新**，其余状态码、超时、连接错误
都沿用旧值。凭据按目标独立轮换，全程在单进程内存中完成（无外部存储）。

| 需求 | 实现 |
| --- | --- |
| 初始凭据来自配置 | 每个目标一个环境变量 `RELAY_TARGET_AUTH_<目标名>`，值是完整的 `Authorization` 头（如 `Bearer xxx`）；未配置则该目标不发认证头 |
| 管理 API 本地密钥授权 | 设置 `RELAY_ADMIN_KEY` 后，凭据接口要求 `X-Management-Key: <key>` 或 `Authorization: Bearer <key>`，常量时间比较；未设置时接口**失败关闭**（503），不允许匿名管理 |
| 提交新凭据先探活 | `POST /api/credentials` 用候选凭据请求目标 `/ready`，200 才提交；失败返回 `failed` 并保留旧凭据（与切换同一套后台探活/取代模型） |
| 受理时固定目标与凭据 | GET/HEAD 进入路由后、第一个 await 之前，同步读取当前代并绑定该目标当前凭据版本，登记为在途请求；凭据版本随响应头 `x-relay-credential-version` 返回 |
| 只发绑定凭据、不透传客户端 Authorization | 代理无条件剥掉客户端的 `Authorization`，仅追加绑定凭据；目标无凭据时上游收不到认证头 |
| 已受理下载不因轮换改路由 | 凭据轮换**不新建代、不触发 drain**；请求受理后持有的凭据对象不可变，轮换只影响之后受理的请求 |
| 切流后的旧代按现有规则排空 | 代(epoch)/drain 模型不变；每代请求同时记录受理时的凭据版本 |
| 探活不阻塞下载、切流、状态查询 | 轮换探活走独立的 probe 连接池（与下载池分离），API 立即返回 202，`/api/status` 为纯同步快照 |
| 迟到探活不得提交切流或覆盖新值 | 同一目标新轮换受理时旧候选立即 `superseded` 并取消探活；提交路径在锁内二次校验“仍是当前待定候选 + 凭据版本未变”，旧候选即使迟到拿到 200 也被丢弃 |
| 状态包含凭据版本 | `/api/status` 与 `/api/targets` 返回每目标 `version`/`configured`/`rotated_at`、当前目标的 `credential_version`、在途请求绑定的版本 |
| 不泄露密钥 | 所有管理响应、状态快照、日志只出现版本号/指纹/布尔位，绝不输出凭据值；上游 demo 的控制接口同样只回 SHA-256 指纹 |

轮换操作记录（`recent_credential_rotations`，最近 50 条）状态机：
`pending → committed / failed / superseded`，字段含 `from_version`/`to_version`/
`probe_status`/`reason`，同样不含任何密钥材料。

### 凭据轮换操作示例（compose 环境）

```bash
# 1) 先给源站换新密钥（demo 上游的运行时控制接口）
curl -X POST 'http://localhost:9001/_control/key?key=Bearer%20blue-secret-v2'
# 2) 把同一新值提交给代理；代理用它探活 /ready，200 才生效
curl -X POST http://localhost:8080/api/credentials \
  -H 'X-Management-Key: local-admin-key' -H 'content-type: application/json' \
  -d '{"target":"blue","authorization":"Bearer blue-secret-v2","wait":true}'
# 3) 查询版本（不回显密钥）
curl -H 'X-Management-Key: local-admin-key' http://localhost:8080/api/credentials
```

## 运行（Docker Compose）

```bash
docker compose up --build
```

| 地址 | 内容 |
| --- | --- |
| http://localhost:8080/docs | 切换 API、凭据轮换 API、状态查询 |
| http://localhost:8080/demo | 演示页（切换/轮换按钮 + 代/在途数/凭据版本 + 下载链接） |
| http://localhost:8080/api/status | 当前目标、各代状态/在途数、凭据版本、最近操作 |
| http://localhost:9001 | blue 上游（直连，用于调探活/换密钥） |
| http://localhost:9002 | green 上游（直连，用于调探活/换密钥） |

compose 里两个上游默认**对 `/ready` 和 `/artifact` 都验密**（`UPSTREAM_REQUIRE_AUTH=1`），
初始密钥与代理的 `RELAY_TARGET_AUTH_*` 对齐；代理的管理密钥为 `RELAY_ADMIN_KEY`
（演示值 `local-admin-key`，生产环境请自行注入）。容器健康检查走免密的 `/healthz`，
不依赖当前凭据。

两个上游都带可控探活延迟、慢下载和**可运行时调整的密钥**：

```bash
# 让 green 的探活慢 4 秒（relay 的探活超时默认 2s → 该次切换会失败）
curl -X POST 'http://localhost:9002/_control/probe?delay=4'
# 让 green 的探活直接 503（非 200 → 不切流）
curl -X POST 'http://localhost:9002/_control/probe?delay=0&status=503'
# 恢复
curl -X POST 'http://localhost:9002/_control/probe?delay=0&status=200'
# 运行时更换源站密钥（不回显密钥，只回指纹与版本号）
curl -X POST 'http://localhost:9001/_control/key?key=Bearer%20blue-secret-v2'
curl 'http://localhost:9001/_control/key'

# 触发切换
curl -X POST http://localhost:8080/api/switch \
  -H 'content-type: application/json' -d '{"target":"green","wait":true}'
curl http://localhost:8080/api/status | python3 -m json.tool
```

下载演示脚本（先起 compose，或者用下面的本地方式）：

```bash
./demo/download.sh          # 慢下载 + 切流 + 观察 5s 后截断 + 新下载立即走新目标
```

它做三件事：开一个约 75 秒的下载 → 切到另一版 → 新下载立刻带 `x-relay-target` 新值 →
旧下载在切流后约 5 秒被截断（`curl: (18)`），`/api/status` 里旧代 `closed`、在途数归零。
脚本读 `RELAY` 环境变量，默认 `http://localhost:8080`（本地手跑时用
`RELAY=http://127.0.0.1:<port> ./demo/download.sh`）。

### 手动跑两个可控上游（不用 docker）

```bash
UPSTREAM_NAME=blue UPSTREAM_REQUIRE_AUTH=1 UPSTREAM_AUTH_KEY='Bearer blue-secret-v1' \
  python -m uvicorn upstream.app:app --port 9001 &
UPSTREAM_NAME=green UPSTREAM_REQUIRE_AUTH=1 UPSTREAM_AUTH_KEY='Bearer green-secret-v1' \
  python -m uvicorn upstream.app:app --port 9002 &
RELAY_TARGETS='blue=http://127.0.0.1:9001,green=http://127.0.0.1:9002' \
  RELAY_TARGET_AUTH_blue='Bearer blue-secret-v1' \
  RELAY_TARGET_AUTH_green='Bearer green-secret-v1' \
  RELAY_ADMIN_KEY='local-admin-key' \
  RELAY_DRAIN_SECONDS=5 python -m relay.main
```

（`python -m relay.main` 发的是单进程 uvicorn；或 `uvicorn relay.main:build_app --factory`。）

## 配置

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `RELAY_TARGETS` | `blue=…:9001,green=…:9002` | `name=url` 逗号分隔，也接受 JSON `{"name":"url"}` |
| `RELAY_TARGET_AUTH_<目标名>` | 无 | 该目标的初始回源凭据，完整 `Authorization` 头值（如 `Bearer token`）；留空/不设表示该目标无凭据，代理不发认证头 |
| `RELAY_ADMIN_KEY` | 无 | 凭据管理 API 的本地管理密钥；不设置时凭据接口返回 503（失败关闭） |
| `RELAY_INITIAL_TARGET` | 第一个 target | 启动时的现行目标（**启动不探活**） |
| `RELAY_PROBE_PATH` | `/ready` | 候选探活路径 |
| `RELAY_PROBE_TIMEOUT` | `2` | 探活总超时（秒），超时=失败 |
| `RELAY_DRAIN_SECONDS` | `5` | 旧代收尾窗口 |
| `RELAY_CONNECT_TIMEOUT` | `5` | 上游连接超时 |
| `RELAY_SOCK_READ_TIMEOUT` | `0` | 上游读超时，0=不限（慢下载需要） |
| `RELAY_CHUNK_SIZE` | `65536` | 转发块大小（不缓冲整份响应） |
| `RELAY_HOST` / `RELAY_PORT` | `0.0.0.0` / `8080` | 监听地址 |
| `RELAY_SHUTDOWN_GRACE_SECONDS` | `5` | 退出时等长下载收尾的上限，超时后强制断开并 abort 所有代 |
| `RELAY_MAX_CLOSED_GENERATIONS` | `100` | 状态里保留的已关闭代数（活跃/收尾中的代永远不会被裁掉） |

## 控制面 API

* `POST /api/switch` — body `{"target": "green", "wait": false, "wait_timeout": 30}`。
  立即返回 `202` + `operation.status == "pending"`；探活完成后状态变为
  `committed` / `noop` / `failed` / `superseded`。`wait=true` 时最多等 `wait_timeout` 秒
  （该操作完成后返回 `200`）。未知目标 → `404`，body 不合法 → `422`。
  切流探活携带该目标**当前**凭据。
* `POST /api/credentials` — **需要管理密钥**。body
  `{"target": "blue", "authorization": "Bearer new-token", "wait": false, "wait_timeout": 30}`。
  代理用候选凭据探活目标 `/ready`：200 提交（凭据版本 +1），否则保留旧值并记 `failed`。
  响应只含版本号与结果，不含密钥；未带/带错管理密钥 → `401`，未配置 `RELAY_ADMIN_KEY` → `503`。
* `GET /api/credentials` — **需要管理密钥**。每目标凭据元数据（版本/是否配置/轮换时间）、
  进行中的轮换与最近 50 条轮换记录，均无密钥值。
* `GET /api/credential-rotations/{id}` — **需要管理密钥**。单次轮换的最终状态。
* `GET /api/status` — 免密。`current`（含 `credential_version`）、`current_generation`、
  `pending`、`generations[]`（`state` = `active`/`draining`/`closed`、`in_flight`、
  `drain_remaining_seconds`、`in_flight_requests[]` 含各自绑定的 `credential_version`）、
  `credentials`、`pending_credential_rotations[]`、`recent_credential_rotations[]`、
  `recent_operations[]`（最近 50 次切换）。
* `GET /api/operations/{id}` — 单次切换的最终状态。
* `GET /api/targets` — 目标表 + 当前目标 + 每目标凭据版本。

代理响应头附带 `x-relay-target` / `x-relay-generation` /
`x-relay-credential-version` / `x-relay-request-id`，便于核对每条下载实际绑定的目标与凭据版本。

## 设计要点（为什么这么做）

* **代(epoch) + 每代独立 abort 事件**：只有“代”能表达“切流那一刻已经在跑的请求”。
  用 target 名做键时，回切同址会把两批请求混在一起，正是“误断新代”的来源。
* **同步登记**：登记发生在路由函数首个 await 之前，配合单进程单事件循环，
  切流提交与请求受理之间不存在交错窗口。
* **两个连接池**：慢下载会占满连接，探活必须有自己的池，否则“慢下载不挡切换”不成立。
* **不缓冲**：`StreamingResponse` 自定义为逐块 `send`；同时把 `send` 与 abort 事件
  `asyncio.wait` 竞速，避免慢客户端把 5 秒 drain 拖成无限长。
* **中断方式**：drain 到期后不再发送最终 body 帧，uvicorn 关闭该连接，客户端得到截断
  （`curl: (18)` / `ClientPayloadError`）而不是假的 200 完整文件。uvicorn 会因此打一条
  `ERROR: ASGI callable returned without completing response` —— 这是被中断下载的预期日志。
* 上游自己返回的 `x-relay-*` 响应头会被剥掉，避免伪造路由标记。
* 上游返回的 `date`/`server` 也被剥掉，由 uvicorn 生成，避免出现两个同名响应头。

## 测试

```bash
pip install -r requirements-dev.txt
python -m pytest -q
```

`tests/` 分两层：`test_switch_unit.py` 用假探活 session 覆盖切换状态机（失败保留、supersede、
迟到 200、drain 只作用于本代、回切同址、no-op）；`test_credential_rotation_unit.py` 用假探活
session 覆盖凭据轮换状态机（初始凭据来自配置、200 提交/失败沿用、同目标 supersede、迟到 200
不得覆盖新版本、按目标隔离、关连取消）；`test_credential_rotation_integration.py` 起真实
TCP 上游 + relay，覆盖管理密钥授权、客户端 `Authorization` 剥离、受理凭据绑定、探活门禁、
在途下载跨轮换不重路由、探活不阻塞下载/切流/状态、响应不泄密；`test_proxy_integration.py`
覆盖逐块转发、长连接重路由、在途计数、5 秒截断、502/404、断开清理等。
