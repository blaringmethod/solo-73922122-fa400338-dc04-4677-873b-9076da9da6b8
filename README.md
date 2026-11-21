# 观测数据 SQL 清洗沙箱

面向采样数据工程师的外协清洗 SQL 试跑平台：上传观测 CSV、按站点提交只读
SQL，成功后下载结果 CSV 交给质控。基于 **FastAPI + DuckDB**，全部数据驻留
内存，每轮独立隔离执行。质控员反复微调同一条 SQL 时，试算结果会被缓存并
自动复用（相同键的并发提交只真正执行一次）。

## 功能与约束（与需求逐条对应）

| 需求 | 实现 |
| --- | --- |
| 上传至多 1 MiB 观测 CSV（站号、UTC时刻、温度） | `POST /api/upload`，超限返回 413；表头支持任意列序，UTF-8 编码 |
| 选站后提交 SQL，每轮输入固定为所选站点数据 | 每轮仅把该站点行装入该轮**私有 DuckDB 连接**的 `observations` 表 |
| 仅允许 SELECT / WITH，支持筛选、连接、聚合 | 静态 SQL 守卫（sqlparse 单语句判定 + 关键字/模式黑名单） |
| 不得读写服务器文件、联网、加载扩展 | 守卫拦截 `read_csv`/`COPY`/`ATTACH`/`LOAD`/`INSTALL`/`http_get` 等；DuckDB 连接再以 `enable_external_access=false` 兜底关闭文件系统、网络与扩展 |
| 不得访问其他轮次数据 | 每轮独立线程 + 独立内存库 + 独立数据副本，其他轮次的连接不可达 |
| 最多两轮并行，满额拒绝 | 注册表实时统计**实际执行**数，第 3 个新执行返回 503 |
| 执行满 5 秒终止 | 看门狗在 5 秒调用 DuckDB 原生 `interrupt()` 立即中止，状态置 `failed` |
| 结果超过 8 MiB 整轮失败 | 流式累计结果字节，超限即失败，已生成内容全部丢弃 |
| 可查询状态、可取消 | `GET /api/rounds/{id}`、`POST /api/rounds/{id}/cancel` |
| 取消受理不等执行结束；实际退出后才报取消并释放名额；迟到结果不开放 | 取消立即返回 `accepted`，监督线程等 worker 真正退出后才置 `cancelled`；取消优先于同时到达的结果 |
| 成功先发布则不可取消 | 已 `succeeded` 的轮次取消返回 409，结果仍可下载 |
| 失败/超时不供部分结果 | 失败轮次结果接口返回 409 |
| 慢查询不挡另一轮、状态查询、已有结果下载 | 每轮独立线程；DuckDB 执行 C++ 期间释放 GIL 真并行；HTTP 接口均为短操作 |
| **缓存键 = 上传批次版本 + 站点 + 校验后的 SQL** | 每次上传生成递增 `batch_id`；SQL 经守卫校验后用规范化文本（去注释、折叠空白）的 SHA-256 参与键 |
| **只有完整成功的 CSV 才能复用** | 仅 `succeeded` 且完整序列化完成的结果插入缓存；失败/超时/取消/超限一律不写 |
| **换入新 CSV 后旧缓存自动失效** | 上传成功即清空缓存并递增缓存代际；换批前开始的在途执行即使成功也不会污染新批次 |
| **相同键并发提交只执行一次，其余等待** | 注册表按缓存键复用同一个共享执行；等待轮次不占名额，状态 `waiting` |
| **等待轮次可单独取消且不影响共享执行** | 等待轮取消只从共享执行解绑；仅当最后一个参与轮取消时才 interrupt |
| **缓存命中轮次直接完成，执行名额只计实际共享执行** | 命中即 `succeeded`（`cache_hit=true`），不创建 worker、不占名额 |
| **最多 8 条成功结果、总 CSV ≤16 MiB，按近期使用淘汰** | `ResultCache` 为 LRU：条目数与字节数双上限，命中/插入均刷新近期使用 |
| **已完成轮次仍可下载** | 每轮持有自己的结果字节，缓存淘汰或批次切换都不影响历史下载；保留量另有 100 轮 / 96 MiB 上限防止内存无限增长 |
| **网页显示本轮是否命中缓存** | 轮次表新增“缓存”列（命中 / 共享等待 / 未命中→已缓存）与缓存配额面板 |

## 目录结构

```
app/
  config.py       限额常量（1 MiB / 5 s / 8 MiB / 2 并行 / 8 条 16 MiB 缓存）
  csv_io.py       CSV 解析与校验
  sql_guard.py    SQL 静态安全校验 + 规范化与摘要
  worker.py       每轮查询执行器（独立 DuckDB 连接、interrupt 中止、8MiB 判定）
  cache.py        有界 LRU 结果缓存（条目/字节双上限、代际防污染）
  registry.py     轮次注册表、共享执行、名额控制、状态机、看门狗与监督线程
  store.py        内存数据集（按站点切分、批次号）
  main.py         FastAPI 路由
  static/index.html  演示网页（上传/选站/提交/轮询/取消/下载/缓存状态）
sample_data/observations.csv  演示数据（3 个站点 9 行）
tests/            91 个 pytest 用例（SQL 守卫、CSV、端到端 API、缓存与共享执行）
Dockerfile, docker-compose.yml
```

## 本地运行（无需 Docker）

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --host 0.0.0.0 --port 8000
# 浏览器打开 http://localhost:8000
```

## Docker Compose 运行

```bash
docker compose up --build
# 打开 http://localhost:8000
```

Compose 额外做了加固：只读根文件系统、`no-new-privileges`、丢弃所有 Linux
capability、512 MiB 内存上限、内置健康检查。服务本身不写任何磁盘文件。

> 说明：本次交付环境未安装 Docker（`docker` 命令不可用），无法在此实际执行
> `docker compose up`。Dockerfile/Compose 已按上一版保持不变并做了静态检查
> （YAML 可解析、镜像基于 `python:3.11-slim` 且依赖来自 `requirements.txt`）；
> 应用本身以相同的 `uvicorn app.main:app --host 0.0.0.0 --port 8000 --workers 1`
> 命令在本机真实启动并通过了下面的端到端自测。

## HTTP API 摘要

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/` | 演示网页 |
| GET | `/api/health` | 健康状态、当前活动执行数、站点列表、批次号、缓存统计 |
| POST | `/api/upload` | multipart 上传 CSV（≤1 MiB）；成功后生成新批次并清空缓存 |
| POST | `/api/rounds` | 提交 `{station_id, sql}` 创建一轮；可缓存命中或加入共享执行；满额 503、SQL 非法 400 |
| GET | `/api/rounds` / `/api/rounds/{id}` | 轮次列表 / 单轮状态；含 `cache_hit`、`shared_role`、`cache_key`、`batch_id` |
| POST | `/api/rounds/{id}/cancel` | 请求取消（成功轮返回 409；等待轮只取消自身） |
| GET | `/api/rounds/{id}/result` | 下载结果 CSV（仅 succeeded 可用） |

轮次状态：`running`（本轮的共享执行在跑）/ `waiting`（等待他人发起的共享
执行）/ `succeeded` / `failed` / `cancelled`。缓存命中直接进入 `succeeded`
并带 `cache_hit=true`。

## 结果缓存与共享执行

- **缓存键**：`(batch_id, station_id, sha256(规范化 SQL))`。SQL 经守卫校验后
  去掉注释、折叠空白再取摘要（字符串字面量与带引号标识符保持原样），所以
  仅改格式/加注释仍能命中；站点或任意语义改动都会换键。
- **只缓存完整成功**：worker 完整序列化并返回 `done` 才写入；`failed`、
  5 秒超时、取消、超过 8 MiB 的结果一律不写，重试会真正执行。
- **换批失效**：每次上传递增 `batch_id` 并清空缓存；上传前启动的在途执行
  即使成功也因缓存代际不符而不会写入新批次（避免旧数据污染）。
- **共享执行**：相同键的并发提交共享一个 worker；发起轮为 `running`，其余
  为 `waiting`，两者之外的新键才占用第 3 个名额并返回 503。等待轮取消只
  解绑自身；最后一个参与轮取消时才 `interrupt`，且该执行结果不缓存。
- **容量与淘汰**：LRU，最多 8 条且总 CSV ≤ 16 MiB，命中与插入都会刷新近期
  使用。轮次自身保存结果字节，缓存淘汰/换批不影响已完成轮次的下载。

## 测试

```bash
pip install pytest httpx
python -m pytest tests/ -q
```

共 91 个用例：SQL 守卫、CSV 解析、并发/超时/取消隔离，以及缓存键语义、
命中不占名额、并发只执行一次、等待轮独立取消、失败/超时/超限不缓存、
LRU 条目与字节上限、换批失效与在途防污染、淘汰后仍可下载、轮次保留上限。

## 设计说明：为什么用线程 + interrupt 而非子进程

首选方案本为每轮独立子进程，以便从进程边界硬终止。当前构建/运行环境的
seccomp 策略禁止向子进程发送信号（`SIGTERM/SIGKILL` 均返回 EPERM），硬杀
进程不可行。因此改为：每轮一个守护线程持有**独立的内存 DuckDB 连接**，
超时/取消时调用 DuckDB 官方的 `connection.interrupt()`，查询在 C++ 层立即
抛 `InterruptException` 退出（已实测中断延迟 < 10 ms）。轮次间无共享连接、
无共享表，数据隔离不依赖进程边界；DuckDB 执行期间释放 GIL，两个慢查询可
真正并行。
