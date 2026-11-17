# 本地数据管道：分区文件 + 发布目录 + 增量消费

从零实现的本地数据管道，演示**目录发布、原子可见性、端到端去重、版本回退、孤儿回收**。
全部为标准库 Python 3，无外部依赖、无外部集群。

## 进程拓扑

```
            分区文件(原子落盘)        提交(batch_id 幂等)
写入器 writer.py ───────────────► objects/ ──────────────► 目录服务 catalog_service.py
 (每批次一个进程,可崩溃)                                      │ 版本化清单 / 回退 / diff / 回执登记
                                                              │ GET /api/diff?from&to
                                                              ▼
控制台 control.py ◄── HTTP ── 消费汇总器 consumer.py ──► consumer.db(明细+汇总+回执,同事务)
 (Web UI + 进程监管)        (轮询目录,增量应用)
                                                              ▲
孤儿回收 gc.py ◄── 引用全集(所有版本) ──────────────────────────┘
```

| 进程 | 端口 | 状态存储 |
|---|---|---|
| 控制台 control.py | 8620 | `pipeline_data/control_state.json` |
| 目录服务 catalog_service.py | 8621 | `pipeline_data/catalog.db` (SQLite/WAL) |
| 消费汇总器 consumer.py | 8622 | `pipeline_data/consumer.db` (SQLite/WAL) |
| 写入器 writer.py | — | 一次性进程，每批次运行一次 |
| 孤儿回收 gc.py | — | 一次性进程 |

## 快速开始

```bash
cd pipeline
python3 control.py          # 自动拉起目录服务与消费者
# 打开 http://127.0.0.1:8620
```

回归测试（覆盖全部故障场景，只走控制台 HTTP API，与网页操作等价）：

```bash
python3 smoke_test.py       # 10 个场景
python3 verify.py --data /workspace/pipeline_data \
  --catalog http://127.0.0.1:8621 --consumer http://127.0.0.1:8622   # 独立对账
```

## 关键设计决策

### 1. 去重放在消费端（而不是目录端）

- **明细按分区键幂等 upsert**：同一分区重复应用，结果完全相同；
- **汇总与回执（checkpoint）在同一个 SQLite 事务提交**：目录提交成功但消费回执丢失（进程在
  提交前崩溃）时，汇总也随之回滚，恢复后重放不会重复计入；
- 上报给目录服务的 `receipt` 仅用于展示消费滞后，**丢失不影响正确性**（权威状态在消费者本地）；
- 目录端只负责**写入器侧**的幂等：提交按 `batch_id` 去重，重试同一批次返回原版本号。

为什么不在目录端去重：目录无法区分"消费者已落账"与"消费者拿到数据后崩溃"，
若由目录标记已投递，回执丢失会导致**丢数据**而不是**防重复**。恰好一次只能由
"状态更新与回执原子提交"的消费端保证。

### 2. 半成品对消费者不可见

- 文件写入：`tmp + fsync + os.replace + fsync(dir)`，任何时刻只有完整文件；
- 可见性只能来自目录：消费者没有扫描对象目录的能力，只凭清单中的路径读取；
- 目录提交前逐文件校验存在性与 sha256，缺失/损坏的半成品进不了目录；
- 消费者应用前再次校验 sha256，校验失败本轮放弃、不前进 checkpoint。

### 3. 版本回退后的重新消费

- 版本只增不减、历史不改写，head 是可回退的指针；
- diff 是**集合运算**（对任意 from/to 成立，包括 from > to）：
  回退导致消失的分区从明细删除、重现或内容变化的分区重新 upsert；
- 汇总 = 当前 head 清单的物化视图，任何时刻可用 `verify.py` 独立对账。

### 4. 孤儿文件回收

- 基准是**所有历史版本**的引用全集（回退可能复活旧版本，不能只看 head）；
- 年龄小于宽限期的文件跳过（写入器可能刚落盘未提交）；
- 仍被任一版本引用的文件绝不删除。

## 故障矩阵（冒烟测试逐条验证）

| 故障时刻 | 恢复行为 |
|---|---|
| 写入器落盘后、提交前崩溃 | 文件成孤儿，消费者不可见；GC 超宽限期后回收 |
| 目录提交成功但响应丢失 | 写入器按 batch_id 重试，目录幂等去重，不产生重复版本 |
| 消费者提交前崩溃（回执丢失） | 事务整体回滚，重启重放，upsert 幂等不重复 |
| 消费者提交后崩溃（回执未上报） | 目录侧 receipt 滞后，本地 checkpoint 权威，不重复 |
| 目录停机 | 写入器提交失败，恢复后同批次重试成功；消费者保留错误并续轮询 |
| 回退到旧版本 | 消费者按集合 diff 删除/重现分区，汇总随之收敛 |
| 下架分区 | 提交 deletes 版本，消费者从明细中移除 |

## API 速览

目录服务：`POST /api/commit` `POST /api/rollback` `POST /api/receipt` ·
`GET /api/head` `/api/versions` `/api/manifest?version=` `/api/diff?from=&to=`
`/api/events` `/api/receipts` `/api/referenced`

消费者：`GET /api/status` `/api/summary` `/api/details`

控制台：`GET /` `GET /api/state` `POST /api/action`
（commit / writer_crash / stop|start_catalog / stop|start_consumer /
consumer_pause|resume / consumer_fault / rollback / delete_partition / gc / reset）
