# storage-compaction-livelock-detector
检测 LSM 压缩活锁、墓碑堆积和写放大异常

> **注意：本工具只分析压缩计划（模拟候选选择并检测轨迹），不执行真实的文件合并。**

## 功能

针对“压缩后很快又达到阈值、写放大持续升高”的场景，仅比较单轮压缩字节数无法
区分恢复与活锁。本工具在检测窗口内模拟后续若干轮压缩候选选择，并识别：

- **重复重写同一键范围**（`range-rewrite-cycle`）：同一键范围签名在窗口内重复
  出现且没有净进展（总字节未下降、垃圾占比未改善），判定为活锁并输出循环链。
- **墓碑无法下沉**（`tombstone-stall`）：非最底层墓碑字节持续增长，压缩未丢弃
  任何墓碑。
- **低层容量抖动**（`capacity-jitter`）：低层利用率围绕容量阈值反复穿越。
- **多任务互相阻塞**（`task-blocking`）：候选键范围冲突或超出并发上限。

窗口内的有效数据量按 `总字节 - 墓碑字节 - 已过期版本字节` 计算，而不是只按
总字节统计。

## 输出

`LivelockDetector.analyze(trace)` 返回 `Report`：

- `status`：`stable` / `degraded` / `livelock` / `recovered`
- `cycle_chain`：循环链（重复出现的键范围签名序列、涉及轮次、重复次数）
- `abort_point`：最稳定的中止点（超容量程度、阻塞数、垃圾占比综合最低的轮次）
- `issues` / `priorities`：检测到的问题与按优先级排序的处理建议
- 窗口起止的总字节、有效字节、垃圾占比

## 用法

```python
from livelock_detector import (
    CompactionSimulator, SimulatorConfig, LivelockDetector, constant_workload,
)

config = SimulatorConfig(
    level_capacities=[10**9, 10**9, 10**9],  # 各层容量（索引即层号）
    l0_file_trigger=2,                        # L0 文件数触发阈值
    max_concurrent=2,                         # 并发压缩任务上限
)
sim = CompactionSimulator(config, initial_files=[...])  # 可传入现有 SSTable
workload = constant_workload(size_bytes=1000, key_start=0, key_end=100,
                             tombstones=500, entries=1000)
trace = sim.simulate(10, workload)            # 模拟 10 轮候选选择

report = LivelockDetector(window=8).analyze(trace)
print(report.status, report.cycle_chain, report.abort_point)
print(report.priorities)
```

输入包括：各层 SSTable（键范围、文件大小、墓碑数量、过期版本字节）、层容量、
写流量（每轮新产生的文件）以及模拟产生的压缩历史。`analyze(trace, end_round=N)`
可只分析到指定轮次为止的轨迹。

## 测试

```bash
python -m unittest
```

覆盖：正常下沉、墓碑活锁、重叠文件竞争、窗口边界、恢复后重新稳定、容量抖动
与有效字节口径。兼容 Python 3.8.2。
