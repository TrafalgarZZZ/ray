# Ray Per-Actor Cgroup 资源隔离设计

> 状态：设计草案  
> 基线：Ray `master`，commit `e4766c5fab`  
> 范围：Linux cgroup v2；仅 Actor worker；依赖现有 Ray resource isolation

## 1. 背景

Ray 当前的 resource isolation 将一个节点内的进程划分为 system、worker 和 non-Ray 三类：

```text
<base cgroup>
└── ray-node_<node_id>
    ├── system
    │   └── leaf
    └── user
        ├── workers
        └── non-ray
```

所有普通 task worker 和 Actor worker 当前都进入同一个 `user/workers` cgroup。现有实现能够在 Ray system 进程和用户进程之间提供 CPU、内存隔离，但不能为单个 Actor 提供独立的硬资源边界。

Actor 适合增加进程级 cgroup 隔离，原因如下：

1. 一个 Actor incarnation 唯一对应一个 CoreWorker 进程。
2. Actor worker 不会在 Actor 存活期间返回普通 idle worker pool 执行其他 task。
3. 显式配置的 Actor resource 是 lifetime resource。
4. `max_concurrency`、async Actor 和 concurrency groups 都在同一 CoreWorker 进程内实现。
5. Actor 创建的本地线程和 subprocess 可以自然继承同一个 cgroup。

本设计在现有 resource isolation 之上，为 Actor worker 创建独立的 cgroup，并将 Actor 的 lifetime CPU、memory resource 转换成 cgroup v2 硬限制。

## 2. 目标

本设计的目标是：

- 为每个 Actor worker incarnation 创建独立的 cgroup。
- 将 Actor 的 lifetime CPU resource 转换为 `cpu.max`。
- 将 Actor 的 lifetime memory resource 转换为 `memory.high` 和 `memory.max`。
- Actor constructor、Actor methods、线程和本地 subprocess 共享同一个聚合资源上限。
- Actor restart 后为新的 worker incarnation 创建新的 cgroup。
- Actor 退出后清理其 cgroup 及残留子进程。
- 不改变普通 task worker、IO worker、driver 和 system process 的现有行为。
- 保持现有 `--enable-resource-isolation` 单独使用时完全向后兼容。

## 3. 非目标

第一版不包含：

- 普通 task worker 的 per-worker cgroup。
- 每次 Actor method 调用独立的 cgroup。
- 根据 `max_concurrency` 自动放大 CPU 或内存。
- 使用 `cpuset.cpus` 绑定或独占物理 CPU core。
- GPU、object store memory 或自定义 Ray resource 的 cgroup 映射。
- 对 Actor 提交的嵌套 Ray task 进行资源继承。
- 在没有显式 lifetime CPU resource 的默认 Actor 上模拟 method-level CPU resource。
- 非 Linux 平台支持。

## 4. 用户接口与功能门控

### 4.1 新参数

CLI 新增：

```text
--enable-actor-resource-isolation
```

SDK 新增对应参数：

```python
ray.init(
    enable_resource_isolation=True,
    enable_actor_resource_isolation=True,
)
```

默认值均为 `False`。

### 4.2 两级门控

Per-Actor cgroup 只有在以下两个参数同时为 `True` 时启用：

```text
actor_resource_isolation_enabled =
    enable_resource_isolation && enable_actor_resource_isolation
```

行为矩阵：

| `enable_resource_isolation` | `enable_actor_resource_isolation` | 行为 |
|---|---|---|
| `false` | `false` | 不启用任何 cgroup resource isolation |
| `true` | `false` | 保持当前 system/user resource isolation 行为 |
| `true` | `true` | 启用当前 resource isolation，并增加 per-Actor cgroup |
| `false` | `true` | 参数校验失败，拒绝启动 |

不允许 Actor isolation 静默降级。仅设置 `--enable-actor-resource-isolation` 时，Python 配置层应抛出 `ValueError`；raylet C++ 参数层也应保留防御性 `RAY_CHECK`。

### 4.3 参数传递路径

建议沿现有 resource isolation 配置链传递新字段：

```text
ray start / ray.init
  → ResourceIsolationConfig
  → RayParams
  → services.start_raylet()
  → raylet --enable-actor-resource-isolation
  → NodeManagerConfig.enable_actor_resource_isolation
  → CgroupManager / Actor cgroup lifecycle hooks
```

主要改动位置：

- `python/ray/scripts/scripts.py`
- `python/ray/_private/worker.py`
- `python/ray/_private/resource_isolation_config.py`
- `python/ray/_private/services.py`
- `src/ray/raylet/main.cc`
- `src/ray/raylet/node_manager.h`

## 5. Actor 与资源语义

### 5.1 Actor method 的执行位置

Actor creation 完成后，所有 Actor methods，包括用户定义的 `__call__`，都被路由到该 Actor 已绑定的同一个 CoreWorker 进程。Actor task 不会从通用 WorkerPool 重新选择进程。

`max_concurrency` 不会增加 Actor worker 数量：

- 同步 Actor 使用同一进程中的线程池。
- Async Actor 使用同一进程中的 event loop/coroutine。
- Concurrency groups 使用同一进程中的多个 executor。

因此 cgroup 限制是 Actor 级聚合限制。例如：

```python
@ray.remote(num_cpus=2, memory=4 * 1024**3, max_concurrency=8)
class A:
    ...
```

其语义是最多 8 个并发调用共同分享 2 CPU 和 4 GiB memory，而不是每个调用分别获得这些资源。

### 5.2 使用 lifetime resource

Per-Actor cgroup 必须基于 Actor creation lease 的 `GetRequiredResources()`，不能使用 `GetRequiredPlacementResources()`。

- `required_resources` 表示 Actor 生命周期实际持有的资源。
- `required_placement_resources` 只用于节点放置，可能包含默认 Actor method 所需的额外临时 CPU。

### 5.3 默认 Actor 的特殊情况

没有显式资源配置的默认 Actor 当前具有以下语义：

- lifetime CPU 为 0；
- Actor method CPU 为 1；
- placement resources 包含 method CPU。

因此不能把 lifetime CPU 0 转换为 `cpu.max = 0`。第一版采用如下规则：

| Actor lifetime resource | cgroup 行为 |
|---|---|
| CPU `> 0` | 设置对应 `cpu.max` |
| CPU 缺失或为 `0` | `cpu.max = max`，不施加 per-Actor CPU 硬限制 |
| memory `> 0` | 设置 `memory.high` 和 `memory.max` |
| memory 缺失或为 `0` | `memory.high = max`、`memory.max = max` |

Ray 的 lease specification 不能可靠地区分“未配置”和“显式配置为 0”，因此两者在第一版中都视为不限制。

### 5.4 Placement group resource 名称

Placement group 会把 `CPU`、`memory` 转换成带 group 后缀的 resource 名称，并可能同时产生 wildcard 和 indexed 两个别名。资源归一化必须：

1. 识别原始 `CPU` 或 `memory` 名称。
2. 使用 `ParsePgFormattedResource()` 识别 placement group 别名。
3. 对同一原始资源的多个别名取最大值，而不是求和，避免 wildcard 和 indexed 重复计算。

建议增加辅助函数：

```cpp
double GetOriginalResourceQuantity(const ResourceSet &resources,
                                   const ResourceID &original_resource);
```

## 6. Cgroup 层级

### 6.1 推荐层级

新增 `actor-workers`，与现有 `workers` 同级：

```text
<base cgroup>
└── ray-node_<node_id>
    ├── system
    │   └── leaf
    └── user
        ├── workers
        │   └── 普通 task worker、IO worker
        ├── actor-workers
        │   ├── actor-<actor_id>-worker-<worker_id>
        │   └── actor-<actor_id>-worker-<worker_id>
        └── non-ray
```

不建议直接在现有 `workers` 下创建 Actor 子 cgroup。当前普通 worker PID 直接位于 `workers`；如果在 `workers/cgroup.subtree_control` 启用 controller，会违反 cgroup v2 的 no-internal-process 约束。

`actor-workers` 是不直接包含 PID 的 domain cgroup，每个 Actor worker cgroup 是叶子 cgroup。

### 6.2 Controller 启用

仅在两个功能开关同时启用时执行：

```text
user/cgroup.subtree_control          += cpu,memory
actor-workers/cgroup.subtree_control += cpu,memory
```

现有 `user` cgroup 本身不包含 PID，PID 位于其子 cgroup，因此可以安全启用 controller。

### 6.3 命名

建议使用：

```text
actor-<actor_id>-worker-<worker_id>
```

ActorID 便于运维定位，WorkerID 用于区分 Actor restart 后的不同 incarnation。不要只使用 PID，避免 PID reuse 和排障歧义。

## 7. 资源到 cgroup 的映射

### 7.1 CPU

使用 `cpu.max` 实现硬上限，不使用 `cpu.weight` 表示 Actor 的 `num_cpus`。

固定 period：

```text
period_us = 100000
quota_us = ceil(num_cpus * period_us)
cpu.max = "<quota_us> <period_us>"
```

示例：

| Actor CPU | `cpu.max` |
|---:|---|
| 0.25 | `25000 100000` |
| 0.5 | `50000 100000` |
| 1 | `100000 100000` |
| 2 | `200000 100000` |

这个限制作用于 Actor worker 的全部线程和本地 subprocess 的 CPU 总和。

`cpu.max` 是最大使用量，不保证物理 core 独占。物理 core 绑定需要 `cpuset.cpus` 和 Ray logical CPU 到 OS CPU ID 的映射，不属于第一版范围。

### 7.2 Memory

对于显式 memory resource：

```text
memory.high = <actor_memory_bytes>
memory.max  = <actor_memory_bytes>
memory.oom.group = 1
```

`memory.high` 提供 reclaim/throttling 压力信号，`memory.max` 提供硬上限。限制是整个 Actor worker、线程和本地 subprocess 的聚合限制。

设置 `memory.oom.group=1`，使 cgroup OOM 按 Actor 进程组整体终止，避免只杀死某个本地 subprocess 后让 Actor 继续处于不完整状态。

这是比 Ray 当前逻辑 memory scheduling resource 更强的执行语义。Python interpreter、native library、Actor state、执行中参数以及被计入该 cgroup 的 page cache 都可能包含在 `memory.current` 中。功能文档必须明确这一点。

第一版不自动添加 worker overhead，保证 cgroup 上限与 Actor 声明的 memory 一致。用户需要为 runtime baseline 和 `max_concurrency` 带来的并发内存预留足够空间。

### 7.3 不映射的资源

以下资源不转换为 cgroup 配置：

- GPU
- `object_store_memory`
- accelerator resource
- placement group bundle marker
- custom resource

这些资源继续只参与 Ray scheduler admission control。

## 8. 生命周期和时序

### 8.1 创建时点

推荐在 Actor creation lease 已选中 worker、但成功 reply 尚未发送给调用方之前创建和配置 cgroup。

```text
LocalLeaseManager::PoppedWorkerHandler
  → 获得 Actor creation worker
  → 检查 IsActorCreationTask()
  → PrepareActorWorkerCgroup(worker, lease_spec)
      1. 创建 Actor leaf cgroup
      2. 写入 cpu.max
      3. 写入 memory.high / memory.max
      4. 将 worker PID 写入 leaf/cgroup.procs
  → Grant(worker, ...)
  → 返回 worker address
  → CoreWorker 接收并执行 Actor __init__
```

这一时点满足：

- WorkerID、PID 和 ActorID 已确定。
- Actor lifetime resource 已完成调度和分配。
- Actor constructor 和用户代码尚未执行。
- 普通 worker fork hook 无需改变。

Worker bootstrap 和注册阶段仍暂时位于公共 `user/workers`。第一版隔离 Actor constructor 和全部 Actor methods，而不隔离 worker bootstrap。若未来要求从 `fork()` 开始隔离，可以把创建动作前移到 `WorkerPool::StartWorkerProcess()`，但需要向 `PopWorkerRequest` 传入 ActorID 和归一化资源，改动范围更大。

### 8.2 注入 callback，避免 scheduler 直接依赖 cgroup

`LocalLeaseManager` 当前不拥有 `CgroupManager`。建议注入一个 callback，而不是让 scheduler 直接包含 cgroup 实现：

```cpp
using PrepareActorWorkerCgroupCallback =
    std::function<Status(const std::shared_ptr<WorkerInterface> &,
                         const LeaseSpecification &)>;
```

raylet `main()` 创建 callback，捕获 `CgroupManagerInterface`，并传入 `LocalLeaseManager`。当 Actor isolation 未启用时 callback 为 no-op。

这样可以：

- 保持 scheduler 与 Linux cgroup 实现解耦；
- 在单元测试中注入 fake callback；
- 保证配置发生在 successful grant reply 之前；
- 复用现有 `NoopCgroupManager` 模式。

### 8.3 Actor creation 完成

Actor `__init__` 完成后，NodeManager 当前通过 `ConvertWorkerToActor()` 将 worker 标记为 Actor worker。此时不需要修改 cgroup；worker 已经位于独立 Actor cgroup，且 lifetime limits 保持不变。

### 8.4 Actor methods 和并发

Actor methods 继承 Actor creation 时分配的 lifetime resources，并始终在相同 CoreWorker 中运行。因此：

- 不在每次 method 调用时更新 cgroup。
- `max_concurrency` 不改变 cgroup limit。
- 多线程 Actor 的全部线程共享 CPU quota 和 memory limit。
- Async Actor 的全部 coroutine 共享 memory limit。
- Concurrency groups 的全部 executor 共享同一个 cgroup。

### 8.5 Actor 本地子进程

Actor worker 移入 cgroup 后创建的本地 subprocess 自动继承 Actor cgroup。Actor 提交的嵌套 Ray task 不继承，因为嵌套 task 在其他 Ray worker 中执行。

## 9. CgroupManager 接口

建议扩展 `CgroupManagerInterface`：

```cpp
struct ActorCgroupLimits {
  std::optional<double> cpu_cores;
  std::optional<int64_t> memory_bytes;
};

virtual Status CreateActorWorkerCgroup(
    const ActorID &actor_id,
    const WorkerID &worker_id,
    const ActorCgroupLimits &limits) = 0;

virtual Status AddProcessToActorWorkerCgroup(
    const WorkerID &worker_id,
    const std::string &pid) = 0;

virtual Status RemoveActorWorkerCgroup(
    const WorkerID &worker_id,
    bool force) = 0;
```

实现维护：

```cpp
absl::flat_hash_map<WorkerID, std::string> actor_worker_cgroups_;
```

动态 Actor cgroup 不能只依赖当前静态的析构 callback 栈；需要支持 Actor 生命周期内随时创建、删除，并在 `CgroupManager` 析构时清理所有仍存活的动态 cgroup。

`NoopCgroupManager` 和 `FakeCgroupManager` 需要实现对应 no-op/fake 方法。

## 10. 清理和故障处理

### 10.1 正常退出

Actor worker disconnect 时，NodeManager 调用：

```text
RemoveActorWorkerCgroup(worker_id, force=false)
```

只有在 cgroup 已无进程且无子 cgroup时执行 `rmdir`。若 worker 正在执行 graceful shutdown，应等待其真正退出，不应提前使用 `cgroup.kill` 中断清理逻辑。

### 10.2 异常退出和残留 subprocess

Actor worker 可能创建本地 subprocess。主进程退出后，叶子 cgroup 仍可能 populated，直接 `rmdir` 会失败。

强制清理流程：

```text
读取 cgroup.events populated
  → populated=1 且存在 cgroup.kill：写 cgroup.kill=1
  → 不存在 cgroup.kill：使用 process-group cleanup，并枚举 cgroup.procs 清理残留 PID
  → 等待 populated=0
  → 删除 Actor leaf cgroup
```

`cgroup.kill` 不是所有 Ray 支持的 Linux kernel/cgroup v2 环境都具备，因此必须运行时探测该文件，不能把它作为启动前提。回退路径应复用现有 worker process-group cleanup；若 cgroup 中仍有不属于原 process group 的残留进程，再读取 `cgroup.procs` 并逐个终止。

建议与现有 process-group cleanup 协调：

- graceful disconnect：先允许 worker/process group 完成退出，再清理 cgroup；
- system error、forced actor kill、raylet shutdown：允许使用 `cgroup.kill`；
- 清理采用异步 retry，不能阻塞 raylet main event loop；
- 所有 retry 都必须有超时和 warning 日志。

### 10.3 Actor restart

Actor restart 会创建新的 WorkerID：

```text
旧：actor-A-worker-W1
新：actor-A-worker-W2
```

新 cgroup 的创建不等待旧 cgroup 名称复用，因此旧 worker 的异步清理不会阻塞 Actor restart。

### 10.4 Raylet 退出

`CgroupManager` 析构时应按以下顺序执行 best-effort cleanup：

1. 停止新的 Actor cgroup 操作。
2. kill 或迁出所有动态 Actor cgroup 内的进程。
3. 删除所有 Actor leaf cgroup。
4. 禁用 `actor-workers` controller。
5. 删除 `actor-workers`。
6. 执行现有 system/user hierarchy 清理。

### 10.5 配置失败原则

功能必须 fail closed：开启 Actor isolation 后，不允许 Actor 在 per-Actor cgroup 创建失败时以未隔离状态运行。

- 初始化 `actor-workers` 层级失败：raylet 启动失败。
- 单个 Actor cgroup 创建或约束写入失败：Actor creation 失败，释放 lease resource 并终止该 worker。
- PID 迁移失败：Actor creation 失败，不能发送 successful grant reply。
- 清理失败：记录 warning 并异步重试；不能阻塞其他 Actor。

不建议因单个 Actor 的动态 cgroup 创建失败直接使整个 raylet `RAY_CHECK` 退出。初始化期的不变量错误可以 fail fast，运行期错误应转换为 Actor creation failure。

## 11. OOM 和错误传播

`memory.max` 超限可能触发 cgroup-local OOM。第一版至少需要：

- 在 worker disconnect 日志中记录 ActorID、WorkerID、cgroup path。
- 读取 `memory.events`，记录 `oom`、`oom_kill` 计数。
- 当 `oom_kill` 增加时，将 worker failure message 标记为 Actor cgroup OOM，而不是笼统的 system error。
- 将 cgroup path、memory limit 和 `memory.peak` 加入诊断日志。

是否增加新的 protobuf worker exit type 可以作为后续增强；第一版可以复用现有 OOM/worker failure 类型并提供明确 reason message。

## 12. 可观测性

建议增加以下日志字段：

```text
actor_id
worker_id
pid
cgroup_path
cpu_quota_us
cpu_period_us
memory_high_bytes
memory_max_bytes
```

建议指标：

- `ray_actor_cgroup_created_total`
- `ray_actor_cgroup_create_failed_total`
- `ray_actor_cgroup_cleanup_failed_total`
- `ray_actor_cgroup_oom_kill_total`
- `ray_actor_cgroup_cpu_throttled_seconds_total`

第一版可以只实现日志和失败计数，避免为每个 Actor 周期性抓取 cgroup 文件造成较高开销。

## 13. 代码改动范围

### 13.1 Python 配置层

- `python/ray/scripts/scripts.py`
  - 增加 `--enable-actor-resource-isolation`。
- `python/ray/_private/worker.py`
  - `ray.init()` 增加 `enable_actor_resource_isolation`。
- `python/ray/_private/resource_isolation_config.py`
  - 保存新开关。
  - 校验 Actor isolation 依赖 resource isolation。
- `python/ray/_private/services.py`
  - 将开关传给 raylet。
- `python/ray/_private/parameter.py`
  - 按当前 `RayParams`/`ResourceIsolationConfig` 数据流更新字段。

### 13.2 Raylet 配置和调度时序

- `src/ray/raylet/main.cc`
  - 增加 gflag。
  - 构造 Actor cgroup callbacks。
- `src/ray/raylet/node_manager.h/.cc`
  - 保存功能开关。
  - 在 worker disconnect 和 raylet shutdown 路径触发清理。
- `src/ray/raylet/scheduling/local_lease_manager.h/.cc`
  - 注入 prepare callback。
  - 在 Actor creation grant reply 之前调用。
  - 正确处理 callback failure 和 lease resource rollback。

### 13.3 Cgroup 实现

- `src/ray/common/cgroup2/cgroup_manager_interface.h`
- `src/ray/common/cgroup2/cgroup_manager.h/.cc`
- `src/ray/common/cgroup2/cgroup_driver_interface.h`
- `src/ray/common/cgroup2/sysfs_cgroup_driver.h/.cc`
- `src/ray/common/cgroup2/noop_cgroup_manager.h`
- `src/ray/common/cgroup2/fake_cgroup_manager.h`

需要增加动态 cgroup CRUD、`cpu.max` string constraint、`memory.high/max`、`memory.oom.group`、`cgroup.events` 和可选的 `cgroup.kill` 支持。

## 14. 测试计划

### 14.1 配置单元测试

覆盖完整开关矩阵：

```text
resource=false, actor=false → success, disabled
resource=true,  actor=false → success, legacy behavior
resource=true,  actor=true  → success, actor isolation enabled
resource=false, actor=true  → ValueError / startup failure
```

同时覆盖 CLI 和 `ray.init()`。

### 14.2 CgroupManager 单元测试

- 仅 Actor isolation 启用时创建 `actor-workers`。
- 正确启用 `cpu`、`memory` controller。
- 创建的 Actor cgroup 名称同时包含 ActorID 和 WorkerID。
- fractional CPU 正确转换为 `cpu.max`。
- memory 正确写入 `memory.high/max`。
- `memory.oom.group` 设置为 `1`。
- CPU/memory 0 或缺失时写入 `max`。
- placement group resource 不重复计算。
- 重复创建、删除不存在 cgroup、权限错误返回明确 Status。
- 析构时动态 cgroup 先于父 cgroup清理。
- 同时覆盖存在和不存在 `cgroup.kill` 的清理路径。

### 14.3 Raylet 单元测试

- 普通 task lease 不调用 prepare callback。
- Actor creation lease 在 successful reply 前调用 prepare callback。
- callback 失败时不发送成功 reply，并释放资源、终止 worker。
- Actor creation 完成后不把 worker推回普通 idle pool。
- disconnect 对存在 Actor cgroup 的 worker触发清理。
- Actor creation 失败、Actor restart、forced kill 路径均清理正确。

### 14.4 Linux 集成测试

在可写 cgroup v2 环境中验证：

1. 普通 worker 仍位于 `user/workers`。
2. Actor worker 位于独立 `user/actor-workers/...`。
3. Actor `__init__` 执行时已经在独立 cgroup。
4. `__call__` 和其他 methods 的 PID/cgroup 不变。
5. `max_concurrency > 1` 时所有线程仍受聚合 CPU quota。
6. Async Actor coroutine 共享同一个 memory limit。
7. Actor 本地 subprocess 继承 Actor cgroup。
8. 嵌套 Ray task 不进入父 Actor cgroup。
9. CPU 密集 Actor 被 `cpu.max` throttling。
10. 超过 `memory.max` 时只触发 Actor cgroup OOM，不杀死 raylet/system process。
11. Actor restart 创建新 WorkerID cgroup，旧 cgroup最终删除。
12. `ray.kill()`、driver exit、detached Actor 和 raylet shutdown 均无残留 cgroup。

## 15. 安全性和兼容性

- 功能默认关闭。
- 现有 `--enable-resource-isolation` 行为不变。
- 只有双开关同时启用时才创建 `actor-workers`。
- 所有 cgroup path 都由 Ray 使用 ActorID/WorkerID 构造，不接受用户提供的相对路径或任意文件名。
- 创建、约束、迁移失败时不允许未隔离执行。
- Actor cgroup 只能位于 raylet 已拥有的 node cgroup 下。
- 不读取 Kubernetes Secret 或引入额外特权；继续依赖现有 writable cgroup v2 要求。

## 16. 分阶段实现建议

### 阶段一：核心隔离

- 新开关和双重门控。
- `actor-workers` hierarchy。
- Actor creation grant 前创建 cgroup、写约束、迁移 PID。
- disconnect 和 raylet shutdown 清理。
- fake driver、单元测试、基础集成测试。

### 阶段二：故障诊断

- `memory.events` OOM 识别。
- cgroup cleanup async retry。
- Actor restart/forced kill 完整测试。
- 日志和基础 metrics。

### 阶段三：增强能力

- 可配置 `memory.high` 与 `memory.max` 策略。
- 从 fork 前开始隔离 worker bootstrap。
- 更完整的 CPU throttling、memory peak metrics。
- 评估默认 Actor method CPU 的动态 cgroup 策略。

## 17. 关键设计结论

1. Per-Actor isolation 是现有 resource isolation 的可选子功能，不是替代品。
2. 必须同时设置 `--enable-resource-isolation` 和 `--enable-actor-resource-isolation`。
3. 一个 Actor worker incarnation 对应一个独立 cgroup。
4. 约束来自 Actor lifetime `required_resources`，而不是 RuntimeEnv 或 placement resources。
5. `max_concurrency` 不改变资源上限；所有并发调用共享 Actor 的聚合 CPU、memory budget。
6. 在 Actor creation grant reply 前迁移 PID，可以覆盖 `__init__` 和全部用户 Actor methods，同时避免修改通用 fork hook。
7. 普通 worker 保持位于现有 `user/workers`，Actor 使用新的 sibling hierarchy `user/actor-workers`。
8. 运行期配置失败必须使 Actor creation 失败，不能静默回退到未隔离执行。
