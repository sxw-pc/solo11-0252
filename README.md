# 容器探针误判与重启预算仲裁庭

本项目是离线云原生业务控制面模拟。使用 Python 标准库和 SQLite；数据、身份与时间均为合成 fixture，不操作真实集群。

## 三个业务子系统

### 探针证据庭

按来源可信度和连续窗口决定实例健康证据是否足以触发动作。持久化：probe_sources,probe_samples,health_windows 分别保存身份、代际、状态与相互引用的决定凭据。

入口：`python -m probecourt probe --db state.sqlite --fixture fixtures/probe_normal.json`。

### 重启预算庭

在服务维持最低可用数前提下裁定是否消耗预算进行重启。持久化：restart_policies,restart_credits,restart_decisions 分别保存身份、代际、状态与相互引用的决定凭据。

入口：`python -m probecourt budget --db state.sqlite --fixture fixtures/budget_normal.json`。

### 会话撤流庭

未排空会话时优先撤流并保留实例，而受控重启后凭新代际证据恢复流量。持久化：drain_cases,session_receipts,traffic_decisions 分别保存身份、代际、状态与相互引用的决定凭据。

入口：`python -m probecourt traffic --db state.sqlite --fixture fixtures/traffic_normal.json`。

## 首版业务能力

1. **探针来源认定**：探针来源认定 的独立裁定：探针进程身份与容器 UID 绑定后才计入健康判断。失败边界：探针来源认定 的异常输入或迟到事实：探针重启后的旧序号样本不冒充新证据
2. **连续窗口裁定**：连续窗口裁定 的独立裁定：连续失败和成功须分别满足可复核窗口才改变健康状态。失败边界：连续窗口裁定 的异常输入或迟到事实：一个成功样本不得抹去连续失败
3. **误判证据复核**：误判证据复核 的独立裁定：独立来源冲突时冻结动作并保存待裁证据。失败边界：误判证据复核 的异常输入或迟到事实：单个失信探针不能强制重启健康实例
4. **重启额度预留**：重启额度预留 的独立裁定：预算需按服务可用副本数与冷却债务原子保留。失败边界：重启额度预留 的异常输入或迟到事实：多个失败实例并发不能同时耗尽最后可用副本
5. **重启回执结算**：重启回执结算 的独立裁定：运行时回执绑定实例和容器代际后才消费预留额度。失败边界：重启回执结算 的异常输入或迟到事实：迟到旧重启回执不能关闭新决策
6. **重启冷却恢复**：重启冷却恢复 的独立裁定：冷却到期和新健康窗口共同恢复重启权。失败边界：重启冷却恢复 的异常输入或迟到事实：仅时钟到期不能掩盖持续崩溃
7. **会话排空裁定**：会话排空裁定 的独立裁定：有活跃会话时先撤流并等待会话确认再执行重启。失败边界：会话排空裁定 的异常输入或迟到事实：旧会话的解除回执不能替代当前排空
8. **流量资格恢复**：流量资格恢复 的独立裁定：新容器代际健康和重启结算均成立后恢复流量。失败边界：流量资格恢复 的异常输入或迟到事实：只看探针成功却未结清重启债务不得恢复

## 本地运行与验证

在仓库根目录运行：

```sh
python -m probecourt demo --db demo.sqlite
python -m probecourt replay --db demo.sqlite
python -m unittest discover -s tests -v
```

每个子系统提供一份独立正常 fixture 与一份实质失败 fixture；`fixtures/path.json` 串联跨子系统场景。每条命令保留原始输入、决定与不可变事件，`replay` 在空数据库重建状态并逐表核验。

## 业务边界

- **探针证据庭 → 重启预算庭**：实例 pod-a/uid-2 的两个独立探针连续失败使健康窗口从怀疑转为确认，重启预算因此为该实例预留一次额度；健康窗口若仍冲突则保留观察且不扣额度。失败分支：pod-a/uid-1 的迟到成功样本不得解除 uid-2 的确认失败，预算申请仍按 uid-2 原证据决定
- **重启预算庭 → 会话撤流庭**：同服务仅剩两个可用实例时，对 pod-a 预留重启额度会把会话排空单改为待执行；会话清空和重启回执同代到达后才允许恢复流量。失败分支：旧会话清空回执到达却缺新容器启动回执时，实例继续撤流且预算保持待结
- **探针证据庭 → 会话撤流庭**：探针误判复核把 pod-a 从确认失败改为证据冲突，撤流庭取消未执行的排空并保留已发重启决定的审计债务。失败分支：若重启已结算，迟到误判纠正不能把已发生的容器代际改回旧实例

该仓仅实现本地业务状态与回执协议；不连接真实 Kubernetes、设备插件、时间服务或告警平台。

### 挂载卷验收

对于磁盘恢复后才能开放流量的实例，通过 `volumes` 命令登记 `requirements-register` 卷清单与原容器 `snapshot-record`。替代容器启动后用 `snapshot-verify` 提交每个实际挂载卷的摘要；校验绑定撤流单、旧快照代际、现役替代代际及卷名。已登记的卷全部核对后才允许复流。未登记卷清单的旧流程仍直接复流。

运行 `python -B -m probecourt volumes --db volumes.sqlite --fixture fixtures/volumes_normal.json`，再执行 `python -B -m probecourt replay --db volumes.sqlite`。

### 跨卷事务恢复单

逐卷摘要各自为真并不代表业务写入完整：备用快照保留时间不齐时，各卷最新快照拼出的事务集合可能互相残缺。登记 `manifest-record` 跨卷事务清单后（每笔业务写入的事务号、参与卷、是否已签完成回执），验收升级为恢复单模式：

- `snapshot-record` 在每份真实快照上签认它包含的完整本地事务号集合；一次验收限同一原 UID、替代 UID、撤流单，最多四卷、每卷最多四份可挂快照，`snapshot-withdraw` 登记保留到期删除。
- `plan-propose` 为每个登记卷选定一份真实快照生成恢复单：每笔事务必须在全部参与卷同现或同现缺；已签完成回执的事务必须全体保留；若另一组可行真实快照能保留当前全部暂存事务且至少多一笔，当前方案不得签发。签发结果给出实际保留的事务集合。
- 复流须在 `traffic-restore` 引用已批准恢复单；`snapshot-verify` 逐卷实读摘要必须绑定恢复单实际选定的同一快照、当前替代 UID 与该案，摘要或 checkpoint 相同也不能以另一分支的卷代替，所有选定卷验证前不放行。

运行 `python -B -m probecourt volumes --db volumes.sqlite --fixture fixtures/volumes_plan.json`（8/13/7 恢复 P 与暂存事务 X）与 `fixtures/volumes_withdrawn.json`（含 P 快照被删后无可签方案、不放行），再执行 `python -B -m probecourt replay --db volumes.sqlite`。无事务清单的旧格式快照仍按原行为验收复流。
