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


### 会话交接

撤流单有尚未关闭的会话时，可用 traffic 的 handoff-manifest 安排整单接收端。输入包含 case、原容器 uid、按名称索引的 gateways（各有 uid 与 capacity）和按会话索引的 assignments（各有 gateway 与 units）。接单后不可用口头 session-ack 清除待办。使用 session-adopt 录入同一 case、uid、session、manifest（安排事件编号）、gateway、gateway_uid 与 receipt。最后一个有效接单回执到达后撤流单为 drained，可经原 restart-settle 结算。没有接单安排的撤流单仍能用 session-ack 关闭会话。该入口按接收端正常联机的容量核对排布。

示例：`python -B -m probecourt traffic --db handoff.sqlite --fixture fixtures/handoff_normal.json`，再用 `python -B -m probecourt replay --db handoff.sqlite` 复核。

### 故障承诺交接（主备双网关）

撤流单最多八个不可拆会话、四个网关。handoff-manifest 改带 `sessions`（各会话固定整数容量）、`eligible`（每个会话经签认的可用接收端列表）即进入故障承诺路径；可附 `plan` 指定每个会话的主、备网关（必须不同且都在签认端内），不附则由确定性回溯求解器择定一份。签单同时核算五种场景：全部在线以及任意单个网关掉线。掉线时只允许该网关承担的主会话转去各自固定备网关，幸存主网关上的会话不得搬动；每种场景下每个网关的合计负载都不得超出签认容量。

拒签给出可重算的逐场景排布证据，并区分两类原因：`handoff_scenario_infeasible` 表示某种掉线本身无可行排布（证据含未排下会话），`handoff_guarantee_unsatisfiable` 表示各掉线分别可排但没有任何主备方案能共同承诺；显式 plan 违反某场景则为 `handoff_plan_violates_guarantee`，并附违反场景与逐场景证据。

签单后用 gateway-offline 报告掉线：已掉线端的旧接单不能再清掉该会话（先前在该主端录入的回执作废、会话回到 pending），备端激活后凭当前实际归属的网关与 UID 重新 session-adopt；幸存主端的会话不得搬动，重送已失联端原件或引用他单归属均返回 `handoff_receipt_mismatch`。全部当前实际目标的回执齐全后撤流单才为 drained，再经 restart-settle 结算。不带故障承诺的旧命令与已结算旧单保持原重放语义。

示例：`python -B -m probecourt traffic --fixture fixtures/handoff_guarantee_bcd.json`（A 掉线后实际归属 BCD）与 `... --fixture fixtures/handoff_guarantee_bcb.json`（实际归属 BCB）；`fixtures/handoff_guarantee_rejected.json` 是容量为 2 时在线与四份离线预案各自可排、却无法共同承诺的拒签原件。
