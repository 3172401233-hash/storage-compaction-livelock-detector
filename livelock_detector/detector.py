# -*- coding: utf-8 -*-
"""压缩活锁检测器。

在检测窗口内分析模拟轨迹，识别四类问题：
  * range-rewrite-cycle  同一键范围被反复重写且没有净进展（活锁）
  * tombstone-stall      墓碑长期滞留在非最底层，无法下沉
  * capacity-jitter      低层容量利用率围绕阈值反复抖动
  * task-blocking        多个压缩任务因键范围冲突 / 并发上限互相阻塞

判断“有无进展”时使用有效字节（扣除墓碑与过期版本），而不是总字节。
"""

from collections import Counter
from dataclasses import dataclass, field
from typing import List, Optional

from .models import RoundRecord


@dataclass
class Issue:
    kind: str
    severity: str
    detail: str
    rounds: List[int] = field(default_factory=list)


@dataclass
class CycleChain:
    signatures: List[str]
    rounds: List[int]
    repeats: int


@dataclass
class Report:
    status: str  # stable / degraded / livelock / recovered
    livelock: bool
    window_start: int
    window_end: int
    cycle_chain: Optional[CycleChain]
    abort_point: Optional[int]
    issues: List[Issue]
    priorities: List[str]
    total_bytes_start: int
    total_bytes_end: int
    effective_bytes_start: int
    effective_bytes_end: int
    garbage_ratio_start: float
    garbage_ratio_end: float


class LivelockDetector(object):
    def __init__(
        self,
        window=8,
        repeat_threshold=3,
        progress_tolerance=0.1,
        jitter_min_flips=2,
        jitter_min_amplitude=0.3,
    ):
        if window < 1:
            raise ValueError("window 必须 >= 1")
        self.window = window
        self.repeat_threshold = repeat_threshold
        self.progress_tolerance = progress_tolerance
        self.jitter_min_flips = jitter_min_flips
        self.jitter_min_amplitude = jitter_min_amplitude

    def analyze(self, trace, end_round=None):
        """分析轨迹中位于检测窗口内的部分，返回 Report。"""
        records = [
            r for r in trace if end_round is None or r.round_index <= end_round
        ]
        records = records[-self.window:]
        if not records:
            raise ValueError("检测窗口内没有任何轮次记录")

        cycle = self._detect_cycle(records)
        issues = []
        for issue in (
            self._detect_tombstone_stall(records),
            self._detect_capacity_jitter(records),
            self._detect_blocking(records),
        ):
            if issue is not None:
                issues.append(issue)

        livelock = cycle is not None
        if livelock:
            status = "livelock"
        elif issues:
            status = "degraded"
        else:
            status = "stable"
        if not livelock and not issues and end_round is None:
            previous = trace[-2 * self.window:-self.window]
            if len(previous) >= self.repeat_threshold and self._detect_cycle(previous):
                status = "recovered"

        abort_point = None
        if cycle is not None or issues:
            abort_point = self._find_abort_point(records)

        start, end = records[0], records[-1]
        return Report(
            status=status,
            livelock=livelock,
            window_start=start.round_index,
            window_end=end.round_index,
            cycle_chain=cycle,
            abort_point=abort_point,
            issues=issues,
            priorities=self._priorities(cycle, issues, abort_point),
            total_bytes_start=start.total_bytes,
            total_bytes_end=end.total_bytes,
            effective_bytes_start=start.effective_bytes,
            effective_bytes_end=end.effective_bytes,
            garbage_ratio_start=start.garbage_ratio,
            garbage_ratio_end=end.garbage_ratio,
        )

    def _detect_cycle(self, records):
        events = [
            (rec.round_index, ev.signature) for rec in records for ev in rec.events
        ]
        if not events:
            return None
        counts = Counter(sig for _, sig in events)
        dominant, count = max(counts.items(), key=lambda kv: (kv[1], kv[0]))
        if count < self.repeat_threshold:
            return None
        if not self._no_progress(records):
            return None
        positions = [i for i, (_, sig) in enumerate(events) if sig == dominant]
        if len(positions) >= 2:
            unit = [sig for _, sig in events[positions[0]:positions[1] + 1]]
        else:
            unit = [dominant]
        rounds = sorted({r for r, sig in events if sig == dominant})
        return CycleChain(signatures=unit, rounds=rounds, repeats=count)

    def _no_progress(self, records):
        """总字节未明显下降且垃圾占比未改善，视为没有净进展。"""
        tol = self.progress_tolerance
        start, end = records[0], records[-1]
        return (
            end.total_bytes >= start.total_bytes * (1 - tol)
            and end.garbage_ratio >= start.garbage_ratio - tol
        )

    def _upper_tombstone_bytes(self, record):
        if len(record.levels) < 2:
            return 0
        return sum(s.tombstone_bytes for s in record.levels[:-1])

    def _detect_tombstone_stall(self, records):
        dropped = sum(ev.tombstones_dropped for rec in records for ev in rec.events)
        compactions = sum(len(rec.events) for rec in records)
        tb_start = self._upper_tombstone_bytes(records[0])
        tb_end = self._upper_tombstone_bytes(records[-1])
        stalled = (
            tb_end > 0
            and dropped == 0
            and tb_end >= tb_start * 0.95
            and compactions >= 2
        )
        if not stalled:
            return None
        rounds = [rec.round_index for rec in records if rec.events]
        return Issue(
            kind="tombstone-stall",
            severity="high",
            detail=(
                "窗口内非最底层墓碑字节从 {} 变为 {}，"
                "压缩未丢弃任何墓碑，墓碑无法下沉".format(tb_start, tb_end)
            ),
            rounds=rounds,
        )

    def _detect_capacity_jitter(self, records):
        num_levels = len(records[0].levels)
        worst = None
        for lvl in range(num_levels):
            utils = [rec.levels[lvl].utilization for rec in records]
            signs = [1 if u > 1.0 else (-1 if u < 1.0 else 0) for u in utils]
            flips = sum(
                1 for a, b in zip(signs, signs[1:]) if a != 0 and b != 0 and a != b
            )
            amplitude = max(utils) - min(utils)
            hit = flips >= self.jitter_min_flips and amplitude >= self.jitter_min_amplitude
            if hit and (worst is None or flips > worst[0]):
                worst = (flips, lvl, amplitude)
        if worst is None:
            return None
        flips, lvl, amplitude = worst
        return Issue(
            kind="capacity-jitter",
            severity="medium",
            detail="L{} 利用率在窗口内围绕容量阈值翻转 {} 次，振幅 {:.2f}".format(
                lvl, flips, amplitude
            ),
            rounds=[rec.round_index for rec in records],
        )

    def _detect_blocking(self, records):
        blocked_rounds = [rec.round_index for rec in records if rec.blocked]
        if not blocked_rounds:
            return None
        total = sum(len(rec.blocked) for rec in records)
        return Issue(
            kind="task-blocking",
            severity="medium",
            detail="窗口内 {} 个压缩任务被阻塞（键范围冲突或并发上限）".format(total),
            rounds=blocked_rounds,
        )

    def _find_abort_point(self, records):
        """最稳定的中止点：超容量程度、阻塞数与垃圾占比综合最低的轮次。"""

        def score(rec):
            overshoot = sum(max(0.0, s.utilization - 1.0) for s in rec.levels)
            return overshoot + 0.5 * len(rec.blocked) + rec.garbage_ratio

        return min(records, key=lambda r: (score(r), r.round_index)).round_index

    def _priorities(self, cycle, issues, abort_point):
        kinds = {issue.kind for issue in issues}
        prios = []
        rank = 1
        if "tombstone-stall" in kinds:
            prios.append(
                "P{} 墓碑下沉：对滞留键范围触发直达最底层的大合并，"
                "让墓碑在末层丢弃以回收空间".format(rank)
            )
            rank += 1
        if cycle is not None:
            prios.append(
                "P{} 打破重写循环：在轮次 {}（最稳定中止点）暂停或限流循环链 "
                "{}".format(rank, abort_point, " -> ".join(cycle.signatures))
            )
            rank += 1
        if "capacity-jitter" in kinds:
            prios.append(
                "P{} 平滑低层容量：调整层容量配比或合并粒度，"
                "避免利用率反复穿越阈值".format(rank)
            )
            rank += 1
        if "task-blocking" in kinds:
            prios.append(
                "P{} 解除任务阻塞：错开并发任务的键范围，"
                "或提高 max_concurrent".format(rank)
            )
            rank += 1
        return prios
