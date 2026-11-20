# -*- coding: utf-8 -*-
"""压缩候选选择模拟器。

只模拟“选哪些文件、往哪一层合并”的计划过程，不执行真实的文件合并。
每轮流程：
  1. 按写流量向 L0 注入新 SSTable；
  2. 各层根据容量 / L0 文件数 / 墓碑密度提出压缩候选；
  3. 键范围冲突或超出并发上限的候选被标记为阻塞；
  4. 执行选中的压缩（墓碑与过期版本只在落入最底层时丢弃）；
  5. 记录本轮快照。
"""

from typing import Callable, Iterable, List, Optional, Sequence, Union

from .models import (
    BlockedTask,
    CompactionEvent,
    FileSpec,
    LevelSnapshot,
    RoundRecord,
    SSTable,
)


class SimulatorConfig(object):
    def __init__(
        self,
        level_capacities,
        l0_file_trigger=4,
        max_concurrent=2,
        tombstone_seek_ratio=None,
    ):
        if not level_capacities:
            raise ValueError("level_capacities 不能为空")
        self.level_capacities = list(level_capacities)
        self.num_levels = len(self.level_capacities)
        self.l0_file_trigger = l0_file_trigger
        self.max_concurrent = max(1, max_concurrent)
        # 墓碑比例超过该值的文件会被主动下沉；None 表示关闭
        self.tombstone_seek_ratio = tombstone_seek_ratio


class _Candidate(object):
    def __init__(self, level, input_files, output_files, reason):
        self.level = level
        self.input_files = list(input_files)
        self.output_files = list(output_files)
        self.reason = reason
        all_files = self.input_files + self.output_files
        self.key_start = min(f.key_start for f in all_files)
        self.key_end = max(f.key_end for f in all_files)

    def overlaps(self, other):
        return self.key_start <= other.key_end and other.key_start <= self.key_end


def constant_workload(
    size_bytes,
    key_start,
    key_end,
    tombstones=0,
    expired_bytes=0,
    entries=1000,
    files_per_round=1,
):
    """生成每轮写入相同键范围文件的工作负载函数。"""

    def gen(round_index):
        return [
            FileSpec(
                key_start=key_start,
                key_end=key_end,
                size_bytes=size_bytes,
                entries=entries,
                tombstones=tombstones,
                expired_bytes=expired_bytes,
            )
            for _ in range(files_per_round)
        ]

    return gen


class CompactionSimulator(object):
    def __init__(self, config, initial_files=()):
        self.config = config
        self.levels = [[] for _ in range(config.num_levels)]
        self._counter = 0
        self._round = 0
        for table in initial_files:
            self.levels[table.level].append(table)

    # ------------------------------------------------------------------ API

    def run_round(self, workload=None, major_compaction=False):
        """推进一轮，返回 RoundRecord。"""
        events = []
        blocked = []
        if workload is not None:
            for spec in workload(self._round):
                self.add_file(0, spec)
        if major_compaction:
            event = self._major_compaction()
            if event is not None:
                events.append(event)
        else:
            selected = self._select(self.config.max_concurrent, blocked)
            for cand in selected:
                events.append(self._execute(cand))
        record = RoundRecord(
            round_index=self._round,
            events=events,
            blocked=blocked,
            levels=self._snapshot(),
        )
        self._round += 1
        return record

    def simulate(self, rounds, workload=None, major_compaction_rounds=()):
        trace = []
        for _ in range(rounds):
            trace.append(
                self.run_round(
                    workload=workload,
                    major_compaction=self._round in major_compaction_rounds,
                )
            )
        return trace

    def add_file(self, level, spec):
        table = SSTable(
            file_id=self._new_id(),
            level=level,
            key_start=spec.key_start,
            key_end=spec.key_end,
            size_bytes=spec.size_bytes,
            entries=spec.entries,
            tombstones=spec.tombstones,
            expired_bytes=spec.expired_bytes,
        )
        self.levels[level].append(table)
        return table

    # ------------------------------------------------------------- 内部实现

    def _new_id(self):
        self._counter += 1
        return "f{:06d}".format(self._counter)

    def _level_size(self, level):
        return sum(f.size_bytes for f in self.levels[level])

    def _select(self, max_concurrent, blocked):
        selected = []
        for cand in self._candidates():
            if len(selected) >= max_concurrent:
                blocked.append(self._blocked(cand, "concurrency-limit"))
                continue
            clash = next((c for c in selected if c.overlaps(cand)), None)
            if clash is not None:
                blocked.append(
                    self._blocked(
                        cand, "overlap-with-L{}->L{}".format(clash.level, clash.level + 1)
                    )
                )
            else:
                selected.append(cand)
        return selected

    def _blocked(self, cand, by):
        return BlockedTask(
            round_index=self._round,
            level=cand.level,
            key_start=cand.key_start,
            key_end=cand.key_end,
            blocked_by=by,
        )

    def _candidates(self):
        cands = []
        for level in range(self.config.num_levels - 1):
            files = self.levels[level]
            if not files:
                continue
            reason = None
            if level == 0:
                if len(files) >= self.config.l0_file_trigger:
                    reason = "l0-trigger"
                elif self._level_size(0) > self.config.level_capacities[0]:
                    reason = "capacity"
            elif self._level_size(level) > self.config.level_capacities[level]:
                reason = "capacity"
            anchor = None
            if reason is not None:
                anchor = self._pick_file(level)
            elif self.config.tombstone_seek_ratio is not None:
                heavy = [
                    f
                    for f in files
                    if f.tombstone_ratio >= self.config.tombstone_seek_ratio
                ]
                if heavy:
                    reason = "tombstone-seek"
                    anchor = max(heavy, key=lambda f: (f.tombstone_ratio, f.file_id))
            if anchor is not None:
                cands.append(self._build_candidate(level, anchor, reason))
        return cands

    def _pick_file(self, level):
        """选与下一层重叠字节最多、墓碑最密的文件（确定性平局）。"""

        def score(f):
            overlap = sum(
                g.size_bytes
                for g in self.levels[level + 1]
                if g.overlaps_range(f.key_start, f.key_end)
            )
            return (-overlap, -f.tombstone_ratio, f.key_start, f.file_id)

        return min(self.levels[level], key=score)

    def _build_candidate(self, level, anchor, reason):
        if level == 0:
            # L0 文件无序，所有与锚点重叠的 L0 文件一起参与
            inputs = [
                f
                for f in self.levels[0]
                if f.overlaps_range(anchor.key_start, anchor.key_end)
            ]
        else:
            inputs = [anchor]
        key_start = min(f.key_start for f in inputs)
        key_end = max(f.key_end for f in inputs)
        outputs = [
            f
            for f in self.levels[level + 1]
            if f.overlaps_range(key_start, key_end)
        ]
        return _Candidate(level, inputs, outputs, reason)

    def _execute(self, cand):
        out_level = cand.level + 1
        inputs = cand.input_files + cand.output_files
        return self._merge(inputs, cand.level, out_level, cand.reason)

    def _merge(self, inputs, input_level, out_level, reason):
        is_last = out_level == self.config.num_levels - 1
        bytes_in = sum(f.size_bytes for f in inputs)
        dropped_tombstones = sum(f.tombstones for f in inputs) if is_last else 0
        dropped_tomb_bytes = sum(f.tombstone_bytes for f in inputs) if is_last else 0
        dropped_expired = sum(f.expired_bytes for f in inputs) if is_last else 0
        size_out = bytes_in - dropped_tomb_bytes - dropped_expired
        entries_out = sum(f.entries for f in inputs)
        tombstones_out = sum(f.tombstones for f in inputs) - dropped_tombstones
        expired_out = sum(f.expired_bytes for f in inputs) - dropped_expired
        key_start = min(f.key_start for f in inputs)
        key_end = max(f.key_end for f in inputs)
        for f in inputs:
            self.levels[f.level].remove(f)
        if size_out > 0:
            self.levels[out_level].append(
                SSTable(
                    file_id=self._new_id(),
                    level=out_level,
                    key_start=key_start,
                    key_end=key_end,
                    size_bytes=size_out,
                    entries=entries_out,
                    tombstones=tombstones_out,
                    expired_bytes=expired_out,
                )
            )
        return CompactionEvent(
            round_index=self._round,
            input_level=input_level,
            output_level=out_level,
            key_start=key_start,
            key_end=key_end,
            input_files=[f.file_id for f in inputs],
            bytes_in=bytes_in,
            bytes_out=max(0, size_out),
            tombstones_dropped=dropped_tombstones,
            expired_dropped=dropped_expired,
            reason=reason,
        )

    def _major_compaction(self):
        all_files = [f for lvl in self.levels for f in lvl]
        if not all_files:
            return None
        last = self.config.num_levels - 1
        input_level = min(f.level for f in all_files)
        return self._merge(all_files, input_level, last, "major")

    def _snapshot(self):
        snaps = []
        for lvl, files in enumerate(self.levels):
            total = sum(f.size_bytes for f in files)
            effective = sum(f.effective_bytes for f in files)
            tomb = sum(f.tombstone_bytes for f in files)
            cap = self.config.level_capacities[lvl]
            util = total / float(cap) if cap > 0 else 0.0
            snaps.append(
                LevelSnapshot(
                    level=lvl,
                    total_bytes=total,
                    effective_bytes=effective,
                    tombstone_bytes=tomb,
                    file_count=len(files),
                    utilization=util,
                )
            )
        return snaps
