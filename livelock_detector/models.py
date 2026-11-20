# -*- coding: utf-8 -*-
"""数据模型：SSTable、压缩事件、轮次快照。

有效字节（effective_bytes）一律按 ``size - tombstone_bytes - expired_bytes``
计算，即扣除墓碑和已过期版本，而不是只看总字节数。
"""

from dataclasses import dataclass, field
from typing import List


@dataclass
class FileSpec:
    """一次写入流量产生的新 SSTable 描述（用于工作负载输入）。"""

    key_start: int
    key_end: int
    size_bytes: int
    entries: int = 1000
    tombstones: int = 0
    expired_bytes: int = 0


@dataclass
class SSTable:
    file_id: str
    level: int
    key_start: int
    key_end: int
    size_bytes: int
    entries: int = 1000
    tombstones: int = 0
    expired_bytes: int = 0

    @property
    def tombstone_ratio(self):
        if self.entries <= 0:
            return 0.0
        return min(self.tombstones, self.entries) / float(self.entries)

    @property
    def tombstone_bytes(self):
        return int(self.size_bytes * self.tombstone_ratio)

    @property
    def effective_bytes(self):
        """扣除墓碑与已过期版本后的有效数据量。"""
        return max(0, self.size_bytes - self.tombstone_bytes - self.expired_bytes)

    def overlaps_range(self, key_start, key_end):
        return self.key_start <= key_end and key_start <= self.key_end


@dataclass
class CompactionEvent:
    round_index: int
    input_level: int
    output_level: int
    key_start: int
    key_end: int
    input_files: List[str] = field(default_factory=list)
    bytes_in: int = 0
    bytes_out: int = 0
    tombstones_dropped: int = 0
    expired_dropped: int = 0
    reason: str = "capacity"

    @property
    def signature(self):
        """键范围签名，用于识别同一范围被反复重写。"""
        return "L{}->L{}[{}-{}]".format(
            self.input_level, self.output_level, self.key_start, self.key_end
        )


@dataclass
class BlockedTask:
    round_index: int
    level: int
    key_start: int
    key_end: int
    blocked_by: str


@dataclass
class LevelSnapshot:
    level: int
    total_bytes: int
    effective_bytes: int
    tombstone_bytes: int
    file_count: int
    utilization: float


@dataclass
class RoundRecord:
    round_index: int
    events: List[CompactionEvent] = field(default_factory=list)
    blocked: List[BlockedTask] = field(default_factory=list)
    levels: List[LevelSnapshot] = field(default_factory=list)

    @property
    def total_bytes(self):
        return sum(s.total_bytes for s in self.levels)

    @property
    def effective_bytes(self):
        return sum(s.effective_bytes for s in self.levels)

    @property
    def tombstone_bytes(self):
        return sum(s.tombstone_bytes for s in self.levels)

    @property
    def garbage_ratio(self):
        total = self.total_bytes
        if total <= 0:
            return 0.0
        return (total - self.effective_bytes) / float(total)
