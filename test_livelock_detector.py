# -*- coding: utf-8 -*-
import unittest

from livelock_detector import (
    CompactionSimulator,
    FileSpec,
    LevelSnapshot,
    LivelockDetector,
    RoundRecord,
    SSTable,
    SimulatorConfig,
    constant_workload,
)

HUGE = 10 ** 9


def tombstone_trace(rounds=10):
    """每轮向同一键范围写入 50% 墓碑的文件，L1 容量极大永不下沉。"""
    config = SimulatorConfig(
        level_capacities=[HUGE, HUGE, HUGE],
        l0_file_trigger=2,
        max_concurrent=2,
    )
    sim = CompactionSimulator(config)
    workload = constant_workload(
        size_bytes=1000, key_start=0, key_end=100, tombstones=500, entries=1000
    )
    return sim, sim.simulate(rounds, workload)


class NormalSinkingTest(unittest.TestCase):
    def test_normal_sinking_is_stable(self):
        config = SimulatorConfig(
            level_capacities=[500, 1500, HUGE],
            l0_file_trigger=2,
            max_concurrent=2,
        )
        sim = CompactionSimulator(config)

        def workload(r):
            return [FileSpec(key_start=r * 200, key_end=r * 200 + 50, size_bytes=400)]

        trace = sim.simulate(12, workload)
        self.assertTrue(any(rec.events for rec in trace), "应当发生过压缩")
        report = LivelockDetector(window=6).analyze(trace)
        self.assertFalse(report.livelock)
        self.assertEqual(report.status, "stable")
        self.assertIsNone(report.cycle_chain)
        self.assertEqual(report.issues, [])
        self.assertEqual(report.effective_bytes_end, report.total_bytes_end)
        self.assertGreater(report.effective_bytes_end, 0)


class TombstoneLivelockTest(unittest.TestCase):
    def test_tombstone_livelock_detected(self):
        _, trace = tombstone_trace(10)
        report = LivelockDetector(window=8).analyze(trace)
        self.assertTrue(report.livelock)
        self.assertEqual(report.status, "livelock")
        self.assertIsNotNone(report.cycle_chain)
        self.assertIn("L0->L1[0-100]", report.cycle_chain.signatures)
        self.assertGreaterEqual(report.cycle_chain.repeats, 3)
        kinds = {issue.kind for issue in report.issues}
        self.assertIn("tombstone-stall", kinds)
        self.assertGreaterEqual(
            report.garbage_ratio_end, report.garbage_ratio_start - 0.1
        )
        self.assertIsNotNone(report.abort_point)
        self.assertGreaterEqual(report.abort_point, report.window_start)
        self.assertLessEqual(report.abort_point, report.window_end)
        self.assertTrue(report.priorities)
        self.assertIn("墓碑", report.priorities[0])


class OverlappingContentionTest(unittest.TestCase):
    def test_overlapping_tasks_blocked(self):
        config = SimulatorConfig(
            level_capacities=[HUGE, 100, HUGE],
            l0_file_trigger=1,
            max_concurrent=2,
        )
        seed = SSTable(
            file_id="seed",
            level=1,
            key_start=0,
            key_end=60,
            size_bytes=500,
            entries=500,
        )
        sim = CompactionSimulator(config, initial_files=[seed])
        workload = constant_workload(size_bytes=300, key_start=40, key_end=100)
        trace = sim.simulate(8, workload)
        report = LivelockDetector(window=6).analyze(trace)
        kinds = {issue.kind for issue in report.issues}
        self.assertIn("task-blocking", kinds)
        blocking = next(i for i in report.issues if i.kind == "task-blocking")
        self.assertGreaterEqual(len(blocking.rounds), 3)
        self.assertTrue(any(rec.blocked for rec in trace), "模拟中应出现被阻塞的任务")
        self.assertTrue(any("阻塞" in p for p in report.priorities))


class WindowBoundaryTest(unittest.TestCase):
    def test_window_controls_detection(self):
        _, trace = tombstone_trace(10)
        small = LivelockDetector(window=2, repeat_threshold=3).analyze(trace)
        self.assertFalse(small.livelock)
        large = LivelockDetector(window=8, repeat_threshold=3).analyze(trace)
        self.assertTrue(large.livelock)

    def test_end_round_boundary(self):
        _, trace = tombstone_trace(10)
        detector = LivelockDetector(window=8, repeat_threshold=3)
        early = detector.analyze(trace, end_round=4)
        self.assertFalse(early.livelock)
        self.assertEqual(early.window_end, 4)
        later = detector.analyze(trace, end_round=5)
        self.assertTrue(later.livelock)
        self.assertEqual(later.window_end, 5)


class RecoveryTest(unittest.TestCase):
    def test_recovery_restabilizes(self):
        config = SimulatorConfig(
            level_capacities=[HUGE, HUGE, HUGE],
            l0_file_trigger=2,
            max_concurrent=2,
        )
        sim = CompactionSimulator(config)
        workload = constant_workload(
            size_bytes=1000, key_start=0, key_end=100, tombstones=500, entries=1000
        )
        trace = sim.simulate(10, workload)
        trace.extend(sim.simulate(1, None, major_compaction_rounds={10}))
        trace.extend(sim.simulate(4, None))

        detector = LivelockDetector(window=6)
        before = detector.analyze(trace, end_round=9)
        self.assertTrue(before.livelock)

        after = detector.analyze(trace)
        self.assertFalse(after.livelock)
        self.assertEqual(after.status, "recovered")
        self.assertEqual(after.issues, [])
        self.assertLess(after.garbage_ratio_end, after.garbage_ratio_start)
        self.assertEqual(after.garbage_ratio_end, 0.0)


class CapacityJitterTest(unittest.TestCase):
    def _record(self, idx, utilization, total, tombstone_bytes, expired_bytes):
        effective = total - tombstone_bytes - expired_bytes
        snaps = [
            LevelSnapshot(0, 0, 0, 0, 0, 0.0),
            LevelSnapshot(1, 0, 0, 0, 0, 0.0),
            LevelSnapshot(2, total, effective, tombstone_bytes, 1, utilization),
        ]
        return RoundRecord(round_index=idx, levels=snaps)

    def test_capacity_jitter_detected(self):
        utils = [1.2, 0.8, 1.3, 0.7, 1.25, 0.75]
        trace = [
            self._record(i, u, total=1000, tombstone_bytes=300, expired_bytes=200)
            for i, u in enumerate(utils)
        ]
        report = LivelockDetector(window=6).analyze(trace)
        self.assertFalse(report.livelock)
        kinds = {issue.kind for issue in report.issues}
        self.assertIn("capacity-jitter", kinds)
        self.assertEqual(report.effective_bytes_end, 500)
        self.assertEqual(report.total_bytes_end, 1000)
        self.assertAlmostEqual(report.garbage_ratio_end, 0.5)


if __name__ == "__main__":
    unittest.main()
