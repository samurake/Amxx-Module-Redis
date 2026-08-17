from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[2]


class RuntimeGateContractTest(unittest.TestCase):
    def test_gate_loads_the_real_artifact_and_is_network_isolated(self) -> None:
        compose = (ROOT / "tests/runtime/compose.yml").read_text(encoding="utf-8")
        entrypoint = (ROOT / "tests/runtime/entrypoint.sh").read_text(encoding="utf-8")

        self.assertIn("REDIS_MODULE_PATH", compose)
        self.assertIn(":/module/redis_amxx_i386.so:ro", compose)
        self.assertIn("internal: true", compose)
        self.assertNotIn("ports:", compose)
        self.assertIn("install -m 0755", entrypoint)
        self.assertIn("redis_xadd_validation_test.amxx", entrypoint)
        self.assertIn("redis_runtime_test.amxx", entrypoint)

    def test_runtime_versions_and_fault_scenarios_are_pinned(self) -> None:
        dockerfile = (ROOT / "tests/runtime/Dockerfile").read_text(encoding="utf-8")
        runner = (ROOT / "tests/runtime/run.py").read_text(encoding="utf-8")

        self.assertIn("REHLDS_VERSION=3.15.0.896", dockerfile)
        self.assertIn("REGAMEDLL_VERSION=5.30.0.814", dockerfile)
        self.assertIn("METAMOD_VERSION=1.3.0.149", dockerfile)
        self.assertIn("AMXX_BUILD=5467", dockerfile)
        self.assertIn("chmod +x /opt/hlds/hlds_linux", dockerfile)
        self.assertIn("/root/.steam/sdk32/steamclient.so", dockerfile)
        self.assertIn('["stop", "redis"]', runner)
        self.assertIn('["start", "redis"]', runner)
        self.assertIn("outage_backpressure_and_recovery", runner)
        self.assertIn("map_change_callback_lifecycle", runner)
        self.assertIn('["down", "--volumes", "--remove-orphans"]', runner)


if __name__ == "__main__":
    unittest.main()
