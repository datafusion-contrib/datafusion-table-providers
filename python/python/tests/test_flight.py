from datafusion import SessionContext
from datafusion_table_providers import flight
import pytest
import pyarrow as pa
import pyarrow.parquet as pq
import socket
import subprocess
import time
from pathlib import Path


def _wait_for_port(host: str, port: int, timeout_secs: float = 30.0) -> None:
    deadline = time.monotonic() + timeout_secs
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((host, port), timeout=1.0):
                return
        except OSError:
            time.sleep(0.25)
    raise TimeoutError(f"Timed out waiting for {host}:{port}")


class TestFlightIntegration:
    @classmethod
    def setup_class(cls):
        """Called once before all test methods in the class"""
        cls.ctx = SessionContext()
        cls.pool = flight.FlightTableFactory()

        # Small local fixture — avoids downloading a multi-GB NYC taxi parquet in CI.
        fixture_dir = Path(__file__).resolve().parent / "fixtures"
        fixture_dir.mkdir(exist_ok=True)
        cls.parquet_path = fixture_dir / "taxi_sample.parquet"
        # Distinct group sizes so ORDER BY COUNT(*) DESC is deterministic.
        table = pa.table(
            {
                "VendorID": pa.array([2, 2, 2, 1, 1, 6], type=pa.int32()),
                "passenger_count": pa.array(
                    [1, 2, 1, 1, None, None], type=pa.float64()
                ),
                "total_amount": pa.array(
                    [10.5, 20.0, 15.0, 5.0, 7.25, 100.03], type=pa.float64()
                ),
            }
        )
        pq.write_table(table, cls.parquet_path)

        cls.process = subprocess.Popen(
            [
                "roapi",
                "-t",
                f"taxi={cls.parquet_path}",
                "--addr-http",
                "127.0.0.1:18080",
                "--addr-postgres",
                "127.0.0.1:15432",
                "--addr-flight-sql",
                "127.0.0.1:32010",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        try:
            _wait_for_port("127.0.0.1", 32010)
            # Brief settle time for Flight SQL registration after the port opens.
            time.sleep(1)
        except Exception:
            cls.process.kill()
            stdout, stderr = cls.process.communicate(timeout=5)
            raise RuntimeError(
                "roapi failed to become ready.\n"
                f"stdout:\n{stdout.decode(errors='replace')}\n"
                f"stderr:\n{stderr.decode(errors='replace')}"
            ) from None

    @classmethod
    def teardown_class(cls):
        """Called once after all test methods in the class"""
        cls.process.kill()
        cls.process.wait(timeout=5)

    def test_query_companies(self):
        """Test querying companies table with SQL"""
        print("Running test_query_companies")
        table_name = "taxi_flight_table"
        self.ctx.register_table_provider(
            table_name,
            self.pool.get_table(
                "http://localhost:32010",
                {"flight.sql.query": "SELECT * FROM taxi"},
            ),
        )
        df = self.ctx.sql(
            f"""
            SELECT "VendorID", COUNT(*) as count, SUM(passenger_count) as passenger_counts, SUM(total_amount) as total_amounts
                FROM {table_name}
                GROUP BY "VendorID"
                ORDER BY COUNT(*) DESC
            """
        )
        result = df.collect()

        vendor_ids = result[0]["VendorID"].tolist()
        assert vendor_ids == [2, 1, 6]

        counts = result[0]["count"].tolist()
        assert counts == [3, 2, 1]

        passenger_counts = result[0]["passenger_counts"].tolist()
        assert passenger_counts == [4.0, 1.0, None]

        total_amounts = result[0]["total_amounts"].tolist()
        assert total_amounts == pytest.approx([45.5, 12.25, 100.03])
