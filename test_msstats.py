import unittest
import os
import tempfile
import json
from unittest.mock import patch, MagicMock
import openpyxl
from google.api_core.exceptions import ResourceExhausted
from google.cloud import monitoring_v3

from msstats import (
    extractDatabaseName,
    get_command_by_args,
    get_all_commands,
    processNodeStats,
    processMetricPoint,
    get_project_from_service_account_and_authenticate,
    process_google_project,
    create_workbooks,
    list_time_series,
)

from memorystore import (
    _resolve_inst_key,
    _attach_memory_usage,
    _attach_capacity_scalar,
    _accumulate_commands,
    _normalize_location,
    collect_for_product,
    REDIS_METRICS,
    main as memorystore_main,
)


class TestMSSStats(unittest.TestCase):
    """Test suite for msstats utility functions"""

    def test_extractDatabaseName(self):
        """Test extracting database name from instance ID"""
        # Test typical instance ID format
        instance_id = (
            "projects/test-project/locations/us-central1/instances/redis-instance"
        )
        expected = "redis-instance"
        result = extractDatabaseName(instance_id)
        self.assertEqual(result, expected)

        # Test with different format
        instance_id = "projects/my-project/locations/europe-west1/instances/my-redis-db"
        expected = "my-redis-db"
        result = extractDatabaseName(instance_id)
        self.assertEqual(result, expected)

        # Test edge case with single part
        instance_id = "redis-simple"
        expected = "redis-simple"
        result = extractDatabaseName(instance_id)
        self.assertEqual(result, expected)

    def test_get_command_by_args(self):
        """Test getting command counts by specific arguments"""
        commands = {"get": 100, "set": 50, "del": 25, "incr": 10, "exists": 5}

        # Test single command
        result = get_command_by_args(commands, "get")
        self.assertEqual(result, 100)

        # Test multiple commands
        result = get_command_by_args(commands, "get", "set")
        self.assertEqual(result, 150)

        # Test with non-existent command
        result = get_command_by_args(commands, "nonexistent")
        self.assertEqual(result, 0)

        # Test mix of existing and non-existent commands
        result = get_command_by_args(commands, "get", "nonexistent", "set")
        self.assertEqual(result, 150)

        # Test empty args
        result = get_command_by_args(commands)
        self.assertEqual(result, 0)

    def test_get_all_commands(self):
        """Test getting total count of all commands"""
        commands = {"get": 100, "set": 50, "del": 25, "incr": 10}

        result = get_all_commands(commands)
        self.assertEqual(result, 185)

        # Test empty commands dict
        result = get_all_commands({})
        self.assertEqual(result, 0)

        # Test single command
        result = get_all_commands({"test": 42})
        self.assertEqual(result, 42)

    def test_processNodeStats(self):
        """Test processing node statistics to get maximum values"""
        # Create sample processed metric points from multiple nodes
        processed_metric_points = {
            "node1": {
                "Throughput (Ops)": 100,
                "GetTypeCmds": 50,
                "SetTypeCmds": 30,
                "OtherTypeCmds": 20,
                "BitmapBasedCmds": 0,
                "ClusterBasedCmds": 0,
                "EvalBasedCmds": 0,
                "GeoSpatialBasedCmds": 0,
                "HashBasedCmds": 10,
                "HyperLogLogBasedCmds": 0,
                "KeyBasedCmds": 5,
                "ListBasedCmds": 0,
                "PubSubBasedCmds": 0,
                "SetBasedCmds": 0,
                "SortedSetBasedCmds": 0,
                "StringBasedCmds": 15,
                "StreamBasedCmds": 0,
                "TransactionBasedCmds": 0,
            },
            "node2": {
                "Throughput (Ops)": 150,  # Higher than node1
                "GetTypeCmds": 40,
                "SetTypeCmds": 60,  # Higher than node1
                "OtherTypeCmds": 10,
                "BitmapBasedCmds": 5,  # Higher than node1
                "ClusterBasedCmds": 0,
                "EvalBasedCmds": 0,
                "GeoSpatialBasedCmds": 0,
                "HashBasedCmds": 8,
                "HyperLogLogBasedCmds": 0,
                "KeyBasedCmds": 12,  # Higher than node1
                "ListBasedCmds": 0,
                "PubSubBasedCmds": 0,
                "SetBasedCmds": 0,
                "SortedSetBasedCmds": 0,
                "StringBasedCmds": 10,
                "StreamBasedCmds": 0,
                "TransactionBasedCmds": 0,
            },
        }

        result = processNodeStats(processed_metric_points)

        # Should take maximum values across all nodes
        self.assertEqual(result["Throughput (Ops)"], 150)  # max from node2
        self.assertEqual(result["GetTypeCmds"], 50)  # max from node1
        self.assertEqual(result["SetTypeCmds"], 60)  # max from node2
        self.assertEqual(result["BitmapBasedCmds"], 5)  # max from node2
        self.assertEqual(result["KeyBasedCmds"], 12)  # max from node2

    def test_processMetricPoint(self):
        """Test processing a single metric point"""
        # Create sample metric point data
        metric_point = {
            "get": 50,
            "set": 30,
            "hget": 20,  # Hash command
            "lpush": 10,  # List command
            "sadd": 5,  # Set command
            "unknown_cmd": 15,  # Should be counted in total but not categorized
        }

        result = processMetricPoint(metric_point)

        # Check that all commands are counted in throughput
        expected_throughput = sum(metric_point.values())
        self.assertEqual(result["Throughput (Ops)"], expected_throughput)

        # Check that hash commands are properly categorized
        self.assertEqual(result["HashBasedCmds"], 20)  # hget

        # Check that list commands are properly categorized
        self.assertEqual(result["ListBasedCmds"], 10)  # lpush

        # Check that set commands are properly categorized
        self.assertEqual(result["SetBasedCmds"], 5)  # sadd

        # Verify structure - all expected keys should be present
        expected_keys = {
            "Throughput (Ops)",
            "GetTypeCmds",
            "SetTypeCmds",
            "OtherTypeCmds",
            "BitmapBasedCmds",
            "ClusterBasedCmds",
            "EvalBasedCmds",
            "GeoSpatialBasedCmds",
            "HashBasedCmds",
            "HyperLogLogBasedCmds",
            "KeyBasedCmds",
            "ListBasedCmds",
            "PubSubBasedCmds",
            "SetBasedCmds",
            "SortedSetBasedCmds",
            "StringBasedCmds",
            "StreamBasedCmds",
            "TransactionBasedCmds",
        }
        self.assertEqual(set(result.keys()), expected_keys)

    def test_processMetricPoint_empty(self):
        """Test processing empty metric point"""
        result = processMetricPoint({})

        # All values should be 0
        for value in result.values():
            self.assertEqual(value, 0)

    def test_processNodeStats_single_node(self):
        """Test processing node stats with single node"""
        processed_metric_points = {
            "node1": {
                "Throughput (Ops)": 100,
                "GetTypeCmds": 50,
                "SetTypeCmds": 30,
                "OtherTypeCmds": 20,
                "BitmapBasedCmds": 0,
                "ClusterBasedCmds": 0,
                "EvalBasedCmds": 0,
                "GeoSpatialBasedCmds": 0,
                "HashBasedCmds": 10,
                "HyperLogLogBasedCmds": 0,
                "KeyBasedCmds": 5,
                "ListBasedCmds": 0,
                "PubSubBasedCmds": 0,
                "SetBasedCmds": 0,
                "SortedSetBasedCmds": 0,
                "StringBasedCmds": 15,
                "StreamBasedCmds": 0,
                "TransactionBasedCmds": 0,
            }
        }

        result = processNodeStats(processed_metric_points)

        # Should return the same values as the single node
        self.assertEqual(result["Throughput (Ops)"], 100)
        self.assertEqual(result["GetTypeCmds"], 50)
        self.assertEqual(result["HashBasedCmds"], 10)

    def test_processNodeStats_empty(self):
        """Test processing empty node stats"""
        result = processNodeStats({})

        # Should return default structure with all zeros
        expected_keys = {
            "Throughput (Ops)",
            "GetTypeCmds",
            "SetTypeCmds",
            "OtherTypeCmds",
            "BitmapBasedCmds",
            "ClusterBasedCmds",
            "EvalBasedCmds",
            "GeoSpatialBasedCmds",
            "HashBasedCmds",
            "HyperLogLogBasedCmds",
            "KeyBasedCmds",
            "ListBasedCmds",
            "PubSubBasedCmds",
            "SetBasedCmds",
            "SortedSetBasedCmds",
            "StringBasedCmds",
            "StreamBasedCmds",
            "TransactionBasedCmds",
        }

        self.assertEqual(set(result.keys()), expected_keys)
        for value in result.values():
            self.assertEqual(value, 0)


class TestMSStatsIntegration(unittest.TestCase):
    """Integration tests for msstats functionality"""

    def setUp(self):
        """Set up test fixtures"""
        self.temp_dir = tempfile.mkdtemp()
        self.test_project_id = "test-project-123"

        # Create a mock service account file with required fields
        self.service_account_data = {
            "project_id": self.test_project_id,
            "type": "service_account",
            "private_key_id": "test-key-id",
            "private_key": "Some Mock Private Key\n",
            "client_email": "test@test-project.iam.gserviceaccount.com",
            "client_id": "123456789",
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
        }

        self.service_account_file = os.path.join(
            self.temp_dir, "test-service-account.json"
        )
        with open(self.service_account_file, "w") as f:
            json.dump(self.service_account_data, f)

    def tearDown(self):
        """Clean up test fixtures"""
        if os.path.exists(self.service_account_file):
            os.remove(self.service_account_file)
        os.rmdir(self.temp_dir)

    def _mock_command_series(self):
        """Mock commands/calls time series for a single primary node"""
        mock_result = MagicMock()
        mock_result.resource.labels = MagicMock()
        mock_result.resource.labels.__getitem__ = MagicMock(
            side_effect=lambda key: {
                "instance_id": "projects/test-project/locations/us-central1/instances/test-redis",
                "region": "us-central1",
                "node_id": "node-0",
            }[key]
        )

        mock_result.metric.labels = MagicMock()
        mock_result.metric.labels.__getitem__ = MagicMock(
            side_effect=lambda key: {"cmd": "get", "role": "primary"}[key]
        )

        # Mock data points
        mock_point = MagicMock()
        mock_point.interval.start_time.timestamp.return_value = (
            1640995200.0  # 2022-01-01
        )
        mock_point.value.int64_value = 100
        mock_result.points = [mock_point]
        mock_result.value_type = 2  # INT64
        return mock_result

    @patch("msstats.monitoring_v3.MetricServiceClient")
    def test_process_google_project_with_mock_data(self, mock_client_class):
        """Test processing service account with mocked Google Cloud responses"""
        # Mock the monitoring client
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        # Mock time series response for commands/calls
        mock_result = self._mock_command_series()

        # Mock memory usage response
        mock_memory_result = MagicMock()
        mock_memory_result.resource.labels = MagicMock()
        mock_memory_result.resource.labels.__getitem__ = MagicMock(
            side_effect=lambda key: {
                "instance_id": "projects/test-project/locations/us-central1/instances/test-redis",
                "node_id": "node-0",
            }[key]
        )
        mock_memory_point = MagicMock()
        mock_memory_point.value.int64_value = 1000000  # 1MB
        mock_memory_result.points = [mock_memory_point]

        # Set up client responses
        mock_client.list_time_series.side_effect = [
            [mock_result],  # commands/calls response
            [mock_memory_result],  # memory/usage response
            [mock_memory_result],  # memory/maxmemory response
        ]

        # Test the function
        stats = process_google_project(
            self.test_project_id,
            duration=3600,  # 1 hour
            step=60,
        )

        # Assertions
        self.assertIsInstance(stats, dict)
        self.assertIn("test-redis", stats)

        # Check that the database stats contain expected structure
        database_stats = stats["test-redis"]
        self.assertIn("node-0", database_stats)

        node_stats = database_stats["node-0"]
        self.assertIn("commandstats", node_stats)
        self.assertIn("Source", node_stats)
        self.assertIn("ClusterId", node_stats)
        self.assertIn("NodeRole", node_stats)
        self.assertEqual(node_stats["NodeRole"], "Master")

    @patch("msstats.monitoring_v3.MetricServiceClient")
    def test_process_google_project_returns_empty_dict_on_query_error(
        self, mock_client_class
    ):
        """A failed commands query yields {} that create_workbooks can write"""
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_client.list_time_series.side_effect = Exception("boom")

        stats = process_google_project(self.test_project_id, duration=3600, step=60)

        self.assertEqual(stats, {})
        create_workbooks(self.temp_dir, {self.test_project_id: stats})
        os.remove(os.path.join(self.temp_dir, f"{self.test_project_id}.xlsx"))

    @patch("msstats.monitoring_v3.MetricServiceClient")
    def test_process_google_project_memory_error_ignores_command_results(
        self, mock_client_class
    ):
        """Failed memory queries must not reuse the commands query results"""
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_client.list_time_series.side_effect = [
            [self._mock_command_series()],  # commands/calls response
            Exception("boom"),  # memory/usage response
            Exception("boom"),  # memory/maxmemory response
        ]

        stats = process_google_project(self.test_project_id, duration=3600, step=60)

        node_stats = stats["test-redis"]["node-0"]
        self.assertNotIn("BytesUsedForCache", node_stats)
        self.assertNotIn("MaxMemory", node_stats)

    def test_create_workbooks_integration(self):
        """Test creating Excel workbooks from processed data"""
        # Create sample processed data
        projects_data = {
            "test-project": {
                "redis-instance-1": {
                    "node-0": {
                        "Source": "MS",
                        "ClusterId": "redis-instance-1",
                        "NodeId": "node-0",
                        "NodeRole": "Master",
                        "NodeType": "",
                        "Region": "us-central1",
                        "Project ID": "test-project",
                        "InstanceId": "projects/test-project/locations/us-central1/instances/redis-instance-1",
                        "BytesUsedForCache": 1000000,
                        "MaxMemory": 2000000,
                        "commandstats": {
                            "Throughput (Ops)": 500,
                            "GetTypeCmds": 300,
                            "SetTypeCmds": 200,
                            "OtherTypeCmds": 0,
                            "BitmapBasedCmds": 0,
                            "ClusterBasedCmds": 0,
                            "EvalBasedCmds": 0,
                            "GeoSpatialBasedCmds": 0,
                            "HashBasedCmds": 0,
                            "HyperLogLogBasedCmds": 0,
                            "KeyBasedCmds": 0,
                            "ListBasedCmds": 0,
                            "PubSubBasedCmds": 0,
                            "SetBasedCmds": 0,
                            "SortedSetBasedCmds": 0,
                            "StringBasedCmds": 0,
                            "StreamBasedCmds": 0,
                            "TransactionBasedCmds": 0,
                        },
                    }
                }
            }
        }

        # Create workbooks
        create_workbooks(self.temp_dir, projects_data)

        # Verify Excel file was created
        excel_file = os.path.join(self.temp_dir, "test-project.xlsx")
        self.assertTrue(os.path.exists(excel_file))

        # Verify Excel file contents
        wb = openpyxl.load_workbook(excel_file)
        ws = wb.active
        self.assertEqual(ws.title, "ClusterData")

        # Check that we have headers and data
        self.assertGreater(ws.max_row, 1)  # Should have header + at least 1 data row
        self.assertGreater(ws.max_column, 10)  # Should have many columns

        # Check some expected headers are present
        headers = [cell.value for cell in ws[1]]
        self.assertIn("Source", headers)
        self.assertIn("ClusterId", headers)
        self.assertIn("NodeRole", headers)
        self.assertIn("Throughput (Ops)", headers)

        # Clean up
        wb.close()
        os.remove(excel_file)

    @patch("msstats.monitoring_v3.MetricServiceClient")
    def test_service_account_file_parsing(self, mock_client_class):
        """Test that service account files are parsed correctly"""
        # Mock the monitoring client
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_client.list_time_series.return_value = []

        # It should read from file:
        project_id = get_project_from_service_account_and_authenticate(
            self.service_account_file
        )
        self.assertEqual(project_id, self.test_project_id)

    def test_invalid_service_account_file(self):
        """Test handling of invalid service account files"""
        # Create invalid JSON file
        invalid_file = os.path.join(self.temp_dir, "invalid.json")
        with open(invalid_file, "w") as f:
            f.write("invalid json content")

        # Should return None for invalid files
        result = get_project_from_service_account_and_authenticate(invalid_file)
        self.assertIsNone(result)

        # Clean up
        os.remove(invalid_file)

    def test_missing_service_account_file(self):
        """Test handling of missing service account files"""
        nonexistent_file = os.path.join(self.temp_dir, "nonexistent.json")

        # Should return None for missing files
        result = get_project_from_service_account_and_authenticate(nonexistent_file)
        self.assertIsNone(result)


class TestListTimeSeries(unittest.TestCase):
    """Test suite for the list_time_series splitting helper"""

    START = 1700000000
    SIZE_ERROR = "Maximum response size of 200000000 bytes reached."

    def _request(self, start, end):
        return {
            "name": "projects/test-project",
            "interval": monitoring_v3.TimeInterval(
                {"start_time": {"seconds": start}, "end_time": {"seconds": end}}
            ),
        }

    @staticmethod
    def _window(request):
        interval = request["interval"]
        return (
            int(interval.start_time.timestamp()),
            int(interval.end_time.timestamp()),
        )

    def test_returns_results_without_splitting(self):
        """A response within the limit is returned from a single request"""
        client = MagicMock()
        client.list_time_series.return_value = ["ts"]

        result = list_time_series(client, self._request(self.START, self.START + 3600))

        self.assertEqual(result, ["ts"])
        client.list_time_series.assert_called_once()

    def test_splits_on_step_boundaries_oldest_first(self):
        """Oversized windows are halved on step boundaries and merged in order"""

        def fake_list_time_series(request):
            start, end = self._window(request)
            if end - start > 1200:
                raise ResourceExhausted(self.SIZE_ERROR)
            return [(start - self.START, end - self.START)]

        client = MagicMock()
        client.list_time_series.side_effect = fake_list_time_series

        chunks = list_time_series(
            client, self._request(self.START, self.START + 3600), step=60
        )

        self.assertEqual(chunks, [(0, 900), (900, 1800), (1800, 2700), (2700, 3600)])

    def test_other_resource_exhausted_is_not_split(self):
        """Quota 429s are re-raised without retrying smaller windows"""
        client = MagicMock()
        client.list_time_series.side_effect = ResourceExhausted("Quota exceeded")

        with self.assertRaises(ResourceExhausted):
            list_time_series(client, self._request(self.START, self.START + 3600))
        client.list_time_series.assert_called_once()

    def test_gives_up_at_one_step(self):
        """A window that cannot be split further re-raises the size error"""
        client = MagicMock()
        client.list_time_series.side_effect = ResourceExhausted(self.SIZE_ERROR)

        with self.assertRaises(ResourceExhausted):
            list_time_series(
                client, self._request(self.START, self.START + 600), step=60
            )


class TestMemorystore(unittest.TestCase):
    """Test suite for memorystore.py functions"""

    def test_resolve_inst_key_redis_standalone_full_path(self):
        """Redis standalone returns full GCP path via instance_id — should be used as-is"""
        rlabels = {
            "instance_id": "projects/my-project/locations/us-central1/instances/my-redis",
            "node_id": "node-0",
        }
        result = _resolve_inst_key(rlabels, "my-project")
        self.assertEqual(
            result,
            "projects/my-project/locations/us-central1/instances/my-redis",
        )

    def test_resolve_inst_key_valkey_short_name_prefixed(self):
        """Valkey returns short name via instance_id — should be prefixed with project_id"""
        rlabels = {"instance_id": "memorystore-valkey", "node_id": "abc123"}
        result = _resolve_inst_key(rlabels, "my-project")
        self.assertEqual(result, "my-project/memorystore-valkey")

    def test_resolve_inst_key_redis_cluster_short_name_prefixed(self):
        """Redis Cluster returns short name via cluster_id — should be prefixed"""
        rlabels = {"cluster_id": "memorystore-redis-cluster", "shard_id": "xyz789"}
        result = _resolve_inst_key(rlabels, "my-project")
        self.assertEqual(result, "my-project/memorystore-redis-cluster")

    def test_resolve_inst_key_fallback_to_unknown(self):
        """When no identifiers are present, return 'unknown'"""
        rlabels = {"region": "us-central1"}
        result = _resolve_inst_key(rlabels, "my-project")
        self.assertEqual(result, "my-project/unknown")

    def test_resolve_inst_key_no_collision_across_projects(self):
        """Same short name in different projects produces different keys"""
        rlabels = {"instance_id": "memorystore-valkey"}
        key_a = _resolve_inst_key(rlabels, "project-a")
        key_b = _resolve_inst_key(rlabels, "project-b")
        self.assertNotEqual(key_a, key_b)
        self.assertEqual(key_a, "project-a/memorystore-valkey")
        self.assertEqual(key_b, "project-b/memorystore-valkey")

    def test_attach_memory_usage_uses_resolve_inst_key(self):
        """_attach_memory_usage should create entries with project-prefixed keys for short names"""
        table = {}
        mock_ts = MagicMock()
        mock_ts.resource.labels = {
            "instance_id": "memorystore-valkey",
            "node_id": "abc123",
        }
        mock_point = MagicMock()
        mock_point.value.int64_value = 5000000
        mock_point.value.double_value = 0
        mock_ts.points = [mock_point]

        _attach_memory_usage([mock_ts], table, project_id="my-project")

        self.assertIn("my-project/memorystore-valkey", table)
        self.assertNotIn("memorystore-valkey", table)

    def test_attach_capacity_scalar_uses_resolve_inst_key(self):
        """_attach_capacity_scalar should match entries using project-prefixed keys"""
        table = {"my-project/memorystore-valkey": {"abc123": {"MaxMemory": 0}}}
        mock_ts = MagicMock()
        mock_ts.resource.labels = {
            "instance_id": "memorystore-valkey",
            "node_id": "abc123",
        }
        mock_point = MagicMock()
        mock_point.value.int64_value = 10000000
        mock_point.value.double_value = 0
        mock_ts.points = [mock_point]

        _attach_capacity_scalar(
            [mock_ts], table, project_id="my-project", key_name="MaxMemory"
        )

        self.assertEqual(
            table["my-project/memorystore-valkey"]["abc123"]["MaxMemory"], 10000000
        )

    def test_normalize_location_zone_form(self):
        """GCP zone string is split into (region, zone)"""
        self.assertEqual(
            _normalize_location("us-central1-a"),
            ("us-central1", "us-central1-a"),
        )

    def test_normalize_location_region_form(self):
        """Region-shaped string is passed through as region, zone blank"""
        self.assertEqual(_normalize_location("us-east4"), ("us-east4", ""))

    def test_normalize_location_multi_segment_zone(self):
        """Multi-word region prefix (e.g. asia-northeast1) is handled"""
        self.assertEqual(
            _normalize_location("asia-northeast1-c"),
            ("asia-northeast1", "asia-northeast1-c"),
        )

    def test_normalize_location_empty_and_none(self):
        """Empty string and None return ('', '')"""
        self.assertEqual(_normalize_location(""), ("", ""))
        self.assertEqual(_normalize_location(None), ("", ""))

    def _make_cmd_ts(self, resource_labels, cmd="GET"):
        mock_ts = MagicMock()
        mock_ts.resource.labels = resource_labels
        mock_ts.metric.labels = {"cmd": cmd}
        mock_point = MagicMock()
        mock_point.interval.start_time.timestamp.return_value = 1000.0
        mock_point.value.int64_value = 1
        mock_point.value.double_value = 0
        mock_ts.points = [mock_point]
        return mock_ts

    def test_accumulate_commands_cluster_location_splits_into_region_and_zone(self):
        """Redis Cluster: location=<zone> must split into Region + Zone"""
        table = {}
        ts = self._make_cmd_ts(
            {"cluster_id": "c1", "shard_id": "s0", "location": "us-central1-a"}
        )
        _accumulate_commands([ts], table, "Redis Cluster", "proj")
        entry = table["proj/c1"]["s0"]
        self.assertEqual(entry["Region"], "us-central1")
        self.assertEqual(entry["Zone"], "us-central1-a")

    def test_accumulate_commands_standalone_keeps_region_and_explicit_zone(self):
        """Redis standalone: explicit region + zone labels survive untouched"""
        table = {}
        ts = self._make_cmd_ts(
            {
                "instance_id": "projects/proj/locations/us-central1/instances/r1",
                "node_id": "n0",
                "region": "us-central1",
                "zone": "us-central1-a",
            }
        )
        _accumulate_commands([ts], table, "Redis", "proj")
        entry = table["projects/proj/locations/us-central1/instances/r1"]["n0"]
        self.assertEqual(entry["Region"], "us-central1")
        self.assertEqual(entry["Zone"], "us-central1-a")

    def test_accumulate_commands_standalone_region_only_leaves_zone_blank(self):
        """Redis standalone: region-only label leaves Zone blank (regex no match)"""
        table = {}
        ts = self._make_cmd_ts(
            {
                "instance_id": "projects/proj/locations/us-east4/instances/r1",
                "node_id": "n0",
                "region": "us-east4",
            }
        )
        _accumulate_commands([ts], table, "Redis", "proj")
        entry = table["projects/proj/locations/us-east4/instances/r1"]["n0"]
        self.assertEqual(entry["Region"], "us-east4")
        self.assertEqual(entry["Zone"], "")

    REDIS_LABELS = {
        "instance_id": "projects/proj/locations/us-central1/instances/r1",
        "node_id": "n0",
    }

    def _client_failing(self, failing_metric, results_by_metric):
        """Mock client that raises for one metric and returns canned series otherwise"""

        def fake_list_time_series(request):
            if failing_metric in request["filter"]:
                raise Exception("boom")
            for metric, series in results_by_metric.items():
                if metric in request["filter"]:
                    return series
            return []

        client = MagicMock()
        client.list_time_series.side_effect = fake_list_time_series
        return client

    def test_collect_for_product_reports_commands_error_and_keeps_rows(self):
        """A failed commands query is reported and memory rows are still returned"""
        mem_ts = MagicMock()
        mem_ts.resource.labels = self.REDIS_LABELS
        mem_point = MagicMock()
        mem_point.value.int64_value = 5000000
        mem_ts.points = [mem_point]
        client = self._client_failing(
            REDIS_METRICS["commands"], {REDIS_METRICS["memory_usage"]: [mem_ts]}
        )
        errors = []

        rows = collect_for_product(
            client, "proj", 3600, 60, REDIS_METRICS, "Redis", errors
        )

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["BytesUsedForCache"], 5000000)
        self.assertEqual(errors, ["Error: could not query Redis command metrics: boom"])

    def test_collect_for_product_reports_memory_error_and_keeps_rows(self):
        """A failed memory query is reported and command rows are still returned"""
        client = self._client_failing(
            REDIS_METRICS["memory_usage"],
            {REDIS_METRICS["commands"]: [self._make_cmd_ts(self.REDIS_LABELS)]},
        )
        errors = []

        rows = collect_for_product(
            client, "proj", 3600, 60, REDIS_METRICS, "Redis", errors
        )

        self.assertEqual(len(rows), 1)
        self.assertEqual(errors, ["Error: could not query Redis memory usage: boom"])

    @patch("memorystore.monitoring_v3.MetricServiceClient")
    @patch("memorystore.service_account.Credentials.from_service_account_file")
    def test_main_writes_csv_and_returns_1_on_query_errors(
        self, mock_creds, mock_client_class
    ):
        """main still writes the CSV but returns 1 when any query failed"""
        mock_client_class.return_value.list_time_series.side_effect = Exception("boom")
        with tempfile.TemporaryDirectory() as out_dir:
            out = os.path.join(out_dir, "out.csv")
            argv = ["memorystore.py", "--project", "proj"]
            argv += ["--credentials", "sa.json", "--out", out]

            with patch("sys.argv", argv):
                exit_code = memorystore_main()

            self.assertEqual(exit_code, 1)
            self.assertTrue(os.path.exists(out))


if __name__ == "__main__":
    unittest.main()
