import random
import unittest
from datetime import datetime, timedelta

from src.event_producer import EVENT_TYPES, PRODUCT_IDS, REGIONS, build_event


class BuildEventTests(unittest.TestCase):
    def test_event_has_expected_fields_and_values(self) -> None:
        event = build_event(random.Random(7))

        self.assertEqual(
            set(event),
            {
                "event_id",
                "event_time",
                "user_id",
                "event_type",
                "product_id",
                "amount",
                "region",
            },
        )
        self.assertTrue(event["event_id"])
        timestamp = datetime.fromisoformat(event["event_time"].replace("Z", "+00:00"))
        self.assertEqual(timestamp.utcoffset(), timedelta(0))
        self.assertIn(event["event_type"], EVENT_TYPES)
        self.assertIn(event["product_id"], PRODUCT_IDS)
        self.assertIn(event["region"], REGIONS)
        self.assertGreaterEqual(event["amount"], 0)
        if event["event_type"] != "purchase":
            self.assertEqual(event["amount"], 0.0)

    def test_seed_makes_random_fields_repeatable(self) -> None:
        first = build_event(random.Random(23))
        second = build_event(random.Random(23))

        self.assertEqual(
            (
                first["user_id"],
                first["event_type"],
                first["product_id"],
                first["amount"],
                first["region"],
            ),
            (
                second["user_id"],
                second["event_type"],
                second["product_id"],
                second["amount"],
                second["region"],
            ),
        )


if __name__ == "__main__":
    unittest.main()
