import unittest

from scripts.updatePrice import collect_price_entries


class CollectPriceEntriesTest(unittest.TestCase):
    def test_duplicate_model_id_uses_later_provider(self):
        raw_price = {
            "first": {
                "models": {
                    "first-key": {
                        "id": "GLM-5.2",
                        "cost": {"input": 1.4, "output": 4.4, "cache_read": 0.28},
                    }
                }
            },
            "second": {
                "models": {
                    "second-key": {
                        "id": "glm-5.2",
                        "cost": {"input": 1.4, "output": 4.4, "cache_read": 0.26},
                    }
                }
            },
        }

        entries, provider_counts, conflicts = collect_price_entries(
            raw_price,
            ["first", "second"],
        )

        self.assertEqual(["glm-5.2"], list(entries))
        self.assertEqual(0.26, entries["glm-5.2"]["cache_read"])
        self.assertEqual({"first": 1, "second": 1}, provider_counts)
        self.assertEqual(
            [("glm-5.2", "first:glm-5.2", "second:glm-5.2")],
            conflicts,
        )

    def test_aliases_are_unique_within_provider(self):
        raw_price = {
            "anthropic": {
                "models": {
                    "claude": {
                        "id": "claude-opus-4-5",
                        "cost": {"input": 5, "output": 25},
                    }
                }
            }
        }

        entries, provider_counts, conflicts = collect_price_entries(
            raw_price,
            ["anthropic"],
        )

        self.assertEqual(4, len(entries))
        self.assertEqual({"anthropic": 4}, provider_counts)
        self.assertEqual([], conflicts)


if __name__ == "__main__":
    unittest.main()
