import importlib
import inspect
import os
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch


class OuiCestFaitSafetyTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmpdir = tempfile.TemporaryDirectory()
        os.environ["DB_PATH"] = str(Path(cls.tmpdir.name) / "test.db")
        os.environ["DEBUG"] = "0"
        os.environ.pop("PROV_MEAL_EUR", None)
        os.environ.pop("PROV_MARGIN_PCT", None)
        cls.main = importlib.import_module("main")

    @classmethod
    def tearDownClass(cls):
        cls.tmpdir.cleanup()

    def test_health(self):
        payload = self.main.health()
        self.assertEqual(payload["status"], "ok")
        self.assertEqual(payload["app"], "OuiCestFait")

    def test_final_province_defaults(self):
        self.assertEqual(self.main.PROV_EUR_PER_KM, Decimal("1.0"))
        self.assertEqual(self.main.PROV_TOLL_MULT, Decimal("2.0"))
        self.assertEqual(self.main.PROV_MEAL_EUR, Decimal("15"))
        self.assertEqual(self.main.PROV_MARGIN_PCT, Decimal("0.30"))
        price = self.main.price_province(100.0, 10.0, "", False)
        self.assertEqual(price, Decimal("175.50"))

    def test_anti_loop_window_and_roundtrip(self):
        self.assertGreater(self.main.ANTI_LOOP_WINDOW_SEC, 0)
        client = "33600000000"
        reply = "Réponse de test OuiCestFait"
        self.main.anti_loop_store_last_response(client, reply)
        self.assertTrue(self.main.anti_loop_ignore(client, reply))
        self.assertFalse(self.main.anti_loop_ignore(client, reply + " autre"))

    def test_log_redaction(self):
        checkout = "cs_" + "live_" + "abcdefghijklmnopqrstuvwxyz0123456789"
        webhook = "wh" + "sec_" + "abcdefghijklmnopqrstuvwxyz"
        stripe_key = "sk_" + "live_" + "abcdefghijklmnopqrstuvwxyz"
        data = {
            "authorization": "Bearer very-secret-token",
            "nested": {
                "api_key": "abc",
                "checkout": checkout,
                "webhook": webhook,
                "stripe": stripe_key,
            },
        }
        redacted = self.main._redact_for_log(data)
        self.assertEqual(redacted["authorization"], "[REDACTED]")
        self.assertEqual(redacted["nested"]["api_key"], "[REDACTED]")
        self.assertNotIn(webhook, str(redacted))
        self.assertNotIn(stripe_key, str(redacted))
        self.assertNotIn(checkout, str(redacted))

    def test_longer_of_mormant_or_paris_is_used(self):
        distances = {
            (self.main.BASE_LOCATION, "Pickup"): 20.0,
            (self.main.SECONDARY_BASE_LOCATION, "Pickup"): 50.0,
            ("Pickup", "Drop"): 30.0,
        }

        def fake_distance(a, b):
            return distances.get((a, b))

        with patch.object(self.main, "compute_distance_km", side_effect=fake_distance):
            total, approach, delivery = self.main.compute_trip_distance_km("Pickup", "Drop")
        self.assertEqual(approach, 50.0)
        self.assertEqual(delivery, 30.0)
        self.assertEqual(total, 80.0)

    def test_success_response_stores_anti_loop_before_final_return(self):
        source = inspect.getsource(self.main.mission_demande)
        pos_store = source.rfind("anti_loop_store_last_response(client_id, resume_out)")
        pos_return = source.rfind("return MissionOut(")
        self.assertGreater(pos_store, 0)
        self.assertLess(pos_store, pos_return)


if __name__ == "__main__":
    unittest.main()
