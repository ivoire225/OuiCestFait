from pathlib import Path
import re

PATH = Path("main.py")
text = PATH.read_text(encoding="utf-8")
original = text


def replace_once(old: str, new: str, label: str) -> None:
    global text
    if new in text:
        return
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"{label}: expected exactly 1 source occurrence, found {count}")
    text = text.replace(old, new, 1)


# 1) Final validated province pricing: 1 €/km + 15 € meal + 2x estimated toll, then +30% margin.
replace_once(
    '# base = (1€ * km) + (2 * péage) + 30€ repas ; total = base * (1 + 40%)',
    '# base = (1€ * km) + (2 * péage) + 15€ repas ; total = base * (1 + 30%)',
    'province pricing comment',
)
replace_once(
    'PROV_MEAL_EUR = Decimal(os.getenv("PROV_MEAL_EUR", "30"))',
    'PROV_MEAL_EUR = Decimal(os.getenv("PROV_MEAL_EUR", "15"))',
    'province meal',
)
replace_once(
    'PROV_MARGIN_PCT = Decimal(os.getenv("PROV_MARGIN_PCT", "0.40"))  # +40%',
    'PROV_MARGIN_PCT = Decimal(os.getenv("PROV_MARGIN_PCT", "0.30"))  # +30%',
    'province margin',
)

# 2) Define the anti-loop window that anti_loop_ignore already uses.
replace_once(
    'SOFT_DEDUP_WINDOW_SEC = int(os.getenv("SOFT_DEDUP_WINDOW_SEC", "12"))',
    'SOFT_DEDUP_WINDOW_SEC = int(os.getenv("SOFT_DEDUP_WINDOW_SEC", "12"))\nANTI_LOOP_WINDOW_SEC = int(os.getenv("ANTI_LOOP_WINDOW_SEC", "120"))',
    'anti-loop window',
)

# 3) Validated pricing rule: calculate from Mormant or Paris and retain the longer base approach.
replace_once(
    'BASE_LOCATION = f"{BASE_CITY} {BASE_POSTAL}"',
    'BASE_LOCATION = f"{BASE_CITY} {BASE_POSTAL}"\nSECONDARY_BASE_LOCATION = os.getenv("SECONDARY_BASE_LOCATION", "Paris 75000").strip()',
    'secondary base',
)

new_trip_function = '''def compute_trip_distance_km(origin: str, destination: str) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    """Distance totale de tarification avec double base validée.

    Pour chaque mission, on compare l'approche vers le point de prise en charge depuis :
    - Mormant 77720 (BASE_LOCATION)
    - Paris 75000 (SECONDARY_BASE_LOCATION)

    On retient l'approche la plus longue, puis on ajoute origin -> destination.
    Retourne (km_total, km_base_vers_origin_retenu, km_origin_vers_destination).
    """
    o = (origin or BASE_LOCATION).strip()
    d = (destination or BASE_LOCATION).strip()

    leg_delivery = compute_distance_km(o, d)
    if leg_delivery is None:
        return None, None, None

    base_candidates: List[float] = []
    for base in (BASE_LOCATION, SECONDARY_BASE_LOCATION):
        base = (base or "").strip()
        if not base:
            continue
        if _canon_addr(base) == _canon_addr(o):
            base_candidates.append(0.0)
            continue
        leg = compute_distance_km(base, o)
        if leg is not None:
            base_candidates.append(float(leg))

    if not base_candidates:
        return None, None, float(leg_delivery)

    leg_to_pickup = max(base_candidates)
    km_total = float(leg_to_pickup) + float(leg_delivery)
    return km_total, float(leg_to_pickup), float(leg_delivery)
'''
if 'double base validée' not in text:
    pattern = re.compile(
        r'def compute_trip_distance_km\(origin: str, destination: str\).*?(?=\n\n# =========================\n# Péage estimé)',
        re.S,
    )
    matches = list(pattern.finditer(text))
    if len(matches) != 1:
        raise SystemExit(f'double-base function: expected 1 function, found {len(matches)}')
    text = text[:matches[0].start()] + new_trip_function.rstrip() + text[matches[0].end():]

# 4) Redact secrets/session identifiers before storing debug logs.
if 'def _redact_for_log(' not in text:
    marker = 'def _log(kind: str, payload: Dict[str, Any]) -> None:\n'
    if text.count(marker) != 1:
        raise SystemExit('log redaction: _log marker not unique')
    redaction = '''_SENSITIVE_LOG_KEYS = {
    "authorization", "token", "api_key", "apikey", "secret", "password",
    "stripe_signature", "stripe-signature", "x-ocf-token", "debug_token",
    "openai_api_key", "webhook_secret",
}


def _mask_sensitive_string(value: str) -> str:
    s = str(value)
    s = re.sub(r"\\bsk_(?:live|test)_[A-Za-z0-9_\\-]+\\b", "[REDACTED_STRIPE_KEY]", s)
    s = re.sub(r"\\bwhsec_[A-Za-z0-9_\\-]+\\b", "[REDACTED_WEBHOOK_SECRET]", s)
    s = re.sub(
        r"\\b(cs_(?:live|test)_[A-Za-z0-9_\\-]+)\\b",
        lambda m: m.group(1)[:12] + "..." + m.group(1)[-6:],
        s,
    )
    s = re.sub(r"(?i)bearer\\s+[A-Za-z0-9._~+\\-/=]+", "Bearer [REDACTED]", s)
    return s


def _redact_for_log(value: Any, key: str = "") -> Any:
    if key.lower() in _SENSITIVE_LOG_KEYS:
        return "[REDACTED]"
    if isinstance(value, dict):
        return {str(k): _redact_for_log(v, str(k)) for k, v in value.items()}
    if isinstance(value, list):
        return [_redact_for_log(v) for v in value]
    if isinstance(value, tuple):
        return tuple(_redact_for_log(v) for v in value)
    if isinstance(value, str):
        return _mask_sensitive_string(value)
    return value


'''
    text = text.replace(marker, redaction + marker, 1)

replace_once(
    '(kind, json.dumps(payload, ensure_ascii=False), utc_now_iso()),',
    '(kind, json.dumps(_redact_for_log(payload), ensure_ascii=False), utc_now_iso()),',
    'log payload redaction',
)

# 5) Store successful responses too; previous line was unreachable after a return.
replace_once(
    '\n        anti_loop_store_last_response(client_id, resume_out)\n\n    return MissionOut(\n',
    '\n    anti_loop_store_last_response(client_id, resume_out)\n\n    return MissionOut(\n',
    'successful-response anti-loop',
)

if text != original:
    PATH.write_text(text, encoding="utf-8")
    print("main.py patched")
else:
    print("main.py already compliant")
