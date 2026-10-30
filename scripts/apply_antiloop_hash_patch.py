from pathlib import Path

PATH = Path("main.py")
text = PATH.read_text(encoding="utf-8")

if "def _sha1(" in text:
    print("_sha1 helper already present")
    raise SystemExit(0)

marker = "def anti_loop_ignore(client_id: str, incoming_text: str) -> bool:\n"
if text.count(marker) != 1:
    raise SystemExit(f"anti-loop marker: expected 1 occurrence, found {text.count(marker)}")

helper = '''def _sha1(value: str) -> str:
    """Stable SHA-1 fingerprint used only for anti-loop comparison, not for passwords."""
    return hashlib.sha1((value or "").encode("utf-8", errors="ignore")).hexdigest()


'''

text = text.replace(marker, helper + marker, 1)
PATH.write_text(text, encoding="utf-8")
print("_sha1 helper patched")
