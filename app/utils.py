import html, json, uuid
from datetime import datetime
from typing import Any, Dict, List
import re
import unicodedata

def e(s: Any) -> str:
    return "—" if s is None else html.escape(str(s), quote=False)

def now_str() -> str:
    return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

def chunks(text: str, size: int = 3800) -> List[str]:
    if len(text) <= size:
        return [text]
    out, cur = [], 0
    while cur < len(text):
        out.append(text[cur:cur+size]); cur += size
    return out

def slugify(text: str, fallback: str = "site", maxlen: int = 80) -> str:
    if not text:
        return fallback
    s = unicodedata.normalize("NFKD", text)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = re.sub(r"[^a-zA-Z0-9]+", "-", s.lower()).strip("-")
    return (s or fallback)[:maxlen]
# ---------- парсеры ----------
def _norm(line: str) -> str:
    return line.replace("|","—").replace(" - "," — ").replace("-", "—")

def parse_services(text: str) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for line in (text or "").splitlines():
        parts = [p.strip() for p in _norm(line).split("—") if p.strip()]
        if not parts: continue
        item: Dict[str, Any] = {"name": parts[0]}
        if len(parts)>1: item["summary"]=parts[1]
        if len(parts)>2: item["priceFrom"]=parts[2]
        out.append(item)
    return out

def parse_portfolio(text: str) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    for line in (text or "").splitlines():
        parts = [p.strip() for p in _norm(line).split("—") if p.strip()]
        if not parts: continue
        obj: Dict[str, Any] = {"title": parts[0]}
        if len(parts)>1: obj["client"]=parts[1]
        if len(parts)>2:
            try: obj["year"]=int(parts[2])
            except: obj["year"]=parts[2]
        if len(parts)>3: obj["summary"]=parts[3]
        if len(parts)>4:
            tags = [t.strip() for t in parts[4].split(";") if t.strip()]
            if tags: obj["tags"]=tags
        if len(parts)>5: obj["link"]=parts[5]
        items.append(obj)
    return items

def parse_testimonials(text: str) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    for line in (text or "").splitlines():
        parts = [p.strip() for p in _norm(line).split("—") if p.strip()]
        if not parts: continue
        obj: Dict[str, Any] = {"name": parts[0]}
        if len(parts)>1: obj["company"]=parts[1]
        if len(parts)>2: obj["quote"]=parts[2]
        if len(parts)>3:
            try: obj["rating"]=min(5, max(1, int(parts[3])))
            except: pass
        items.append(obj)
    return items

def parse_faq(text: str) -> List[Dict[str, str]]:
    items: List[Dict[str, str]] = []
    for line in (text or "").splitlines():
        parts = [p.strip() for p in _norm(line).split("—") if p.strip()]
        if not parts: continue
        q = parts[0]; a = parts[1] if len(parts)>1 else ""
        items.append({"q": q, "a": a})
    return items

def default_seo_title(company: str, business: str) -> str:
    base = (company or "").strip()
    offer = (business or "").strip()
    if base and offer:
        return f"{base} — {offer}"
    return base or offer or "Компания"

def merge_site(existing: Dict[str, Any], patch: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(existing or {})
    for k, v in patch.items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = merge_site(out[k], v)
        else:
            out[k] = v
    return out

# ---------- payload/expor helpers ----------
def convert_uuids_to_strings(obj):
    if isinstance(obj, dict):
        return {key: convert_uuids_to_strings(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [convert_uuids_to_strings(item) for item in obj]
    elif isinstance(obj, uuid.UUID):
        return str(obj)
    else:
        return obj

def build_request_payload(rec: dict) -> dict:
    data_json = json.loads(rec.get("site_params_json") or "{}")
    return {
        "request_id": str(rec["id"]) if rec.get("id") else None,
        "manager_id": str(rec.get("manager_id")) if rec.get("manager_id") else None,
        "client": data_json.get("client", {}),
        "site": data_json.get("site", {}),
    }
