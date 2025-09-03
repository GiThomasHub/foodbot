# persistence.py
# Einheitliche Persistenz-API für Profile, Favoriten und Sessions.
# Backend umschaltbar per Env: PERSISTENCE=json (Default) oder firestore.

import os
import json
from datetime import datetime
from typing import Dict, Any, List, Optional

# ---------------------------------------------------------------------------
# Public API (nutzen wir später im Bot)
# ---------------------------------------------------------------------------

def user_key(tg_user_id: int) -> str:
    """Schlüssel für nutzerbezogene Daten (Profile, Favoriten)."""
    return f"u:{tg_user_id}"

def chat_key(tg_chat_id: int) -> str:
    """Schlüssel für chatbezogene Daten (Sessions/Listen)."""
    return f"c:{tg_chat_id}"

# ---- Profile
def get_profile(uid: str) -> Optional[Dict[str, Any]]:
    return _backend().get_profile(uid)

def set_profile(uid: str, data: Dict[str, Any]) -> None:
    d = dict(data)
    d.setdefault("created_at", _now_iso())
    d["updated_at"] = _now_iso()
    _backend().set_profile(uid, d)

def delete_profile(uid: str) -> None:
    _backend().delete_profile(uid)

# ---- Favoriten (Array von String-Namen)
def get_favorites(uid: str) -> List[str]:
    return _backend().get_favorites(uid)

def set_favorites(uid: str, items: List[str]) -> None:
    _backend().set_favorites(uid, _unique(items))

def add_favorite(uid: str, item: str) -> None:
    cur = get_favorites(uid)
    cur.append(item)
    set_favorites(uid, cur)

def remove_favorite(uid: str, item: str) -> None:
    cur = [x for x in get_favorites(uid) if x != item]
    set_favorites(uid, cur)

# ---- Sessions (aktueller Menü-/Planungszustand)
def get_session(cid: str) -> Optional[Dict[str, Any]]:
    return _backend().get_session(cid)

def set_session(cid: str, sess: Dict[str, Any]) -> None:
    d = dict(sess)
    d.setdefault("created_at", _now_iso())
    d["updated_at"] = _now_iso()
    _backend().set_session(cid, d)

def delete_session(cid: str) -> None:
    _backend().delete_session(cid)

# ---------------------------------------------------------------------------
# Backend Switch
# ---------------------------------------------------------------------------

def _backend():
    mode = (os.getenv("PERSISTENCE") or "json").strip().lower()
    if mode == "firestore":
        return _FirestoreBackend.instance()
    return _JsonBackend.instance()

def _unique(items: List[str]) -> List[str]:
    seen, out = set(), []
    for x in items:
        if x not in seen:
            out.append(x); seen.add(x)
    return out

def _now_iso() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

# ---------------------------------------------------------------------------
# JSON Backend (Status Quo, /tmp)
# ---------------------------------------------------------------------------

class _JsonBackend:
    _inst = None

    @classmethod
    def instance(cls):
        if not cls._inst:
            cls._inst = cls()
        return cls._inst

    def __init__(self):
        self.data_dir = os.getenv("DATA_DIR", "/tmp")
        os.makedirs(self.data_dir, exist_ok=True)
        self.files = {
            "profiles":  os.path.join(self.data_dir, "profiles.json"),
            "favorites": os.path.join(self.data_dir, "favorites.json"),
            "sessions":  os.path.join(self.data_dir, "sessions.json"),
        }

    # -- Helpers
    def _load(self, key: str) -> Dict[str, Any]:
        path = self.files[key]
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        return {}

    def _save(self, key: str, data: Dict[str, Any]) -> None:
        path = self.files[key]
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

    # -- Profile
    def get_profile(self, uid: str):
        return self._load("profiles").get(uid)

    def set_profile(self, uid: str, data: Dict[str, Any]):
        allp = self._load("profiles")
        allp[uid] = data
        self._save("profiles", allp)

    def delete_profile(self, uid: str):
        allp = self._load("profiles")
        if uid in allp:
            del allp[uid]
            self._save("profiles", allp)

    # -- Favoriten
    def get_favorites(self, uid: str) -> List[str]:
        return self._load("favorites").get(uid, [])

    def set_favorites(self, uid: str, items: List[str]) -> None:
        allf = self._load("favorites")
        allf[uid] = items
        self._save("favorites", allf)

    # -- Sessions
    def get_session(self, cid: str):
        return self._load("sessions").get(cid)

    def set_session(self, cid: str, sess: Dict[str, Any]):
        alls = self._load("sessions")
        alls[cid] = sess
        self._save("sessions", alls)

    def delete_session(self, cid: str):
        alls = self._load("sessions")
        if cid in alls:
            del alls[cid]
            self._save("sessions", alls)

# ---------------------------------------------------------------------------
# Firestore Backend (wird erst aktiv, wenn PERSISTENCE=firestore)
# ---------------------------------------------------------------------------

class _FirestoreBackend:
    _inst = None

    @classmethod
    def instance(cls):
        if not cls._inst:
            cls._inst = cls()
        return cls._inst

    def __init__(self):
        try:
            from google.cloud import firestore
        except Exception as e:
            raise RuntimeError(
                "google-cloud-firestore nicht installiert. "
                "Füge es in requirements/Dockerfile hinzu."
            ) from e
        self._fs = firestore.Client()
        self._col_profiles  = self._fs.collection("profiles")
        self._col_favorites = self._fs.collection("favorites")
        self._col_sessions  = self._fs.collection("sessions")

    # -- Profile
    def get_profile(self, uid: str):
        doc = self._col_profiles.document(uid).get()
        return doc.to_dict() if doc.exists else None

    def set_profile(self, uid: str, data: Dict[str, Any]):
        self._col_profiles.document(uid).set(data, merge=True)

    def delete_profile(self, uid: str):
        self._col_profiles.document(uid).delete()

    # -- Favoriten
    def get_favorites(self, uid: str) -> List[str]:
        doc = self._col_favorites.document(uid).get()
        d = doc.to_dict() if doc.exists else None
        return (d.get("items") if d else []) or []

    def set_favorites(self, uid: str, items: List[str]) -> None:
        self._col_favorites.document(uid).set(
            {"items": items, "updated_at": _now_iso()},
            merge=True
        )

    # -- Sessions (pro Chat)
    def get_session(self, cid: str):
        doc = self._col_sessions.document(cid).get()
        return doc.to_dict() if doc.exists else None

    def set_session(self, cid: str, sess: Dict[str, Any]):
        self._col_sessions.document(cid).set(sess, merge=True)

    def delete_session(self, cid: str):
        self._col_sessions.document(cid).delete()
