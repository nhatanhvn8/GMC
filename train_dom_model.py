# -*- coding: utf-8 -*-
"""
train_dom_model.py — Train model local từ DOM features (không phụ thuộc ngôn ngữ).

Input:  training_data/dom_samples.jsonl  (thu thập bởi data_collector.py)
        config/dom_patterns.json          (cache AI học được, dùng luôn làm seed data)
Output: config/dom_model.pkl             (sklearn model, ~100KB)
        config/dom_model_meta.json        (labels, feature names, accuracy)

Không cần PyTorch/GPU. Chạy trong vài giây.
Chạy: python train_dom_model.py
"""
from __future__ import annotations

import json
import logging
import pickle
from collections import Counter
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
_log = logging.getLogger(__name__)

TOOL_DIR = Path(__file__).resolve().parent
SAMPLES_FILE = TOOL_DIR / "training_data" / "dom_samples.jsonl"
CACHE_FILE   = TOOL_DIR / "config" / "dom_patterns.json"
MODEL_FILE   = TOOL_DIR / "config" / "dom_model.pkl"
META_FILE    = TOOL_DIR / "config" / "dom_model_meta.json"

MIN_CONFIDENCE = 0.75
MIN_SAMPLES_PER_CLASS = 3  # ít nhất 3 mẫu/class thì mới train


# ── Feature extraction ───────────────────────────────────────────────────────

INPUT_NAMES = [
    "identifier", "Passwd", "password", "totpPin", "knowledgeLoginValueInput",
    "phoneNumberId", "answer", "recoveryIdentifier", "email",
]
INPUT_TYPES = ["text", "password", "tel", "email", "number"]
AUTOCOMPLETES = ["username", "current-password", "one-time-code", "email", "tel"]
URL_KEYWORDS = [
    "/signin/v2/identifier", "/signin/v2/challenge/pwd",
    "/challenge/totp", "/challenge/iknow", "/challenge/ipp",
    "/challenge/iap", "/challenge/dp", "/challenge/pk",
    "/v3/signin", "/ServiceLogin", "/signinoptions",
    "myaccount.google.com", "mail.google.com",
    "accounts.google.com",
    "/disabled", "/suspended", "/deactivated",
]

def extract_features(dom: dict) -> list[float]:
    """Chuyển DOM snapshot thành feature vector số (language-independent)."""
    feats: list[float] = []

    inputs = dom.get("inputs") or []
    inp_names  = {i.get("name", "") for i in inputs}
    inp_types  = {i.get("type", "") for i in inputs}
    inp_autos  = {i.get("autocomplete", "") for i in inputs}
    inp_maxlen = {int(i.get("maxlength") or 0) for i in inputs}
    inp_ids    = {i.get("id", "") for i in inputs}

    # Input name presence (most reliable, language-independent)
    for name in INPUT_NAMES:
        feats.append(1.0 if name in inp_names else 0.0)

    # Input type presence
    for t in INPUT_TYPES:
        feats.append(1.0 if t in inp_types else 0.0)

    # Autocomplete presence
    for ac in AUTOCOMPLETES:
        feats.append(1.0 if ac in inp_autos else 0.0)

    # TOTP-specific signals
    feats.append(1.0 if "totpPin" in inp_names or "totpPin" in inp_ids else 0.0)
    feats.append(1.0 if 6 in inp_maxlen else 0.0)   # TOTP maxlength=6
    feats.append(1.0 if "one-time-code" in inp_autos else 0.0)

    # Error signals (language-independent)
    feats.append(1.0 if dom.get("has_error") else 0.0)
    feats.append(1.0 if dom.get("pass_input_visible") else 0.0)
    feats.append(1.0 if dom.get("totp_input_visible") else 0.0)
    feats.append(1.0 if dom.get("tel_input_visible") else 0.0)

    # challenge_type (Google's internal challenge identifier)
    ct = str(dom.get("challenge_type") or "")
    for ct_val in ["2", "6", "12", "8", "9", "39", "56"]:
        feats.append(1.0 if ct == ct_val else 0.0)

    # URL keyword presence
    url = (dom.get("url") or "").lower()
    for kw in URL_KEYWORDS:
        feats.append(1.0 if kw in url else 0.0)

    # Number of visible inputs
    n_inputs = len(inputs)
    feats.append(min(n_inputs / 5.0, 1.0))   # normalize 0-1

    # Form action hints
    form_action = (dom.get("form_action") or "").lower()
    for kw in ["/signin", "/challenge", "/lookup", "/webupdates"]:
        feats.append(1.0 if kw in form_action else 0.0)

    # Error text snippets (language-independent keywords)
    err = (dom.get("error_text") or "").lower()
    red = (dom.get("red_text") or "").lower()
    combined_err = err + " " + red
    for kw in ["changed", "wrong", "incorrect", "invalid", "attempts", "suspended",
               "disabled", "blocked", "unusual", "suspicious", "verify", "match"]:
        feats.append(1.0 if kw in combined_err else 0.0)

    return feats


def get_feature_names() -> list[str]:
    names = []
    for n in INPUT_NAMES:    names.append(f"inp_name_{n}")
    for t in INPUT_TYPES:    names.append(f"inp_type_{t}")
    for a in AUTOCOMPLETES:  names.append(f"autocomplete_{a}")
    names += ["is_totpPin", "maxlen_6", "is_otp_autocomplete"]
    names += ["has_error", "pass_visible", "totp_visible", "tel_visible"]
    for v in ["2", "6", "12", "8", "9", "39", "56"]:
        names.append(f"challenge_type_{v}")
    for kw in URL_KEYWORDS:  names.append(f"url_{kw.strip('/')}")
    names.append("n_inputs_norm")
    for kw in ["/signin", "/challenge", "/lookup", "/webupdates"]:
        names.append(f"form_{kw.strip('/')}")
    for kw in ["changed", "wrong", "incorrect", "invalid", "attempts", "suspended",
               "disabled", "blocked", "unusual", "suspicious", "verify", "match"]:
        names.append(f"err_{kw}")
    return names


# ── Data loading ──────────────────────────────────────────────────────────────

def load_samples() -> list[tuple[list[float], str]]:
    """Load samples từ dom_samples.jsonl + dom_patterns.json (cache AI)."""
    samples = []
    seen = set()

    def _add(dom, label, confidence, source):
        if not dom or not label:
            return
        if confidence < MIN_CONFIDENCE:
            return
        # Dedup by feature fingerprint
        key = f"{label}|{str(extract_features(dom))}"
        if key in seen:
            return
        seen.add(key)
        feats = extract_features(dom)
        samples.append((feats, label, source))

    # 1. Load từ dom_samples.jsonl (thu thập trong lúc chạy)
    if SAMPLES_FILE.exists():
        with open(SAMPLES_FILE, encoding="utf-8") as f:
            for line in f:
                try:
                    rec = json.loads(line)
                    _add(rec.get("dom"), rec.get("label"), rec.get("confidence", 1.0), "collector")
                except Exception:
                    pass
        _log.info("Loaded từ dom_samples.jsonl")

    # 2. Load từ dom_patterns.json (cache AI đã học — seed data miễn phí)
    if CACHE_FILE.exists():
        with open(CACHE_FILE, encoding="utf-8") as f:
            cache = json.load(f)
        for key, val in cache.items():
            dom  = val.get("dom") or {}
            label = val.get("page_type") or val.get("label") or ""
            conf = float(val.get("confidence") or 0.8)
            _add(dom, label, conf, "ai_cache")
        _log.info("Loaded từ dom_patterns.json (AI cache)")

    return samples


# ── Train ─────────────────────────────────────────────────────────────────────

def train():
    try:
        from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
        from sklearn.linear_model import LogisticRegression
        from sklearn.model_selection import cross_val_score, train_test_split
        from sklearn.preprocessing import LabelEncoder
        from sklearn.pipeline import Pipeline
        from sklearn.preprocessing import StandardScaler
        import numpy as np
    except ImportError:
        _log.error("Thiếu scikit-learn. Chạy: pip install scikit-learn numpy")
        return False

    _log.info("=== TRAIN DOM MODEL ===")
    samples = load_samples()

    if not samples:
        _log.error("Không có data! Chạy tool trước để thu thập mẫu, hoặc có AI cache.")
        return False

    # Count per label
    label_counts = Counter(s[1] for s in samples)
    _log.info("Phân bố data:")
    for lbl, cnt in sorted(label_counts.items(), key=lambda x: -x[1]):
        _log.info("  %-35s %d", lbl, cnt)

    # Lọc class có đủ mẫu
    valid_labels = {lbl for lbl, cnt in label_counts.items() if cnt >= MIN_SAMPLES_PER_CLASS}
    samples = [(f, l, s) for f, l, s in samples if l in valid_labels]
    _log.info("Classes đủ data (%d+ mẫu): %d/%d", MIN_SAMPLES_PER_CLASS, len(valid_labels), len(label_counts))

    if len(samples) < 10:
        _log.error("Cần ít nhất 10 mẫu để train. Hiện có: %d", len(samples))
        return False

    X = np.array([s[0] for s in samples])
    y_raw = [s[1] for s in samples]

    le = LabelEncoder()
    y = le.fit_transform(y_raw)
    labels = list(le.classes_)

    _log.info("Total samples: %d | Classes: %d | Features: %d", len(X), len(labels), X.shape[1])

    # Train/test split
    if len(samples) >= 20:
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)
    else:
        X_train, X_test, y_train, y_test = X, X, y, y

    # Thử 2 model, chọn cái tốt hơn
    models_to_try = [
        ("RandomForest", RandomForestClassifier(n_estimators=100, random_state=42, class_weight="balanced")),
        ("GradientBoosting", GradientBoostingClassifier(n_estimators=100, random_state=42)),
    ]

    best_model = None
    best_acc = 0.0
    best_name = ""

    for name, clf in models_to_try:
        clf.fit(X_train, y_train)
        acc = clf.score(X_test, y_test)
        _log.info("  %s accuracy: %.1f%%", name, acc * 100)
        if acc > best_acc:
            best_acc = acc
            best_model = clf
            best_name = name

    _log.info("Best model: %s (%.1f%%)", best_name, best_acc * 100)

    # Cross-validation nếu đủ data
    if len(samples) >= 30:
        cv_scores = cross_val_score(best_model, X, y, cv=min(5, len(valid_labels)), scoring="accuracy")
        _log.info("Cross-val: %.1f%% ± %.1f%%", cv_scores.mean() * 100, cv_scores.std() * 100)

    # Feature importance
    if hasattr(best_model, "feature_importances_"):
        feat_names = get_feature_names()
        top_feats = sorted(
            zip(feat_names, best_model.feature_importances_),
            key=lambda x: -x[1]
        )[:10]
        _log.info("Top 10 features quan trọng nhất:")
        for fname, imp in top_feats:
            _log.info("  %-40s %.3f", fname, imp)

    # Save model
    MODEL_FILE.parent.mkdir(exist_ok=True)
    with open(MODEL_FILE, "wb") as f:
        pickle.dump({"model": best_model, "label_encoder": le}, f)

    meta = {
        "model_type": best_name,
        "accuracy": round(best_acc, 4),
        "n_samples": len(samples),
        "n_classes": len(labels),
        "labels": labels,
        "n_features": int(X.shape[1]),
        "feature_names": get_feature_names(),
        "label_counts": dict(label_counts),
    }
    META_FILE.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")

    _log.info("✅ Model saved → %s", MODEL_FILE)
    _log.info("✅ Meta  saved → %s", META_FILE)
    return True


if __name__ == "__main__":
    import sys
    if "--stats" in sys.argv:
        # Chỉ xem thống kê data
        from data_collector import print_stats
        print_stats()
    else:
        ok = train()
        if ok:
            print("\n✅ Train xong! Chạy tool để dùng model local.")
        else:
            print("\n❌ Train thất bại. Xem log ở trên.")
