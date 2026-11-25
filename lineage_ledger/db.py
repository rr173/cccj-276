"""SQLite persistence and schema initialization."""
from __future__ import annotations

import sqlite3
from pathlib import Path

SCHEMA = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS schema_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS unit_conversion (
    unit TEXT PRIMARY KEY,
    factor_num TEXT NOT NULL,
    factor_den TEXT NOT NULL,
    note TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS event_head (
    event_id TEXT PRIMARY KEY,
    current_rev INTEGER NOT NULL,
    seq INTEGER NOT NULL UNIQUE,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS event_version (
    event_id TEXT NOT NULL,
    rev INTEGER NOT NULL,
    seq INTEGER NOT NULL,
    content_hash TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    raw_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (event_id, rev)
);

CREATE INDEX IF NOT EXISTS idx_event_version_seq ON event_version(seq);
CREATE INDEX IF NOT EXISTS idx_event_version_hash ON event_version(event_id, rev, content_hash);

-- Sample references in every submitted revision, including superseded ones.
CREATE TABLE IF NOT EXISTS event_sample_ref (
    event_id TEXT NOT NULL,
    rev INTEGER NOT NULL,
    seq INTEGER NOT NULL,
    sample_id TEXT NOT NULL,
    role TEXT NOT NULL,
    PRIMARY KEY (event_id, rev, sample_id, role)
);
CREATE INDEX IF NOT EXISTS idx_esr_sample ON event_sample_ref(sample_id);
CREATE INDEX IF NOT EXISTS idx_esr_event_rev ON event_sample_ref(event_id, rev);

CREATE TABLE IF NOT EXISTS event_dependency (
    event_id TEXT NOT NULL,
    rev INTEGER NOT NULL,
    depends_on_event_id TEXT NOT NULL,
    dep_seq INTEGER NOT NULL,
    PRIMARY KEY (event_id, rev, depends_on_event_id)
);
CREATE INDEX IF NOT EXISTS idx_edep_on ON event_dependency(depends_on_event_id);

-- Permanent identity claims, including invalid/superseded derived outputs.
CREATE TABLE IF NOT EXISTS sample_identity_claim (
    sample_id TEXT NOT NULL,
    event_id TEXT NOT NULL,
    rev INTEGER NOT NULL,
    seq INTEGER NOT NULL,
    claim_kind TEXT NOT NULL CHECK (claim_kind IN ('original', 'derived_output')),
    PRIMARY KEY (sample_id, event_id, rev, claim_kind)
);
CREATE INDEX IF NOT EXISTS idx_sic_sample_kind ON sample_identity_claim(sample_id, claim_kind);

-- Current derivation status for the current head revision only.
CREATE TABLE IF NOT EXISTS event_derivation (
    event_id TEXT PRIMARY KEY,
    rev INTEGER NOT NULL,
    seq INTEGER NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('valid', 'blocked')),
    reason_code TEXT,
    reason_json TEXT,
    derived_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_derivation_status ON event_derivation(status);

-- Signed exact-rational balance deltas produced by valid current events.
CREATE TABLE IF NOT EXISTS effect (
    event_id TEXT NOT NULL,
    rev INTEGER NOT NULL,
    seq INTEGER NOT NULL,
    sample_id TEXT NOT NULL,
    delta_num TEXT NOT NULL,
    delta_den TEXT NOT NULL,
    PRIMARY KEY (event_id, sample_id)
);
CREATE INDEX IF NOT EXISTS idx_effect_sample_seq ON effect(sample_id, seq);
CREATE INDEX IF NOT EXISTS idx_effect_seq ON effect(seq);

CREATE TABLE IF NOT EXISTS effect_source (
    event_id TEXT NOT NULL,
    sample_id TEXT NOT NULL,
    original_id TEXT NOT NULL,
    delta_num TEXT NOT NULL,
    delta_den TEXT NOT NULL,
    PRIMARY KEY (event_id, sample_id, original_id)
);
CREATE INDEX IF NOT EXISTS idx_effect_source_sample ON effect_source(sample_id);
CREATE INDEX IF NOT EXISTS idx_effect_source_original ON effect_source(original_id);

-- Current interpreted dependency edges, used for bounded evidence traversal.
CREATE TABLE IF NOT EXISTS event_upstream (
    event_id TEXT NOT NULL,
    upstream_event_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    PRIMARY KEY (event_id, upstream_event_id, kind)
);
CREATE INDEX IF NOT EXISTS idx_upstream_up ON event_upstream(upstream_event_id);

-- Source quarantine / release records. Every revision is retained; queries
-- join event_head so only the current revision of each record is interpreted.
CREATE TABLE IF NOT EXISTS quarantine_record (
    event_id TEXT NOT NULL,
    rev INTEGER NOT NULL,
    seq INTEGER NOT NULL,
    source_id TEXT NOT NULL,
    action TEXT NOT NULL CHECK (action IN ('quarantine', 'release')),
    reason TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (event_id, rev)
);
CREATE INDEX IF NOT EXISTS idx_quarantine_source_seq
    ON quarantine_record(source_id, seq);
CREATE INDEX IF NOT EXISTS idx_quarantine_seq ON quarantine_record(seq);

CREATE TABLE IF NOT EXISTS citation (
    citation_id TEXT PRIMARY KEY,
    sample_id TEXT NOT NULL,
    at_seq INTEGER NOT NULL,
    claimed_num TEXT,
    claimed_den TEXT,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    current_version INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_citation_sample ON citation(sample_id);
CREATE INDEX IF NOT EXISTS idx_citation_at ON citation(at_seq);

CREATE TABLE IF NOT EXISTS citation_explanation (
    citation_id TEXT NOT NULL,
    version INTEGER NOT NULL,
    trigger_event_id TEXT,
    trigger_rev INTEGER,
    status TEXT NOT NULL CHECK (status IN ('traceable', 'pending')),
    traceable INTEGER NOT NULL CHECK (traceable IN (0, 1)),
    as_of_seq INTEGER NOT NULL,
    available_num TEXT,
    available_den TEXT,
    reason_code TEXT,
    report_json TEXT NOT NULL,
    explanation_text TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (citation_id, version)
);

CREATE TABLE IF NOT EXISTS ledger_write (
    write_id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT,
    rev INTEGER,
    operation TEXT NOT NULL,
    content_hash TEXT,
    committed_at TEXT NOT NULL
);
"""


def connect(path: str | Path = ":memory:") -> sqlite3.Connection:
    if path != ":memory:":
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA busy_timeout = 5000")
    conn.executescript(SCHEMA)
    conn.execute(
        "INSERT OR IGNORE INTO schema_meta(key, value) VALUES ('schema_version', '1')"
    )
    conn.commit()
    return conn
