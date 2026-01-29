-- Schéma SQL pour le data mart de scores biométriques
-- Table fact pour le reporting BI

CREATE TABLE IF NOT EXISTS biometric_scores (
  id              SERIAL PRIMARY KEY,

  -- Contexte transaction
  re_id           TEXT NOT NULL,
  re_code         INTEGER,
  rq_type         TEXT NOT NULL DEFAULT 'IP',
  log_date        DATE,
  server_name     TEXT,
  source_file     TEXT,

  -- Dimensions biométriques
  modality        TEXT NOT NULL,        -- 'FACE' | 'IRIS' | 'FINGER'
  channel         TEXT NOT NULL,         -- 'FACE' | 'LEFT_EYE' | 'RIGHT_EYE' | 'RIGHT_THUMB' | ...
  sample_id       INTEGER,               -- FaceSampleId / IrisSampleId / FingerprintSampleId
  sample_type     TEXT,                  -- STILL, TENPRINT_SLAP, ...

  -- Mesures
  score           INTEGER,               -- score principal
  nbpk            INTEGER,               -- nombre de points caractéristiques (minutiae) - NULL pour FACE/IRIS

  -- Technique
  raw_line        TEXT,
  created_at      TIMESTAMPTZ DEFAULT now()
);

-- Index pour les requêtes BI courantes
CREATE INDEX IF NOT EXISTS idx_biometric_scores_log_date ON biometric_scores(log_date);
CREATE INDEX IF NOT EXISTS idx_biometric_scores_server ON biometric_scores(server_name);
CREATE INDEX IF NOT EXISTS idx_biometric_scores_modality_channel ON biometric_scores(modality, channel);
CREATE INDEX IF NOT EXISTS idx_biometric_scores_re_id ON biometric_scores(re_id);
CREATE INDEX IF NOT EXISTS idx_biometric_scores_rq_type ON biometric_scores(rq_type);
