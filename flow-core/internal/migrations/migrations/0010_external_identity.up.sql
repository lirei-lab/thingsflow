CREATE TABLE IF NOT EXISTS external_identity (
    id uuid NOT NULL CONSTRAINT external_identity_pkey PRIMARY KEY,
    created_time bigint NOT NULL,
    updated_time bigint NOT NULL,
    provider_id varchar(255) NOT NULL,
    issuer varchar(512) NOT NULL,
    subject varchar(512) NOT NULL,
    user_id uuid NOT NULL REFERENCES tb_user(id) ON DELETE CASCADE,
    email varchar(255),
    claims jsonb,
    CONSTRAINT external_identity_provider_subject_unq UNIQUE (provider_id, issuer, subject)
);

CREATE INDEX IF NOT EXISTS idx_external_identity_user_id ON external_identity(user_id);
CREATE INDEX IF NOT EXISTS idx_external_identity_email ON external_identity(email);
