
/*
    File: 01_create_paytrace_ingest.sql
    Description: Creates the paytrace_ingest schema and initial ingest tables,
                 indexes, and constraints for CSV file ingest processing.
    Copyright: OpenFintechLab
    Change Log:
        - 2026-06-06: Added file header comment block.
        - 2026-06-06: [1] Added response file metadata fields to oftl_fwcsv_registry for response writing.
                      [2] Updated status check constraint on oftl_fwcsv_registry to include new response-related statuses.
*/

CREATE SCHEMA IF NOT EXISTS paytrace_ingest;
SET search_path TO paytrace_ingest;

DROP TABLE IF EXISTS oftl_fwcsv_registry;
 CREATE TABLE IF NOT EXISTS oftl_fwcsv_registry (
                file_id VARCHAR(128) PRIMARY KEY,
                filename TEXT NOT NULL,
                file_size BIGINT NOT NULL,
                mtime_ns BIGINT NOT NULL,
                checksum_sha256 VARCHAR(64),
                status VARCHAR(32) NOT NULL,
                row_count BIGINT NOT NULL DEFAULT 0,
                error_message TEXT,
                started_at TIMESTAMPTZ,
                ended_at TIMESTAMPTZ,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                -- Additional metadata fields for response writing
                response_status VARCHAR(32) DEFAULT 'PROCESSING',
                response_file_name VARCHAR(128) NULL,
                response_file_generated_at TIMESTAMPTZ  NULL
            );
DROP TABLE IF EXISTS  oftl_fwcsv_checkpoint;
CREATE TABLE IF NOT EXISTS oftl_fwcsv_checkpoint (
    file_id VARCHAR(128) PRIMARY KEY,
    row_number BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

DROP TABLE IF EXISTS   oftl_fwcsv_row_dispatch;
CREATE TABLE IF NOT EXISTS oftl_fwcsv_row_dispatch (
     transfer_id VARCHAR(36) PRIMARY KEY,
     file_id VARCHAR(128) NOT NULL,
     row_number BIGINT NOT NULL,
     request_queue TEXT NOT NULL,
     status VARCHAR(32) NOT NULL,
     published_at TIMESTAMPTZ NULL,
     error_message TEXT NULL,
     updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
 );

 CREATE INDEX IF NOT EXISTS idx_oftl_fwcsv_row_dispatch_file_id
    ON oftl_fwcsv_row_dispatch (file_id);

CREATE INDEX IF NOT EXISTS idx_oftl_fwcsv_row_dispatch_status
    ON oftl_fwcsv_row_dispatch (status);

CREATE INDEX IF NOT EXISTS idx_oftl_fwcsv_row_dispatch_updated_at
    ON oftl_fwcsv_row_dispatch (updated_at);

ALTER TABLE oftl_fwcsv_row_dispatch
    ADD CONSTRAINT chk_oftl_fwcsv_row_dispatch_status
    CHECK (status IN ('PUBLISHED', 'FAILED', 'PROCESSED'));

ALTER TABLE oftl_fwcsv_registry
    ADD CONSTRAINT chk_oftl_fwcsv_registry_status
    CHECK (status IN ('COMPLETED', 'FAILED', 'PROCESSED', 'PROCESSING', 'RESP_FILE_GENERATED', 'READY_FOR_RESPONSE', 'RESP_FILE_GENERATED'));

-- [MFB:20260606]: Added new response-related statuses to the status check constraint on oftl_fwcsv_registry.
ALTER TABLE oftl_fwcsv_registry
    ADD CONSTRAINT chk_oftl_fwcsy_registry_response_status
    CHECK (response_status IN ('PROCESSING', 'READY_FOR_RESPONSE', 'RESP_FILE_GENERATED'));


