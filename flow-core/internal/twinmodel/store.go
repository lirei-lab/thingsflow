package twinmodel

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/lib/pq"
)

var (
	ErrConflict     = errors.New("twin model version already exists")
	ErrNotFound     = errors.New("twin model or entity not found")
	ErrDeprecated   = errors.New("twin model version is deprecated")
	ErrForbidden    = errors.New("cross-tenant access denied")
	ErrKindMismatch = errors.New("model kind does not match entity type")
	ErrInvalidModel = errors.New("invalid twin model")
)

// Record is one immutable authored model version plus catalog lifecycle data.
// Definition and Schema are the exact JSON documents persisted in jsonb.
type Record struct {
	Model       Model           `json:"-"`
	Definition  json.RawMessage `json:"-"`
	Schema      json.RawMessage `json:"-"`
	Deprecated  bool            `json:"deprecated"`
	CreatedTime int64           `json:"createdTime"`
	UpdatedTime int64           `json:"updatedTime"`
}

type ListOptions struct {
	Page              int
	PageSize          int
	Kind              string
	Latest            bool
	IncludeDeprecated bool
}

type Page struct {
	Data          []Record `json:"data"`
	TotalElements int      `json:"totalElements"`
	TotalPages    int      `json:"totalPages"`
	HasNext       bool     `json:"hasNext"`
	Page          int      `json:"page"`
}

type Pin struct {
	EntityType string `json:"entityType"`
	EntityID   string `json:"entityId"`
	ModelID    string `json:"modelId"`
	Version    string `json:"version"`
	Definition string `json:"definition"`
}

type Store struct {
	db *sql.DB
}

func NewStore(db *sql.DB) *Store {
	return &Store{db: db}
}

// Create normalizes and inserts one immutable model version, then activates it
// only for matching, currently unpinned registry rows in the same transaction.
func (s *Store) Create(ctx context.Context, tenantID string, authored json.RawMessage) (Record, error) {
	model, schema, err := Normalize(authored)
	if err != nil {
		return Record{}, fmt.Errorf("%w: %v", ErrInvalidModel, err)
	}
	definition, err := json.Marshal(model)
	if err != nil {
		return Record{}, fmt.Errorf("marshal normalized model: %w", err)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Record{}, err
	}
	defer tx.Rollback()

	now := time.Now().UnixMilli()
	var record Record
	err = tx.QueryRowContext(ctx, `
		INSERT INTO twin_model
		    (tenant_id, model_id, version, kind, definition, schema, deprecated, created_time, updated_time)
		VALUES ($1, $2, $3, $4, $5::jsonb, $6::jsonb, false, $7, $7)
		RETURNING definition, schema, deprecated, created_time, updated_time`,
		tenantID, model.ModelID, model.Version, model.Kind, definition, schema, now,
	).Scan(&record.Definition, &record.Schema, &record.Deprecated, &record.CreatedTime, &record.UpdatedTime)
	if err != nil {
		if isUniqueViolation(err) {
			return Record{}, ErrConflict
		}
		return Record{}, err
	}
	record.Model = model

	if err := activateMatchingRegistryRows(ctx, tx, tenantID, model); err != nil {
		return Record{}, err
	}
	if err := tx.Commit(); err != nil {
		return Record{}, err
	}
	return record, nil
}

func activateMatchingRegistryRows(ctx context.Context, tx *sql.Tx, tenantID string, model Model) error {
	table := "device"
	if model.Kind == "ASSET" {
		table = "asset"
	}
	definition := definitionURN(model.Kind, model.ModelID, model.Version)
	query := `UPDATE twin_registry tr
		SET model_id=$2, model_version=$3, definition=$4,
		    updated_time=(extract(epoch from now())*1000)::bigint,
		    version=tr.version+1
		FROM ` + table + ` entity
		WHERE tr.tenant_id=$1 AND tr.entity_type=$5 AND tr.model_id IS NULL
		  AND entity.id=tr.entity_id AND entity.tenant_id=tr.tenant_id
		  AND COALESCE(NULLIF(trim(both '_' from regexp_replace(lower(COALESCE(entity.type, 'default')), '[^a-z0-9_]+', '_', 'g')), ''), 'default')=$2`
	_, err := tx.ExecContext(ctx, query, tenantID, model.ModelID, model.Version, definition, model.Kind)
	return err
}

func (s *Store) Get(ctx context.Context, tenantID, modelID, version string) (Record, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT definition, schema, deprecated, created_time, updated_time
		  FROM twin_model
		 WHERE tenant_id=$1 AND model_id=$2 AND version=$3`, tenantID, modelID, version)
	return scanRecord(row)
}

func (s *Store) Deprecate(ctx context.Context, tenantID, modelID, version string) (Record, error) {
	row := s.db.QueryRowContext(ctx, `
		UPDATE twin_model
		   SET updated_time=CASE WHEN deprecated THEN updated_time ELSE (extract(epoch from now())*1000)::bigint END,
		       deprecated=true
		 WHERE tenant_id=$1 AND model_id=$2 AND version=$3
		RETURNING definition, schema, deprecated, created_time, updated_time`, tenantID, modelID, version)
	return scanRecord(row)
}

func (s *Store) List(ctx context.Context, tenantID string, options ListOptions) (Page, error) {
	conditions := []string{"tenant_id=$1"}
	args := []interface{}{tenantID}
	if options.Kind != "" {
		args = append(args, options.Kind)
		conditions = append(conditions, fmt.Sprintf("kind=$%d", len(args)))
	}
	if !options.IncludeDeprecated {
		conditions = append(conditions, "deprecated=false")
	}
	where := strings.Join(conditions, " AND ")
	selection := `SELECT definition, schema, deprecated, created_time, updated_time,
		ROW_NUMBER() OVER (PARTITION BY model_id ORDER BY string_to_array(version,'.')::int[] DESC) AS version_rank
		FROM twin_model WHERE ` + where
	if !options.Latest {
		selection = `SELECT definition, schema, deprecated, created_time, updated_time, 1::bigint AS version_rank
		FROM twin_model WHERE ` + where
	}
	args = append(args, options.PageSize, options.Page*options.PageSize)
	query := `WITH selected AS (` + selection + `), page_rows AS (
		SELECT *, count(*) OVER () AS total_elements
		FROM selected WHERE version_rank=1
		ORDER BY (definition->>'modelId'), string_to_array(definition->>'version','.')::int[] DESC
		LIMIT $` + fmt.Sprint(len(args)-1) + ` OFFSET $` + fmt.Sprint(len(args)) + `)
		SELECT definition, schema, deprecated, created_time, updated_time, total_elements FROM page_rows`
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return Page{}, err
	}
	defer rows.Close()

	page := Page{Data: []Record{}, Page: options.Page}
	for rows.Next() {
		var record Record
		if err := rows.Scan(&record.Definition, &record.Schema, &record.Deprecated, &record.CreatedTime, &record.UpdatedTime, &page.TotalElements); err != nil {
			return Page{}, err
		}
		if err := json.Unmarshal(record.Definition, &record.Model); err != nil {
			return Page{}, err
		}
		page.Data = append(page.Data, record)
	}
	if err := rows.Err(); err != nil {
		return Page{}, err
	}
	// A page beyond the final row has no window count; obtain the total with
	// the same selection so the envelope remains exact for empty pages.
	if len(page.Data) == 0 {
		countQuery := `WITH selected AS (` + selection + `) SELECT count(*) FROM selected WHERE version_rank=1`
		countArgs := args[:len(args)-2]
		if err := s.db.QueryRowContext(ctx, countQuery, countArgs...).Scan(&page.TotalElements); err != nil {
			return Page{}, err
		}
	}
	if page.TotalElements > 0 {
		page.TotalPages = (page.TotalElements + options.PageSize - 1) / options.PageSize
	}
	page.HasNext = options.Page+1 < page.TotalPages
	return page, nil
}

// Repoint resolves the source entity first, enforces caller tenancy, then
// resolves the active model inside the entity's actual tenant. All reads and
// the registry update are protected by one transaction.
func (s *Store) Repoint(ctx context.Context, callerTenant, entityType, entityID, modelID, version string, sysAdmin bool) (Pin, error) {
	table := "device"
	if entityType == "ASSET" {
		table = "asset"
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Pin{}, err
	}
	defer tx.Rollback()

	var actualTenant, sourceType string
	err = tx.QueryRowContext(ctx, `SELECT tenant_id::text, COALESCE(type,'') FROM `+table+` WHERE id=$1 FOR UPDATE`, entityID).Scan(&actualTenant, &sourceType)
	if errors.Is(err, sql.ErrNoRows) {
		return Pin{}, ErrNotFound
	}
	if err != nil {
		return Pin{}, err
	}
	if !sysAdmin && actualTenant != callerTenant {
		return Pin{}, ErrForbidden
	}

	var kind string
	var deprecated bool
	err = tx.QueryRowContext(ctx, `SELECT kind, deprecated FROM twin_model
		WHERE tenant_id=$1 AND model_id=$2 AND version=$3 FOR SHARE`, actualTenant, modelID, version).Scan(&kind, &deprecated)
	if errors.Is(err, sql.ErrNoRows) {
		return Pin{}, ErrNotFound
	}
	if err != nil {
		return Pin{}, err
	}
	if deprecated {
		return Pin{}, ErrDeprecated
	}
	if kind != entityType {
		return Pin{}, ErrKindMismatch
	}
	_ = sourceType // Source type is locked with the entity for activation consistency.

	definition := definitionURN(entityType, modelID, version)
	result, err := tx.ExecContext(ctx, `UPDATE twin_registry
		SET model_id=$4::varchar, model_version=$5::varchar, definition=$6,
		    updated_time=CASE WHEN model_id=$4::varchar AND model_version=$5::varchar THEN updated_time
		                      ELSE (extract(epoch from now())*1000)::bigint END,
		    version=CASE WHEN model_id=$4::varchar AND model_version=$5::varchar THEN version ELSE version+1 END
		WHERE tenant_id=$1 AND entity_type=$2 AND entity_id=$3`,
		actualTenant, entityType, entityID, modelID, version, definition)
	if err != nil {
		return Pin{}, err
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return Pin{}, err
	}
	if changed == 0 {
		return Pin{}, ErrNotFound
	}
	if err := tx.Commit(); err != nil {
		return Pin{}, err
	}
	return Pin{EntityType: entityType, EntityID: entityID, ModelID: modelID, Version: version, Definition: definition}, nil
}

type rowScanner interface {
	Scan(dest ...interface{}) error
}

func scanRecord(row rowScanner) (Record, error) {
	var record Record
	if err := row.Scan(&record.Definition, &record.Schema, &record.Deprecated, &record.CreatedTime, &record.UpdatedTime); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Record{}, ErrNotFound
		}
		return Record{}, err
	}
	if err := json.Unmarshal(record.Definition, &record.Model); err != nil {
		return Record{}, err
	}
	return record, nil
}

func definitionURN(kind, modelID, version string) string {
	return "thingsflow:" + strings.ToLower(kind) + ":" + modelID + ":" + version
}

func isUniqueViolation(err error) bool {
	var pqError *pq.Error
	return errors.As(err, &pqError) && pqError.Code == "23505"
}
