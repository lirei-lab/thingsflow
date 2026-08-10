package policy

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
	ErrConflict      = errors.New("policy version already exists")
	ErrNotFound      = errors.New("policy not found")
	ErrDeprecated    = errors.New("policy version is deprecated")
	ErrForbidden     = errors.New("cross-tenant access denied")
	ErrInvalidPolicy = errors.New("invalid policy")
)

// ListOptions controls policy catalog listing. Latest restricts to the newest
// non-deprecated version per policy_id; IncludeDeprecated surfaces deprecated
// versions too.
type ListOptions struct {
	Page              int
	PageSize          int
	Latest            bool
	IncludeDeprecated bool
}

// Page is the standard TB-style envelope for catalog listings.
type Page struct {
	Data          []Record `json:"data"`
	TotalElements int      `json:"totalElements"`
	TotalPages    int      `json:"totalPages"`
	HasNext       bool     `json:"hasNext"`
	Page          int      `json:"page"`
}

// Store is the tenant-scoped, versioned policy catalog backed by the 0015
// migration. Every operation is scoped by the caller's tenant.
type Store struct {
	db *sql.DB
}

func NewStore(db *sql.DB) *Store {
	return &Store{db: db}
}

// Create normalizes and inserts one immutable policy version. Nothing invalid
// is persisted; a duplicate (tenant, policyId, version) is rejected.
func (s *Store) Create(ctx context.Context, tenantID string, authored json.RawMessage) (Record, error) {
	policy, derived, err := Normalize(authored)
	if err != nil {
		return Record{}, fmt.Errorf("%w: %v", ErrInvalidPolicy, err)
	}
	definition, err := json.Marshal(policy)
	if err != nil {
		return Record{}, fmt.Errorf("marshal normalized policy: %w", err)
	}
	derivedJSON, err := json.Marshal(derived)
	if err != nil {
		return Record{}, fmt.Errorf("marshal derived policy: %w", err)
	}

	now := time.Now().UnixMilli()
	var record Record
	err = s.db.QueryRowContext(ctx, `
		INSERT INTO policy
		    (tenant_id, policy_id, version, kind, definition, schema, deprecated, created_time, updated_time)
		VALUES ($1, $2, $3, $4, $5::jsonb, $6::jsonb, false, $7, $7)
		RETURNING definition, schema, deprecated, created_time, updated_time`,
		tenantID, policy.PolicyID, policy.Version, policy.Kind, definition, derivedJSON, now,
	).Scan(&record.Definition, &record.Schema, &record.Deprecated, &record.CreatedTime, &record.UpdatedTime)
	if err != nil {
		if isUniqueViolation(err) {
			return Record{}, ErrConflict
		}
		return Record{}, err
	}
	record.Policy = policy
	return record, nil
}

// Get returns one explicit version. Deprecated versions remain readable.
func (s *Store) Get(ctx context.Context, tenantID, policyID, version string) (Record, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT definition, schema, deprecated, created_time, updated_time
		  FROM policy
		 WHERE tenant_id=$1 AND policy_id=$2 AND version=$3`, tenantID, policyID, version)
	return scanRecord(row)
}

// Deprecate flags one version deprecated (kept so twin references stay valid).
func (s *Store) Deprecate(ctx context.Context, tenantID, policyID, version string) (Record, error) {
	row := s.db.QueryRowContext(ctx, `
		UPDATE policy
		   SET updated_time=CASE WHEN deprecated THEN updated_time ELSE (extract(epoch from now())*1000)::bigint END,
		       deprecated=true
		 WHERE tenant_id=$1 AND policy_id=$2 AND version=$3
		RETURNING definition, schema, deprecated, created_time, updated_time`, tenantID, policyID, version)
	return scanRecord(row)
}

// List pages over the tenant's catalog. With Latest, each policy_id yields its
// newest (non-deprecated unless IncludeDeprecated) version only.
func (s *Store) List(ctx context.Context, tenantID string, options ListOptions) (Page, error) {
	conditions := []string{"tenant_id=$1"}
	args := []interface{}{tenantID}
	if !options.IncludeDeprecated {
		conditions = append(conditions, "deprecated=false")
	}
	where := strings.Join(conditions, " AND ")
	selection := `SELECT definition, schema, deprecated, created_time, updated_time,
		ROW_NUMBER() OVER (PARTITION BY policy_id ORDER BY string_to_array(version,'.')::int[] DESC) AS version_rank
		FROM policy WHERE ` + where
	if !options.Latest {
		selection = `SELECT definition, schema, deprecated, created_time, updated_time, 1::bigint AS version_rank
		FROM policy WHERE ` + where
	}
	args = append(args, options.PageSize, options.Page*options.PageSize)
	query := `WITH selected AS (` + selection + `), page_rows AS (
		SELECT *, count(*) OVER () AS total_elements
		FROM selected WHERE version_rank=1
		ORDER BY (definition->>'policyId'), string_to_array(definition->>'version','.')::int[] DESC
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
		if err := json.Unmarshal(record.Definition, &record.Policy); err != nil {
			return Page{}, err
		}
		page.Data = append(page.Data, record)
	}
	if err := rows.Err(); err != nil {
		return Page{}, err
	}
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

// Resolve maps a twin registry policyId string to a real, deterministic policy
// document. It is the enforcement contract the 06-02 middleware calls.
//
//   - A foreign-tenant policyId (tenant:<other>:...) fails closed.
//   - An unknown policyId fails closed (ErrNotFound).
//   - The seeded tenant:<tid>:default (or the bare 'default') identity resolves
//     to the built-in owner-full-access policy when no explicit 'default' row
//     exists, so legacy twins keep working before any explicit policy is pinned.
func (s *Store) Resolve(ctx context.Context, tenantID, policyID string) (DerivedPolicy, error) {
	tid, pid, ok := SplitPolicyID(policyID)
	if ok {
		if tid != tenantID {
			return DerivedPolicy{}, ErrForbidden
		}
	} else {
		pid = policyID
	}
	if pid == "" {
		pid = DefaultPolicyID
	}
	canonical, err := NormalizePolicyID(pid)
	if err != nil {
		return DerivedPolicy{}, ErrNotFound
	}

	var schema json.RawMessage
	err = s.db.QueryRowContext(ctx, `
		SELECT schema
		  FROM (
			SELECT schema, ROW_NUMBER() OVER (ORDER BY string_to_array(version,'.')::int[] DESC) AS rn
			  FROM policy
			 WHERE tenant_id=$1 AND policy_id=$2 AND deprecated=false
		  ) latest WHERE rn=1`, tenantID, canonical).Scan(&schema)
	if err == nil {
		var derived DerivedPolicy
		if err := json.Unmarshal(schema, &derived); err != nil {
			return DerivedPolicy{}, fmt.Errorf("decode resolved policy: %w", err)
		}
		return derived, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return DerivedPolicy{}, err
	}

	// No explicit row. Only the default identity falls back to the built-in
	// owner policy; any other unknown id fails closed.
	if canonical != DefaultPolicyID {
		return DerivedPolicy{}, ErrNotFound
	}
	return builtinOwnerPolicy(tenantID), nil
}

// SplitPolicyID parses the registry policyId shape tenant:<tid>[:<pid>].
// It returns ok=false for bare canonical ids (no tenant: prefix).
func SplitPolicyID(policyID string) (tenantID, pid string, ok bool) {
	if !strings.HasPrefix(policyID, "tenant:") {
		return "", "", false
	}
	rest := strings.TrimPrefix(policyID, "tenant:")
	parts := strings.SplitN(rest, ":", 2)
	tenantID = parts[0]
	if len(parts) == 2 {
		pid = parts[1]
	}
	if pid == "" {
		pid = DefaultPolicyID
	}
	return tenantID, pid, true
}

// builtinOwnerPolicy is the implicit full-access policy backing the seeded
// tenant:<tid>:default identity. It grants the owning tenant subject every
// action on the tenant's thing:/<tid> resource tree (prefix match covers all
// descendants), so existing twins work with zero explicit policy rows.
func builtinOwnerPolicy(tenantID string) DerivedPolicy {
	root := "thing:/" + tenantID
	return DerivedPolicy{
		PolicyID:  DefaultPolicyID,
		Version:   "1.0.0",
		Kind:      "TWIN",
		Subjects:  []string{"tenant:" + tenantID},
		Resources: []string{root, root + "/#"},
		Grants:    map[string][]string{root: {"READ", "WRITE", "DELETE"}},
		Revokes:   map[string][]string{},
		Builtin:   true,
	}
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
	if err := json.Unmarshal(record.Definition, &record.Policy); err != nil {
		return Record{}, err
	}
	return record, nil
}

func isUniqueViolation(err error) bool {
	var pqError *pq.Error
	return errors.As(err, &pqError) && pqError.Code == "23505"
}
