package topology

import (
	"flow-core/internal/testdb"

	"database/sql"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	_ "github.com/lib/pq"
)

// BenchmarkTopologyNeighborsDepth5 fixes the Phase 3 decision baseline at an
// exact logical ring node00001..node10000. UUIDs are the physical PostgreSQL
// encoding; benchmarkNodeID keeps the logical nodeNNNNN names explicit.
func BenchmarkTopologyNeighborsDepth5(b *testing.B) {
	dsn := os.Getenv("FLOW_TEST_PG_DSN")
	if dsn == "" {
		b.Skip("FLOW_TEST_PG_DSN not set")
	}
	db, err := sql.Open("postgres", testdb.Scoped(b, dsn))
	if err != nil {
		b.Fatalf("open: %v", err)
	}
	if err := db.Ping(); err != nil {
		b.Fatalf("ping: %v", err)
	}
	b.Cleanup(func() { db.Close() })

	statements := []string{
		`DROP TABLE IF EXISTS topology_edge CASCADE`,
		`DROP TABLE IF EXISTS relation CASCADE`,
		`CREATE TABLE relation (
			from_id uuid, from_type text, to_id uuid, to_type text,
			relation_type_group text, relation_type text, additional_info text, version bigint default 0,
			PRIMARY KEY (from_id,from_type,relation_type_group,relation_type,to_id,to_type))`,
		`CREATE TABLE topology_edge (
			tenant_id uuid NOT NULL, from_id uuid NOT NULL, from_type varchar(255) NOT NULL,
			to_id uuid NOT NULL, to_type varchar(255) NOT NULL,
			relation_type_group varchar(255) NOT NULL DEFAULT 'COMMON', relation_type varchar(255) NOT NULL,
			direction varchar(32) NOT NULL DEFAULT 'DIRECTED', metadata jsonb NOT NULL DEFAULT '{}',
			created_time bigint NOT NULL, updated_time bigint NOT NULL, version bigint NOT NULL DEFAULT 1,
			PRIMARY KEY (tenant_id,from_id,from_type,relation_type_group,relation_type,to_id,to_type,direction),
			CHECK (direction IN ('DIRECTED','BIDIRECTIONAL')))`,
		`CREATE UNIQUE INDEX topology_edge_bidirectional_unq
			ON topology_edge (tenant_id,relation_type_group,relation_type,
			LEAST(from_type||':'||from_id::text,to_type||':'||to_id::text),
			GREATEST(from_type||':'||from_id::text,to_type||':'||to_id::text))
			WHERE direction='BIDIRECTIONAL'`,
		`CREATE INDEX idx_topology_edge_from
			ON topology_edge (tenant_id,relation_type_group,from_type,from_id,relation_type)`,
		`CREATE INDEX idx_topology_edge_to
			ON topology_edge (tenant_id,relation_type_group,to_type,to_id,relation_type)`,
	}
	for _, statement := range statements {
		if _, err := db.Exec(statement); err != nil {
			b.Fatalf("benchmark schema: %v\n%s", err, statement)
		}
	}

	const edgeCount = 10000
	const bidirectionalCount = edgeCount / 10
	now := time.Now().UnixMilli()
	tx, err := db.Begin()
	if err != nil {
		b.Fatalf("begin setup: %v", err)
	}
	stmt, err := tx.Prepare(`INSERT INTO topology_edge
		(tenant_id,from_id,from_type,to_id,to_type,relation_type_group,relation_type,
		 direction,metadata,created_time,updated_time,version)
		VALUES ($1,$2,'DEVICE',$3,'DEVICE','COMMON','ConnectedTo',$4,'{}',$5,$5,1)`)
	if err != nil {
		b.Fatalf("prepare setup: %v", err)
	}
	for i := 1; i <= edgeCount; i++ {
		fromName := fmt.Sprintf("node%05d", i)
		toName := fmt.Sprintf("node%05d", i%edgeCount+1)
		direction := "DIRECTED"
		if i%10 == 0 {
			direction = "BIDIRECTIONAL"
		}
		if _, err := stmt.Exec(tenantA, benchmarkNodeID(fromName), benchmarkNodeID(toName), direction, now); err != nil {
			b.Fatalf("insert edge %s -> %s: %v", fromName, toName, err)
		}
	}
	if err := stmt.Close(); err != nil {
		b.Fatalf("close setup statement: %v", err)
	}
	if err := tx.Commit(); err != nil {
		b.Fatalf("commit setup: %v", err)
	}
	var edges, bidirectional int
	if err := db.QueryRow(`SELECT count(*), count(*) FILTER (WHERE direction='BIDIRECTIONAL') FROM topology_edge`).Scan(&edges, &bidirectional); err != nil {
		b.Fatalf("count graph: %v", err)
	}
	if edges != edgeCount || bidirectional != bidirectionalCount {
		b.Fatalf("graph shape edges=%d bidirectional=%d", edges, bidirectional)
	}

	root := EntityRef{Type: "DEVICE", ID: benchmarkNodeID("node00001")}
	visited, err := benchmarkWalk(db, root, 5)
	if err != nil {
		b.Fatalf("warmup traversal: %v", err)
	}
	if len(visited) != 7 {
		b.Fatalf("warmup visited=%d, want 7", len(visited))
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		visited, err = benchmarkWalk(db, root, 5)
		if err != nil {
			b.Fatalf("traversal: %v", err)
		}
		if len(visited) != 7 {
			b.Fatalf("visited=%d, want 7", len(visited))
		}
	}
	b.ReportMetric(edgeCount, "edges")
	b.ReportMetric(bidirectionalCount, "bidirectional_edges")
	b.ReportMetric(float64(bidirectionalCount)/edgeCount, "bidirectional_share")
	b.ReportMetric(7, "visited_nodes")
}

func benchmarkWalk(db *sql.DB, root EntityRef, depth int) (map[EntityRef]struct{}, error) {
	visited := map[EntityRef]struct{}{root: {}}
	frontier := []EntityRef{root}
	for level := 0; level < depth && len(frontier) > 0; level++ {
		next := make([]EntityRef, 0)
		for _, current := range frontier {
			neighbors, err := Neighbors(db, current, "FROM", []string{"ConnectedTo"})
			if err != nil {
				return nil, err
			}
			for _, neighbor := range neighbors {
				if _, seen := visited[neighbor]; seen {
					continue
				}
				visited[neighbor] = struct{}{}
				next = append(next, neighbor)
			}
		}
		frontier = next
	}
	return visited, nil
}

// benchmarkNodeID maps a logical nodeNNNNN name to its UUID. The bound is wide
// enough for the CTE benchmark's 20,000-node two-ring graph; the original
// 10,000-node ring used by BenchmarkTopologyNeighborsDepth5 stays in range.
func benchmarkNodeID(name string) string {
	index, err := strconv.Atoi(strings.TrimPrefix(name, "node"))
	if err != nil || index < 1 || index > 20000 {
		panic("invalid benchmark node name: " + name)
	}
	return fmt.Sprintf("00000000-0000-0000-0000-%012d", index)
}

// BenchmarkExpandCTEDepth5 measures the tenant-scoped recursive CTE
// (ExpandWithCTE) at depth 5 over a 40,000-edge synthetic graph: two
// interleaved 20,000-node rings with offsets {1, 1000} (fan-out 2, so the
// depth-10 walk count stays bounded). It fixes DefaultExpandMaxDepth /
// DefaultExpandMaxNodes (see docs/DIGITAL_TWIN.md); do not raise those limits
// without re-running this benchmark.
//
// Run: go test -run '^$' -bench BenchmarkExpandCTE ./internal/topology/
func BenchmarkExpandCTEDepth5(b *testing.B) {
	benchmarkExpandCTE(b, 5)
}

// BenchmarkExpandCTEDepth10 is the depth-10 counterpart that bounds the
// DefaultExpandMaxDepth ceiling.
func BenchmarkExpandCTEDepth10(b *testing.B) {
	benchmarkExpandCTE(b, 10)
}

func benchmarkExpandCTE(b *testing.B, depth int) {
	dsn := os.Getenv("FLOW_TEST_PG_DSN")
	if dsn == "" {
		b.Skip("FLOW_TEST_PG_DSN not set")
	}
	db, err := sql.Open("postgres", testdb.Scoped(b, dsn))
	if err != nil {
		b.Fatalf("open: %v", err)
	}
	if err := db.Ping(); err != nil {
		b.Fatalf("ping: %v", err)
	}
	b.Cleanup(func() { db.Close() })

	statements := []string{
		`DROP TABLE IF EXISTS topology_edge CASCADE`,
		`DROP TABLE IF EXISTS relation CASCADE`,
		`DROP TABLE IF EXISTS asset CASCADE`,
		`DROP TABLE IF EXISTS device CASCADE`,
		`DROP TABLE IF EXISTS customer CASCADE`,
		`DROP TABLE IF EXISTS entity_view CASCADE`,
		`DROP TABLE IF EXISTS dashboard CASCADE`,
		`DROP TABLE IF EXISTS device_profile CASCADE`,
		`DROP TABLE IF EXISTS asset_profile CASCADE`,
		`CREATE TABLE relation (
			from_id uuid, from_type text, to_id uuid, to_type text,
			relation_type_group text, relation_type text, additional_info text, version bigint default 0,
			PRIMARY KEY (from_id,from_type,relation_type_group,relation_type,to_id,to_type))`,
		// The tenant-resolved legacy branch of the traversal union LEFT JOINs
		// these entity tables; they stay empty in the synthetic graph.
		`CREATE TABLE asset (id uuid PRIMARY KEY, tenant_id uuid)`,
		`CREATE TABLE device (id uuid PRIMARY KEY, tenant_id uuid)`,
		`CREATE TABLE customer (id uuid PRIMARY KEY, tenant_id uuid)`,
		`CREATE TABLE entity_view (id uuid PRIMARY KEY, tenant_id uuid)`,
		`CREATE TABLE dashboard (id uuid PRIMARY KEY, tenant_id uuid)`,
		`CREATE TABLE device_profile (id uuid PRIMARY KEY, tenant_id uuid)`,
		`CREATE TABLE asset_profile (id uuid PRIMARY KEY, tenant_id uuid)`,
		`CREATE TABLE topology_edge (
			tenant_id uuid NOT NULL, from_id uuid NOT NULL, from_type varchar(255) NOT NULL,
			to_id uuid NOT NULL, to_type varchar(255) NOT NULL,
			relation_type_group varchar(255) NOT NULL DEFAULT 'COMMON', relation_type varchar(255) NOT NULL,
			direction varchar(32) NOT NULL DEFAULT 'DIRECTED', metadata jsonb NOT NULL DEFAULT '{}',
			created_time bigint NOT NULL, updated_time bigint NOT NULL, version bigint NOT NULL DEFAULT 1,
			PRIMARY KEY (tenant_id,from_id,from_type,relation_type_group,relation_type,to_id,to_type,direction),
			CHECK (direction IN ('DIRECTED','BIDIRECTIONAL')))`,
		`CREATE INDEX idx_topology_edge_from
			ON topology_edge (tenant_id,relation_type_group,from_type,from_id,relation_type)`,
		`CREATE INDEX idx_topology_edge_to
			ON topology_edge (tenant_id,relation_type_group,to_type,to_id,relation_type)`,
	}
	for _, statement := range statements {
		if _, err := db.Exec(statement); err != nil {
			b.Fatalf("benchmark schema: %v\n%s", err, statement)
		}
	}

	// A sparse two-ring graph: 20,000 nodes each with two forward edges at
	// offsets {1, 1000} = 40,000 edges total (tens of thousands). Fan-out is 2
	// so the depth-10 walk count is bounded (2^10) instead of exploding; a
	// denser pure-expansion graph is the pathological case the node budget
	// exists to reject, not the limit-fixing baseline.
	const edgeCount = 20000
	const ringCount = 2
	offsets := []int{1, 1000}
	now := time.Now().UnixMilli()
	tx, err := db.Begin()
	if err != nil {
		b.Fatalf("begin setup: %v", err)
	}
	stmt, err := tx.Prepare(`INSERT INTO topology_edge
		(tenant_id,from_id,from_type,to_id,to_type,relation_type_group,relation_type,
		 direction,metadata,created_time,updated_time,version)
		VALUES ($1,$2,'DEVICE',$3,'DEVICE','COMMON','ConnectedTo','DIRECTED','{}',$4,$4,1)`)
	if err != nil {
		b.Fatalf("prepare setup: %v", err)
	}
	for i := 1; i <= edgeCount; i++ {
		fromName := fmt.Sprintf("node%05d", i)
		for _, offset := range offsets {
			toIndex := (i-1+offset)%edgeCount + 1
			toName := fmt.Sprintf("node%05d", toIndex)
			if _, err := stmt.Exec(tenantA, benchmarkNodeID(fromName), benchmarkNodeID(toName), now); err != nil {
				b.Fatalf("insert edge %s -> %s: %v", fromName, toName, err)
			}
		}
	}
	if err := stmt.Close(); err != nil {
		b.Fatalf("close setup statement: %v", err)
	}
	if err := tx.Commit(); err != nil {
		b.Fatalf("commit setup: %v", err)
	}
	var edges int
	if err := db.QueryRow(`SELECT count(*) FROM topology_edge`).Scan(&edges); err != nil {
		b.Fatalf("count graph: %v", err)
	}
	if edges != edgeCount*ringCount {
		b.Fatalf("graph shape edges=%d want %d", edges, edgeCount*ringCount)
	}

	root := EntityRef{Type: "DEVICE", ID: benchmarkNodeID("node00001")}
	minVisited := 10
	if depth >= 10 {
		minVisited = 40
	}
	visited, err := ExpandWithCTE(db, tenantA, root, "FROM", []string{"ConnectedTo"}, depth, DefaultExpandMaxNodes)
	if err != nil {
		b.Fatalf("warmup traversal: %v", err)
	}
	if len(visited) <= minVisited || len(visited) > DefaultExpandMaxNodes {
		b.Fatalf("warmup visited=%d (want >%d, <=%d)", len(visited), minVisited, DefaultExpandMaxNodes)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		visited, err = ExpandWithCTE(db, tenantA, root, "FROM", []string{"ConnectedTo"}, depth, DefaultExpandMaxNodes)
		if err != nil {
			b.Fatalf("traversal: %v", err)
		}
		if len(visited) <= minVisited || len(visited) > DefaultExpandMaxNodes {
			b.Fatalf("visited=%d out of range", len(visited))
		}
	}
	b.ReportMetric(edgeCount*ringCount, "edges")
	b.ReportMetric(float64(len(visited)), "visited_nodes")
}
