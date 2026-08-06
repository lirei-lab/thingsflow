package topology

import "testing"

func TestBidirectionalConsistencyMatchesBothLegacyOrientations(t *testing.T) {
	db := newTestDB(t)
	setupTopologyTables(t, db)
	pinConnectedModels(t, db)
	edge := Edge{
		TenantID:     tenantA,
		From:         EntityRef{Type: "DEVICE", ID: deviceA},
		To:           EntityRef{Type: "DEVICE", ID: deviceC},
		RelationType: "ConnectedTo",
		Direction:    "BIDIRECTIONAL",
	}
	if err := SaveEdge(db, edge); err != nil {
		t.Fatalf("SaveEdge: %v", err)
	}
	report, err := CheckConsistency(db)
	if err != nil {
		t.Fatalf("CheckConsistency: %v", err)
	}
	if report.Status != "OK" || report.TopologyEdges != 1 || report.LegacyRelations != 2 ||
		report.GovernedLegacyRelations != 2 || report.LegacyMissingTopology != 0 ||
		report.TopologyMissingLegacy != 0 || report.InvalidTopologyRelationType != 0 {
		t.Fatalf("orientation-insensitive report=%+v", report)
	}

	result, err := RepairBackfill(db, false)
	if err != nil {
		t.Fatalf("RepairBackfill: %v", err)
	}
	if result.WouldInsert != 0 || result.Inserted != 0 || result.After.TopologyEdges != 1 {
		t.Fatalf("repair invented reverse duplicate: %+v", result)
	}
}

func TestBidirectionalConsistencyRequiresBothLegacyMirrors(t *testing.T) {
	db := newTestDB(t)
	setupTopologyTables(t, db)
	pinConnectedModels(t, db)
	edge := Edge{
		TenantID:     tenantA,
		From:         EntityRef{Type: "DEVICE", ID: deviceA},
		To:           EntityRef{Type: "DEVICE", ID: deviceC},
		RelationType: "ConnectedTo",
		Direction:    "BIDIRECTIONAL",
	}
	if err := SaveEdge(db, edge); err != nil {
		t.Fatalf("SaveEdge: %v", err)
	}
	if _, err := db.Exec(`DELETE FROM relation WHERE from_id=$1 AND from_type='DEVICE' AND to_id=$2`, deviceC, deviceA); err != nil {
		t.Fatalf("remove reverse mirror: %v", err)
	}
	report, err := CheckConsistency(db)
	if err != nil {
		t.Fatalf("CheckConsistency: %v", err)
	}
	if report.Status != "WARN" || report.TopologyMissingLegacy != 1 {
		t.Fatalf("missing reverse mirror was not detected: %+v", report)
	}
}
