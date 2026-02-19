package hlc

import "testing"

func TestNowMonotonic(t *testing.T) {
	clock := &HLC{}
	a := clock.Now()
	b := clock.Now()
	if Compare(a, b) >= 0 {
		t.Fatalf("expected a < b, got a=%+v b=%+v", a, b)
	}
}

func TestUpdateMonotonic(t *testing.T) {
	clock := &HLC{}
	local := clock.Now()
	remote := Timestamp{PhysicalMS: local.PhysicalMS + 10, Logical: 3}
	updated := clock.Update(remote)
	if updated.PhysicalMS != remote.PhysicalMS {
		t.Fatalf("expected physical to follow remote")
	}
	if updated.Logical != remote.Logical+1 {
		t.Fatalf("expected logical remote+1")
	}
}

func TestCompare(t *testing.T) {
	if Compare(Timestamp{1, 0}, Timestamp{2, 0}) != -1 {
		t.Fatal("physical ordering broken")
	}
	if Compare(Timestamp{2, 0}, Timestamp{2, 1}) != -1 {
		t.Fatal("logical ordering broken")
	}
	if Compare(Timestamp{2, 1}, Timestamp{2, 1}) != 0 {
		t.Fatal("equality broken")
	}
}
