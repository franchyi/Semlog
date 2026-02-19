package finalizer

import pb "github.com/yourorg/redpanda-mm/gen/proto"

func CompareOps(a, b *pb.OpRef) int {
	if a.Hlc == nil && b.Hlc == nil {
		if a.Region < b.Region {
			return -1
		}
		if a.Region > b.Region {
			return 1
		}
		if a.OpId < b.OpId {
			return -1
		}
		if a.OpId > b.OpId {
			return 1
		}
		return 0
	}
	if a.Hlc == nil {
		return -1
	}
	if b.Hlc == nil {
		return 1
	}
	if a.Hlc.PhysicalMs < b.Hlc.PhysicalMs {
		return -1
	}
	if a.Hlc.PhysicalMs > b.Hlc.PhysicalMs {
		return 1
	}
	if a.Hlc.Logical < b.Hlc.Logical {
		return -1
	}
	if a.Hlc.Logical > b.Hlc.Logical {
		return 1
	}
	if a.Region < b.Region {
		return -1
	}
	if a.Region > b.Region {
		return 1
	}
	if a.OpId < b.OpId {
		return -1
	}
	if a.OpId > b.OpId {
		return 1
	}
	return 0
}

func CompareAccepted(a, b *pb.AcceptedRecord) int {
	return CompareOps(
		&pb.OpRef{Region: a.Region, OpId: a.OpId, Hlc: a.Hlc},
		&pb.OpRef{Region: b.Region, OpId: b.OpId, Hlc: b.Hlc},
	)
}
