package wire

import (
	"bytes"
	"encoding/binary"
	"testing"

	"google.golang.org/protobuf/proto"

	drsyncpb "drsync/proto/gen/drsyncpb"
)

func TestRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	in := &drsyncpb.Hello{AgentId: "agent-1", Hostname: "host1", ProtoMajor: 1, Cores: 64}
	if err := WriteFrame(&buf, drsyncpb.FrameType_FRAME_HELLO, in); err != nil {
		t.Fatal(err)
	}
	ft, payload, err := ReadFrame(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if ft != drsyncpb.FrameType_FRAME_HELLO {
		t.Fatalf("frame type = %v", ft)
	}
	out := &drsyncpb.Hello{}
	if err := proto.Unmarshal(payload, out); err != nil {
		t.Fatal(err)
	}
	if out.AgentId != "agent-1" || out.Cores != 64 {
		t.Fatalf("round trip mismatch: %+v", out)
	}
}

func TestOversizeRejected(t *testing.T) {
	var hdr [6]byte
	binary.LittleEndian.PutUint32(hdr[0:4], MaxFrameLen+1)
	binary.LittleEndian.PutUint16(hdr[4:6], uint16(drsyncpb.FrameType_FRAME_HELLO))
	if _, _, err := ReadFrame(bytes.NewReader(hdr[:])); err == nil {
		t.Fatal("oversize frame accepted")
	}
}
