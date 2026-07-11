// Package wire implements the drsync frame codec: u32-LE length, u16-LE frame
// type, protobuf payload. See docs/DESIGN-protocol.md §2.
package wire

import (
	"encoding/binary"
	"fmt"
	"io"

	"google.golang.org/protobuf/proto"

	drsyncpb "drsync/proto/gen/drsyncpb"
)

const (
	// MaxFrameLen caps a single frame payload; larger logical payloads batch.
	MaxFrameLen = 16 << 20
	headerLen   = 6
)

// WriteFrame marshals msg and writes one frame. Callers must serialize writes
// to the same conn (frames must not interleave).
func WriteFrame(w io.Writer, ft drsyncpb.FrameType, msg proto.Message) error {
	payload, err := proto.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal %v: %w", ft, err)
	}
	if len(payload) > MaxFrameLen {
		return fmt.Errorf("frame %v exceeds max length: %d", ft, len(payload))
	}
	buf := make([]byte, headerLen+len(payload))
	binary.LittleEndian.PutUint32(buf[0:4], uint32(len(payload)))
	binary.LittleEndian.PutUint16(buf[4:6], uint16(ft))
	copy(buf[headerLen:], payload)
	_, err = w.Write(buf)
	return err
}

// ReadFrame reads one frame header + payload. The returned payload is owned by
// the caller.
func ReadFrame(r io.Reader) (drsyncpb.FrameType, []byte, error) {
	var hdr [headerLen]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return 0, nil, err
	}
	n := binary.LittleEndian.Uint32(hdr[0:4])
	ft := drsyncpb.FrameType(binary.LittleEndian.Uint16(hdr[4:6]))
	if n > MaxFrameLen {
		return 0, nil, fmt.Errorf("frame %v exceeds max length: %d", ft, n)
	}
	payload := make([]byte, n)
	if _, err := io.ReadFull(r, payload); err != nil {
		return 0, nil, fmt.Errorf("short frame %v: %w", ft, err)
	}
	return ft, payload, nil
}
