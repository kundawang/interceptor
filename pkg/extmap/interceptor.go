// SPDX-FileCopyrightText: 2026 The Pion community <https://pion.ly>
// SPDX-License-Identifier: MIT

package extmap

import (
	"sync"
	"sync/atomic"

	"github.com/pion/interceptor"
	"github.com/pion/rtp"
)

// Option configures an Interceptor.
type Option func(*Interceptor) error

// WithMappings installs the initial extension ID mappings. The list is
// validated; duplicate URIs, conflicting or invalid IDs are rejected.
// Mappings can be replaced at runtime with Interceptor.SetMappings.
func WithMappings(mappings ...Mapping) Option {
	return func(i *Interceptor) error {
		return i.SetMappings(mappings...)
	}
}

// Factory is an interceptor.Factory for the extmap Interceptor.
type Factory struct {
	opts []Option
}

// NewInterceptor returns a new Factory.
func NewInterceptor(opts ...Option) (*Factory, error) {
	return &Factory{opts: opts}, nil
}

// NewInterceptor returns a new Interceptor.
func (f *Factory) NewInterceptor(string) (interceptor.Interceptor, error) {
	i := &Interceptor{
		remoteIDs: map[string]uint8{},
		localIDs:  map[string]uint8{},
	}
	empty := []Mapping{}
	i.mappings.Store(&empty)

	for _, opt := range f.opts {
		if err := opt(i); err != nil {
			return nil, err
		}
	}

	return i, nil
}

// Interceptor rewrites RTP header extension IDs on both the inbound
// (BindRemoteStream) and outbound (BindLocalStream) paths, translating the
// IDs negotiated with the remote peer into the IDs negotiated with the local
// peer. It is safe for concurrent use by any number of streams, and its
// mappings can be replaced at runtime with SetMappings.
type Interceptor struct {
	interceptor.NoOp

	mappings atomic.Pointer[[]Mapping]

	mu        sync.RWMutex
	remoteIDs map[string]uint8 // extension URI -> ID negotiated with remote peers
	localIDs  map[string]uint8 // extension URI -> ID negotiated with local peers
}

// SetMappings atomically replaces the mapping table. Packets processed after
// SetMappings returns use the new table; packets in flight keep the old one.
// An invalid list is rejected and the previous table stays in effect.
func (i *Interceptor) SetMappings(mappings ...Mapping) error {
	if err := validateMappings(mappings); err != nil {
		return err
	}
	cp := make([]Mapping, len(mappings))
	copy(cp, mappings)
	i.mappings.Store(&cp)

	return nil
}

// Mappings returns a copy of the currently installed mapping table.
func (i *Interceptor) Mappings() []Mapping {
	ms := *i.mappings.Load()
	out := make([]Mapping, len(ms))
	copy(out, ms)

	return out
}

// BindLocalStream rewrites extension IDs on outgoing RTP packets and records
// the locally negotiated extension URIs for URI-based mappings.
func (i *Interceptor) BindLocalStream(info *interceptor.StreamInfo, writer interceptor.RTPWriter) interceptor.RTPWriter {
	i.recordExtensions(i.localIDs, info)

	return interceptor.RTPWriterFunc(func(header *rtp.Header, payload []byte, attributes interceptor.Attributes) (int, error) {
		if table := i.resolve(); len(table) > 0 {
			if _, err := remapHeader(header, table); err != nil {
				return 0, err
			}
		}

		return writer.Write(header, payload, attributes)
	})
}

// BindRemoteStream rewrites extension IDs on incoming RTP packets and records
// the remotely negotiated extension URIs for URI-based mappings.
func (i *Interceptor) BindRemoteStream(info *interceptor.StreamInfo, reader interceptor.RTPReader) interceptor.RTPReader {
	i.recordExtensions(i.remoteIDs, info)

	return interceptor.RTPReaderFunc(func(b []byte, a interceptor.Attributes) (int, interceptor.Attributes, error) {
		n, attr, err := reader.Read(b, a)
		if err != nil || n == 0 {
			return n, attr, err
		}

		table := i.resolve()
		if len(table) == 0 {
			return n, attr, nil
		}

		header := &rtp.Header{}
		headerLen, err := header.Unmarshal(b[:n])
		if err != nil {
			// Short or malformed packet: forward it unchanged.
			return n, attr, nil
		}

		changed, err := remapHeader(header, table)
		if err != nil || !changed {
			return n, attr, nil
		}

		newHeaderLen := header.MarshalSize()
		payloadLen := n - headerLen
		if newHeaderLen+payloadLen > len(b) {
			// The rewritten header does not fit the receive buffer
			// (only possible when upgrading to the two-byte profile):
			// forward the packet unchanged.
			return n, attr, nil
		}

		// Marshal the new header before moving the payload: the extension
		// values still point into b, so marshaling in place could overwrite
		// them while they are being read.
		newHeader, err := header.Marshal()
		if err != nil {
			return n, attr, nil
		}
		copy(b[newHeaderLen:], b[headerLen:n]) //nolint:gocritic // copy is memmove-safe for overlapping ranges
		copy(b[:newHeaderLen], newHeader)

		return newHeaderLen + payloadLen, attr, nil
	})
}

// recordExtensions remembers the first ID bound for each extension URI.
// Later conflicting bindings for the same URI are ignored and unbinding a
// stream does not remove the entry; use SetMappings to reconfigure.
func (i *Interceptor) recordExtensions(dst map[string]uint8, info *interceptor.StreamInfo) {
	if info == nil {
		return
	}

	i.mu.Lock()
	defer i.mu.Unlock()

	for _, ext := range info.RTPHeaderExtensions {
		if ext.URI == "" || ext.ID < 1 || ext.ID > 255 {
			continue
		}
		if _, ok := dst[ext.URI]; !ok {
			dst[ext.URI] = uint8(ext.ID) //nolint:gosec // range checked above
		}
	}
}

// resolve builds the inbound ID -> outbound ID table for the current
// mappings and bound streams. Entries that cannot be resolved yet, identity
// mappings, and duplicates (first one wins) are skipped.
func (i *Interceptor) resolve() map[uint8]uint8 {
	ms := i.mappings.Load()
	if ms == nil || len(*ms) == 0 {
		return nil
	}

	i.mu.RLock()
	defer i.mu.RUnlock()

	var table map[uint8]uint8
	usedTo := make(map[uint8]struct{}, len(*ms))
	for _, m := range *ms {
		from, to := m.FromID, m.ToID
		if m.URI != "" {
			if from == 0 {
				from = i.remoteIDs[m.URI]
			}
			if to == 0 {
				to = i.localIDs[m.URI]
			}
		}
		if from == 0 || to == 0 || from == to {
			continue
		}
		if table == nil {
			table = make(map[uint8]uint8, len(*ms))
		}
		if _, ok := table[from]; ok {
			continue
		}
		if _, ok := usedTo[to]; ok {
			continue
		}
		table[from] = to
		usedTo[to] = struct{}{}
	}

	return table
}
