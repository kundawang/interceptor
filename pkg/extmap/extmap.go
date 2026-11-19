// SPDX-FileCopyrightText: 2026 The Pion community <https://pion.ly>
// SPDX-License-Identifier: MIT

// Package extmap provides an interceptor that rewrites RTP header extension
// IDs, for SFUs that forward RTP between peers that negotiated different
// extension IDs for the same extension URI.
//
// The interceptor rewrites only the extension identifiers: extension values,
// the payload, SSRC, sequence numbers and timestamps are preserved. Both the
// RFC 8285 one-byte and two-byte header extension profiles are supported; the
// profile is upgraded from one-byte to two-byte when a mapped ID or value
// cannot be represented in the one-byte form (it is never downgraded).
//
// Deterministic rules applied to every packet:
//   - packets without extensions, or with an empty extension list, are
//     forwarded unchanged;
//   - extensions that have no configured mapping are preserved as-is;
//   - opaque RFC3550 extensions and encrypted (Cryptex, RFC 9335) extension
//     profiles are forwarded unchanged;
//   - short or malformed packets are forwarded unchanged on the read path;
//   - if a mapped target ID is already present in the packet (carried by an
//     unmapped extension), the mapping is skipped for that extension;
//   - configuration errors (duplicate URIs, conflicting or invalid IDs) are
//     reported when the mapping is installed, never per packet.
package extmap

import (
	"errors"

	"github.com/pion/rtp"
)

// Mapping describes how one RTP header extension is translated from the ID
// negotiated with the inbound (remote) peer to the ID negotiated with the
// outbound (local) peer.
//
// A Mapping may be configured by URI, by explicit IDs, or both:
//   - URI set, IDs zero: the inbound ID is resolved from the RTPHeaderExtensions
//     of bound remote streams, the outbound ID from bound local streams.
//   - URI empty: FromID and ToID are used directly and must both be non-zero.
//   - URI and one ID set: the zero ID is resolved from stream information.
type Mapping struct {
	// URI is the RFC 8285 extension URI, e.g.
	// "urn:ietf:params:rtp-hdrext:ssrc-audio-level". Optional.
	URI string

	// FromID is the extension ID used by the inbound (remote) peer.
	// Zero means "resolve from remote stream information" and requires URI.
	FromID uint8

	// ToID is the extension ID used by the outbound (local) peer.
	// Zero means "resolve from local stream information" and requires URI.
	ToID uint8
}

var (
	// ErrDuplicateURI is returned when a mapping list contains the same
	// extension URI twice.
	ErrDuplicateURI = errors.New("extmap: duplicate extension URI in mappings")
	// ErrConflictingMapping is returned when two mappings share the same
	// explicit source ID or the same explicit target ID.
	ErrConflictingMapping = errors.New("extmap: conflicting extension ID in mappings")
	// ErrInvalidID is returned when a mapping uses ID 0 (the padding ID)
	// where an explicit ID is required.
	ErrInvalidID = errors.New("extmap: invalid extension ID in mappings")
	// ErrUnrepresentable is returned when an extension cannot be represented
	// in either header extension profile (ID 0 or a value larger than 255
	// bytes). It can only occur for hand-crafted headers on the write path.
	ErrUnrepresentable = errors.New("extmap: extension cannot be represented")
)

// validateMappings checks a mapping list for duplicates, conflicts and
// invalid IDs. URI-resolved IDs are validated when streams are bound.
func validateMappings(mappings []Mapping) error {
	uris := make(map[string]struct{}, len(mappings))
	fromIDs := make(map[uint8]struct{}, len(mappings))
	toIDs := make(map[uint8]struct{}, len(mappings))

	for _, m := range mappings {
		if m.URI != "" {
			if _, ok := uris[m.URI]; ok {
				return ErrDuplicateURI
			}
			uris[m.URI] = struct{}{}
		} else if m.FromID == 0 || m.ToID == 0 {
			return ErrInvalidID
		}

		if m.FromID != 0 {
			if _, ok := fromIDs[m.FromID]; ok {
				return ErrConflictingMapping
			}
			fromIDs[m.FromID] = struct{}{}
		}
		if m.ToID != 0 {
			if _, ok := toIDs[m.ToID]; ok {
				return ErrConflictingMapping
			}
			toIDs[m.ToID] = struct{}{}
		}
	}

	return nil
}

// remapHeader rewrites the extension IDs of h according to table
// (inbound ID -> outbound ID). It reports whether the header was modified.
//
// Only the RFC 8285 one-byte and two-byte profiles are remapped; any other
// profile (opaque RFC3550 or encrypted Cryptex extensions) is left untouched.
// Extension order and values are preserved. The profile is upgraded to
// two-byte when a mapped ID or value does not fit the one-byte form.
func remapHeader(h *rtp.Header, table map[uint8]uint8) (bool, error) {
	if !h.Extension || len(h.Extensions) == 0 || len(table) == 0 {
		return false, nil
	}
	if h.ExtensionProfile != rtp.ExtensionProfileOneByte && h.ExtensionProfile != rtp.ExtensionProfileTwoByte {
		return false, nil
	}

	type extension struct {
		id      uint8
		payload []byte
	}

	// Snapshot the current extensions in order. If a packet carries the same
	// ID twice (invalid per RFC 8285) only the first occurrence is visible
	// through the public API and therefore remapped.
	ids := h.GetExtensionIDs()
	seen := make(map[uint8]struct{}, len(ids))
	in := make([]extension, 0, len(ids))
	for _, id := range ids {
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		in = append(in, extension{id: id, payload: h.GetExtension(id)})
	}

	out := make([]extension, 0, len(in))
	used := make(map[uint8]struct{}, len(in))
	changed := false
	for _, ext := range in {
		id := ext.id
		if to, ok := table[ext.id]; ok && to != ext.id {
			if _, taken := used[to]; !taken {
				id = to
				changed = true
			}
			// If the target ID is already taken by an earlier extension the
			// mapping is skipped and the extension keeps its original ID.
		}
		used[id] = struct{}{}
		out = append(out, extension{id: id, payload: ext.payload})
	}
	if !changed {
		return false, nil
	}

	// Determine the profile that can represent every extension. The two-byte
	// profile is a superset of the one-byte form, so upgrading is always safe.
	profile := h.ExtensionProfile
	if profile == rtp.ExtensionProfileOneByte {
		for _, ext := range out {
			if ext.id > 14 || len(ext.payload) > 16 {
				profile = rtp.ExtensionProfileTwoByte

				break
			}
		}
	}
	for _, ext := range out {
		if ext.id == 0 || len(ext.payload) > 255 {
			return false, ErrUnrepresentable
		}
	}

	// Rebuild in order. All (id, payload) pairs were validated above, so
	// SetExtension cannot fail here.
	h.Extensions = h.Extensions[:0]
	h.ExtensionProfile = profile
	for _, ext := range out {
		if err := h.SetExtension(ext.id, ext.payload); err != nil {
			return false, err
		}
	}

	return true, nil
}
