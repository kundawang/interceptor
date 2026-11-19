// SPDX-FileCopyrightText: 2026 The Pion community <https://pion.ly>
// SPDX-License-Identifier: MIT

package extmap

import (
	"sync"
	"testing"

	"github.com/pion/interceptor"
	"github.com/pion/interceptor/pkg/mock"
	"github.com/pion/rtp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	audioLevelURI  = "urn:ietf:params:rtp-hdrext:ssrc-audio-level"
	toffsetURI     = "urn:ietf:params:rtp-hdrext:toffset"
	transportCCURI = "http://www.ietf.org/id/draft-holmer-rmcat-transport-wide-cc-extensions-01"
)

// fixedPacket is an RTP packet with RFC 8285 one-byte header extensions as
// negotiated with the remote peer: audio-level on ID 1, toffset on ID 2.
var fixedPacket = []byte{
	0x90, 0xE0, 0x01, 0x02, // V=2, X=1, M=1, PT=96, seq=0x0102
	0x0A, 0x0B, 0x0C, 0x0D, // timestamp
	0x11, 0x22, 0x33, 0x44, // SSRC
	0xBE, 0xDE, 0x00, 0x02, // one-byte profile, 8 bytes of extensions
	0x10, 0xAB, // id=1, len=1: audio level
	0x21, 0x01, 0x02, // id=2, len=2: toffset
	0x00, 0x00, 0x00, // extension padding
	0xDE, 0xAD, 0xBE, 0xEF, // payload
}

// fixedPacketMapped is fixedPacket with the local peer's IDs: audio-level on
// ID 10, toffset on ID 14. Values, lengths, SSRC, timestamps and payload are
// identical to fixedPacket.
var fixedPacketMapped = []byte{
	0x90, 0xE0, 0x01, 0x02,
	0x0A, 0x0B, 0x0C, 0x0D,
	0x11, 0x22, 0x33, 0x44,
	0xBE, 0xDE, 0x00, 0x02,
	0xA0, 0xAB, // id=10, len=1: audio level
	0xE1, 0x01, 0x02, // id=14, len=2: toffset
	0x00, 0x00, 0x00,
	0xDE, 0xAD, 0xBE, 0xEF,
}

func newInterceptor(t *testing.T, opts ...Option) interceptor.Interceptor {
	t.Helper()

	factory, err := NewInterceptor(opts...)
	require.NoError(t, err)

	ic, err := factory.NewInterceptor("test")
	require.NoError(t, err)

	t.Cleanup(func() { assert.NoError(t, ic.Close()) })

	return ic
}

func readOnce(t *testing.T, ic interceptor.Interceptor, info *interceptor.StreamInfo, pkt []byte) []byte {
	t.Helper()

	reader := ic.BindRemoteStream(info, interceptor.RTPReaderFunc(
		func(b []byte, a interceptor.Attributes) (int, interceptor.Attributes, error) {
			return copy(b, pkt), a, nil
		}))

	buf := make([]byte, 1500)
	n, _, err := reader.Read(buf, nil)
	require.NoError(t, err)

	return buf[:n]
}

func TestExplicitMappingFixedPacket(t *testing.T) {
	ic := newInterceptor(t, WithMappings(
		Mapping{FromID: 1, ToID: 10},
		Mapping{FromID: 2, ToID: 14},
	))

	out := readOnce(t, ic, &interceptor.StreamInfo{}, fixedPacket)
	assert.Equal(t, fixedPacketMapped, out)

	// Verify every extension value, and that nothing else changed.
	pkt := &rtp.Packet{}
	require.NoError(t, pkt.Unmarshal(out))
	assert.Equal(t, []byte{0xAB}, pkt.Header.GetExtension(10))
	assert.Equal(t, []byte{0x01, 0x02}, pkt.Header.GetExtension(14))
	assert.Nil(t, pkt.Header.GetExtension(1))
	assert.Nil(t, pkt.Header.GetExtension(2))
	assert.Equal(t, uint32(0x11223344), pkt.Header.SSRC)
	assert.Equal(t, uint32(0x0A0B0C0D), pkt.Header.Timestamp)
	assert.Equal(t, uint16(0x0102), pkt.Header.SequenceNumber)
	assert.Equal(t, uint8(96), pkt.Header.PayloadType)
	assert.True(t, pkt.Header.Marker)
	assert.Equal(t, []byte{0xDE, 0xAD, 0xBE, 0xEF}, pkt.Payload)
}

func TestRoundTrip(t *testing.T) {
	forward := newInterceptor(t, WithMappings(
		Mapping{FromID: 1, ToID: 10},
		Mapping{FromID: 2, ToID: 14},
	))
	backward := newInterceptor(t, WithMappings(
		Mapping{FromID: 10, ToID: 1},
		Mapping{FromID: 14, ToID: 2},
	))

	mapped := readOnce(t, forward, &interceptor.StreamInfo{}, fixedPacket)
	require.Equal(t, fixedPacketMapped, mapped)

	restored := readOnce(t, backward, &interceptor.StreamInfo{}, mapped)
	assert.Equal(t, fixedPacket, restored)
}

func TestURIBasedMapping(t *testing.T) {
	ic := newInterceptor(t, WithMappings(
		Mapping{URI: audioLevelURI},
		Mapping{URI: toffsetURI},
	))

	remote := &interceptor.StreamInfo{RTPHeaderExtensions: []interceptor.RTPHeaderExtension{
		{URI: audioLevelURI, ID: 1},
		{URI: toffsetURI, ID: 2},
	}}
	local := &interceptor.StreamInfo{RTPHeaderExtensions: []interceptor.RTPHeaderExtension{
		{URI: audioLevelURI, ID: 10},
		{URI: toffsetURI, ID: 14},
	}}

	// The local negotiation is not known yet: nothing is remapped.
	out := readOnce(t, ic, remote, fixedPacket)
	assert.Equal(t, fixedPacket, out)

	// Once the local stream is bound, URI mappings resolve.
	ic.BindLocalStream(local, interceptor.RTPWriterFunc(
		func(header *rtp.Header, payload []byte, attributes interceptor.Attributes) (int, error) {
			return 0, nil
		}))

	out = readOnce(t, ic, remote, fixedPacket)
	assert.Equal(t, fixedPacketMapped, out)
}

func TestMixedURIAndExplicitID(t *testing.T) {
	// Inbound ID explicit, outbound ID resolved from the local stream by URI.
	ic := newInterceptor(t, WithMappings(
		Mapping{URI: audioLevelURI, FromID: 1},
		Mapping{FromID: 2, ToID: 14},
	))

	ic.BindLocalStream(&interceptor.StreamInfo{RTPHeaderExtensions: []interceptor.RTPHeaderExtension{
		{URI: audioLevelURI, ID: 10},
	}}, interceptor.RTPWriterFunc(
		func(header *rtp.Header, payload []byte, attributes interceptor.Attributes) (int, error) {
			return 0, nil
		}))

	out := readOnce(t, ic, &interceptor.StreamInfo{}, fixedPacket)
	assert.Equal(t, fixedPacketMapped, out)
}

func TestOneByteToTwoByteUpgrade(t *testing.T) {
	// ID 15 cannot be represented in the one-byte profile: the packet must be
	// upgraded to the two-byte profile, values preserved.
	ic := newInterceptor(t, WithMappings(
		Mapping{FromID: 1, ToID: 10},
		Mapping{FromID: 2, ToID: 15},
	))

	out := readOnce(t, ic, &interceptor.StreamInfo{}, fixedPacket)

	expected := []byte{
		0x90, 0xE0, 0x01, 0x02,
		0x0A, 0x0B, 0x0C, 0x0D,
		0x11, 0x22, 0x33, 0x44,
		0x10, 0x00, 0x00, 0x02, // two-byte profile, 8 bytes of extensions
		0x0A, 0x01, 0xAB, // id=10, len=1
		0x0F, 0x02, 0x01, 0x02, // id=15, len=2
		0x00,                   // extension padding
		0xDE, 0xAD, 0xBE, 0xEF, // payload
	}
	assert.Equal(t, expected, out)

	pkt := &rtp.Packet{}
	require.NoError(t, pkt.Unmarshal(out))
	assert.Equal(t, uint16(rtp.ExtensionProfileTwoByte), pkt.Header.ExtensionProfile)
	assert.Equal(t, []byte{0xAB}, pkt.Header.GetExtension(10))
	assert.Equal(t, []byte{0x01, 0x02}, pkt.Header.GetExtension(15))
}

// twoBytePacket carries audio-level on ID 20 and transport-cc on ID 21 in the
// RFC 8285 two-byte profile.
var twoBytePacket = []byte{
	0x90, 0xE0, 0x01, 0x02,
	0x0A, 0x0B, 0x0C, 0x0D,
	0x11, 0x22, 0x33, 0x44,
	0x10, 0x00, 0x00, 0x02, // two-byte profile, 8 bytes of extensions
	0x14, 0x01, 0xAB, // id=20, len=1
	0x15, 0x02, 0x01, 0x02, // id=21, len=2
	0x00,                   // padding
	0xDE, 0xAD, 0xBE, 0xEF, // payload
}

func TestTwoByteSourceStaysTwoByte(t *testing.T) {
	// Mapping to IDs that would fit the one-byte profile must not downgrade.
	ic := newInterceptor(t, WithMappings(
		Mapping{FromID: 20, ToID: 5},
		Mapping{FromID: 21, ToID: 6},
	))

	out := readOnce(t, ic, &interceptor.StreamInfo{}, twoBytePacket)

	expected := []byte{
		0x90, 0xE0, 0x01, 0x02,
		0x0A, 0x0B, 0x0C, 0x0D,
		0x11, 0x22, 0x33, 0x44,
		0x10, 0x00, 0x00, 0x02,
		0x05, 0x01, 0xAB,
		0x06, 0x02, 0x01, 0x02,
		0x00,
		0xDE, 0xAD, 0xBE, 0xEF,
	}
	assert.Equal(t, expected, out)
}

func TestTwoByteToTwoByte(t *testing.T) {
	ic := newInterceptor(t, WithMappings(
		Mapping{FromID: 20, ToID: 30},
	))

	out := readOnce(t, ic, &interceptor.StreamInfo{}, twoBytePacket)

	pkt := &rtp.Packet{}
	require.NoError(t, pkt.Unmarshal(out))
	assert.Equal(t, uint16(rtp.ExtensionProfileTwoByte), pkt.Header.ExtensionProfile)
	assert.Equal(t, []byte{0xAB}, pkt.Header.GetExtension(30))
	assert.Equal(t, []byte{0x01, 0x02}, pkt.Header.GetExtension(21)) // unmapped, preserved
	assert.Nil(t, pkt.Header.GetExtension(20))
}

func TestUnmappedExtensionPreserved(t *testing.T) {
	// Packet with an extra unmapped extension on ID 3.
	pkt := []byte{
		0x90, 0xE0, 0x01, 0x02,
		0x0A, 0x0B, 0x0C, 0x0D,
		0x11, 0x22, 0x33, 0x44,
		0xBE, 0xDE, 0x00, 0x02, // 8 bytes of extensions
		0x10, 0xAB, // id=1
		0x21, 0x01, 0x02, // id=2
		0x30, 0x99, // id=3, unmapped
		0x00, // extension padding
		0xDE, 0xAD, 0xBE, 0xEF,
	}

	ic := newInterceptor(t, WithMappings(
		Mapping{FromID: 1, ToID: 10},
		Mapping{FromID: 2, ToID: 14},
	))

	out := readOnce(t, ic, &interceptor.StreamInfo{}, pkt)

	parsed := &rtp.Packet{}
	require.NoError(t, parsed.Unmarshal(out))
	assert.Equal(t, []byte{0xAB}, parsed.Header.GetExtension(10))
	assert.Equal(t, []byte{0x01, 0x02}, parsed.Header.GetExtension(14))
	assert.Equal(t, []byte{0x99}, parsed.Header.GetExtension(3))
	assert.Equal(t, []uint8{10, 14, 3}, parsed.Header.GetExtensionIDs())
}

func TestEmptyExtensionList(t *testing.T) {
	pkt := []byte{
		0x90, 0xE0, 0x01, 0x02,
		0x0A, 0x0B, 0x0C, 0x0D,
		0x11, 0x22, 0x33, 0x44,
		0xBE, 0xDE, 0x00, 0x00, // extension bit set, zero-length extension block
		0xDE, 0xAD, 0xBE, 0xEF,
	}

	ic := newInterceptor(t, WithMappings(Mapping{FromID: 1, ToID: 10}))
	assert.Equal(t, pkt, readOnce(t, ic, &interceptor.StreamInfo{}, pkt))
}

func TestNoExtensionBit(t *testing.T) {
	pkt := []byte{
		0x80, 0xE0, 0x01, 0x02,
		0x0A, 0x0B, 0x0C, 0x0D,
		0x11, 0x22, 0x33, 0x44,
		0xDE, 0xAD, 0xBE, 0xEF,
	}

	ic := newInterceptor(t, WithMappings(Mapping{FromID: 1, ToID: 10}))
	assert.Equal(t, pkt, readOnce(t, ic, &interceptor.StreamInfo{}, pkt))
}

func TestEncryptedExtensionProfileUntouched(t *testing.T) {
	// Cryptex (RFC 9335) one-byte profile: the extension block is encrypted
	// and must be forwarded unchanged.
	for _, profile := range []byte{0xC0, 0xC2} {
		pkt := []byte{
			0x90, 0xE0, 0x01, 0x02,
			0x0A, 0x0B, 0x0C, 0x0D,
			0x11, 0x22, 0x33, 0x44,
			profile, 0xDE, 0x00, 0x01,
			0x01, 0x02, 0x03, 0x04, // opaque (encrypted) extension block
			0xDE, 0xAD, 0xBE, 0xEF,
		}

		ic := newInterceptor(t, WithMappings(Mapping{FromID: 1, ToID: 10}))
		assert.Equal(t, pkt, readOnce(t, ic, &interceptor.StreamInfo{}, pkt))
	}
}

func TestShortAndMalformedPackets(t *testing.T) {
	ic := newInterceptor(t, WithMappings(Mapping{FromID: 1, ToID: 10}))

	// Shorter than an RTP header.
	short := []byte{0x90, 0xE0, 0x01}
	assert.Equal(t, short, readOnce(t, ic, &interceptor.StreamInfo{}, short))

	// Extension bit set but the extension block is truncated.
	truncated := []byte{
		0x90, 0xE0, 0x01, 0x02,
		0x0A, 0x0B, 0x0C, 0x0D,
		0x11, 0x22, 0x33, 0x44,
		0xBE, 0xDE, 0x00, 0x04, // declares 16 bytes, carries none
	}
	assert.Equal(t, truncated, readOnce(t, ic, &interceptor.StreamInfo{}, truncated))
}

func TestTargetCollisionWithUnmappedExtension(t *testing.T) {
	// Mapping 1 -> 2, but the packet already carries an unmapped extension on
	// ID 2: the mapping is skipped for that extension, both are preserved.
	pkt := []byte{
		0x90, 0xE0, 0x01, 0x02,
		0x0A, 0x0B, 0x0C, 0x0D,
		0x11, 0x22, 0x33, 0x44,
		0xBE, 0xDE, 0x00, 0x02,
		0x10, 0xAB, // id=1
		0x20, 0x99, // id=2, unmapped
		0x00, 0x00, 0x00,
		0xDE, 0xAD, 0xBE, 0xEF,
	}

	ic := newInterceptor(t, WithMappings(Mapping{FromID: 1, ToID: 2}))
	out := readOnce(t, ic, &interceptor.StreamInfo{}, pkt)
	assert.Equal(t, pkt, out)
}

func TestMappingValidation(t *testing.T) {
	for _, tc := range []struct {
		name     string
		mappings []Mapping
		err      error
	}{
		{"duplicate URI", []Mapping{
			{URI: audioLevelURI},
			{URI: audioLevelURI},
		}, ErrDuplicateURI},
		{"conflicting source ID", []Mapping{
			{FromID: 1, ToID: 10},
			{FromID: 1, ToID: 11},
		}, ErrConflictingMapping},
		{"conflicting target ID", []Mapping{
			{FromID: 1, ToID: 10},
			{FromID: 2, ToID: 10},
		}, ErrConflictingMapping},
		{"zero source ID", []Mapping{
			{FromID: 0, ToID: 10},
		}, ErrInvalidID},
		{"zero target ID", []Mapping{
			{FromID: 1, ToID: 0},
		}, ErrInvalidID},
		{"valid explicit", []Mapping{
			{FromID: 1, ToID: 10},
			{FromID: 2, ToID: 14},
		}, nil},
		{"valid URI", []Mapping{
			{URI: audioLevelURI},
			{URI: transportCCURI, FromID: 3},
		}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			factory, err := NewInterceptor(WithMappings(tc.mappings...))
			require.NoError(t, err)
			_, err = factory.NewInterceptor("test")
			if tc.err == nil {
				assert.NoError(t, err)
			} else {
				assert.ErrorIs(t, err, tc.err)
			}
		})
	}
}

func TestRuntimeReplacement(t *testing.T) {
	ic := newInterceptor(t, WithMappings(Mapping{FromID: 1, ToID: 10}))

	em, ok := ic.(*Interceptor)
	require.True(t, ok)

	// Invalid replacement is rejected and the old table stays in effect.
	require.ErrorIs(t, em.SetMappings(Mapping{URI: audioLevelURI}, Mapping{URI: audioLevelURI}), ErrDuplicateURI)
	assert.Equal(t, []Mapping{{FromID: 1, ToID: 10}}, em.Mappings())

	require.NoError(t, em.SetMappings(Mapping{FromID: 1, ToID: 12}, Mapping{FromID: 2, ToID: 13}))

	out := readOnce(t, ic, &interceptor.StreamInfo{}, fixedPacket)
	pkt := &rtp.Packet{}
	require.NoError(t, pkt.Unmarshal(out))
	assert.Equal(t, []byte{0xAB}, pkt.Header.GetExtension(12))
	assert.Equal(t, []byte{0x01, 0x02}, pkt.Header.GetExtension(13))

	// Clearing the table disables remapping.
	require.NoError(t, em.SetMappings())
	assert.Equal(t, fixedPacket, readOnce(t, ic, &interceptor.StreamInfo{}, fixedPacket))
}

func TestWritePath(t *testing.T) {
	ic := newInterceptor(t, WithMappings(
		Mapping{FromID: 1, ToID: 10},
		Mapping{FromID: 2, ToID: 14},
	))

	var got *rtp.Header
	var gotPayload []byte
	writer := ic.BindLocalStream(&interceptor.StreamInfo{}, interceptor.RTPWriterFunc(
		func(header *rtp.Header, payload []byte, attributes interceptor.Attributes) (int, error) {
			got = header
			gotPayload = payload

			return header.MarshalSize() + len(payload), nil
		}))

	header := &rtp.Header{
		Version:          2,
		Extension:        true,
		ExtensionProfile: rtp.ExtensionProfileOneByte,
		SequenceNumber:   0x0102,
		Timestamp:        0x0A0B0C0D,
		SSRC:             0x11223344,
	}
	require.NoError(t, header.SetExtension(1, []byte{0xAB}))
	require.NoError(t, header.SetExtension(2, []byte{0x01, 0x02}))

	_, err := writer.Write(header, []byte{0xDE, 0xAD, 0xBE, 0xEF}, nil)
	require.NoError(t, err)

	assert.Equal(t, []byte{0xAB}, got.GetExtension(10))
	assert.Equal(t, []byte{0x01, 0x02}, got.GetExtension(14))
	assert.Nil(t, got.GetExtension(1))
	assert.Equal(t, []byte{0xDE, 0xAD, 0xBE, 0xEF}, gotPayload)
	assert.Equal(t, uint32(0x11223344), got.SSRC)
	assert.Equal(t, uint32(0x0A0B0C0D), got.Timestamp)
}

func TestWritePathURIResolution(t *testing.T) {
	ic := newInterceptor(t, WithMappings(Mapping{URI: audioLevelURI}))

	ic.BindRemoteStream(&interceptor.StreamInfo{RTPHeaderExtensions: []interceptor.RTPHeaderExtension{
		{URI: audioLevelURI, ID: 1},
	}}, interceptor.RTPReaderFunc(
		func(b []byte, a interceptor.Attributes) (int, interceptor.Attributes, error) {
			return 0, a, nil
		}))

	var got *rtp.Header
	writer := ic.BindLocalStream(&interceptor.StreamInfo{RTPHeaderExtensions: []interceptor.RTPHeaderExtension{
		{URI: audioLevelURI, ID: 10},
	}}, interceptor.RTPWriterFunc(
		func(header *rtp.Header, payload []byte, attributes interceptor.Attributes) (int, error) {
			got = header

			return 0, nil
		}))

	header := &rtp.Header{Version: 2, Extension: true, ExtensionProfile: rtp.ExtensionProfileOneByte}
	require.NoError(t, header.SetExtension(1, []byte{0xAB}))
	_, err := writer.Write(header, nil, nil)
	require.NoError(t, err)
	assert.Equal(t, []byte{0xAB}, got.GetExtension(10))
}

func TestConcurrentStreamsAndRuntimeReplacement(t *testing.T) {
	ic := newInterceptor(t, WithMappings(Mapping{FromID: 1, ToID: 10}, Mapping{FromID: 2, ToID: 14}))
	em := ic.(*Interceptor) //nolint:errcheck // TestRuntimeReplacement proves the type

	const streams = 8
	const packetsPerStream = 50

	var wg sync.WaitGroup
	for s := 0; s < streams; s++ {
		reader := ic.BindRemoteStream(&interceptor.StreamInfo{
			SSRC: uint32(s), //nolint:gosec // test data
			RTPHeaderExtensions: []interceptor.RTPHeaderExtension{
				{URI: audioLevelURI, ID: 1},
			},
		}, interceptor.RTPReaderFunc(
			func(b []byte, a interceptor.Attributes) (int, interceptor.Attributes, error) {
				return copy(b, fixedPacket), a, nil
			}))

		wg.Add(1)
		go func() {
			defer wg.Done()
			buf := make([]byte, 1500)
			for p := 0; p < packetsPerStream; p++ {
				n, _, err := reader.Read(buf, nil)
				if !assert.NoError(t, err) {
					return
				}
				pkt := &rtp.Packet{}
				if !assert.NoError(t, pkt.Unmarshal(buf[:n])) {
					return
				}
				// Whichever table was active, every packet is fully mapped.
				ids := pkt.Header.GetExtensionIDs()
				for _, id := range ids {
					assert.NotContains(t, []uint8{1, 2}, id)
				}
				assert.Len(t, ids, 2)
			}
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for r := 0; r < 20; r++ {
			_ = em.SetMappings(Mapping{FromID: 1, ToID: 11}, Mapping{FromID: 2, ToID: 12})
			_ = em.SetMappings(Mapping{FromID: 1, ToID: 10}, Mapping{FromID: 2, ToID: 14})
			_ = em.Mappings()
		}
	}()

	wg.Wait()
}

func TestInterceptorChainCompatibility(t *testing.T) {
	ic := newInterceptor(t, WithMappings(Mapping{FromID: 1, ToID: 10}, Mapping{FromID: 2, ToID: 14}))

	// The interceptor must compose with others in an interceptor.Chain in
	// either position.
	for _, chain := range []*interceptor.Chain{
		interceptor.NewChain([]interceptor.Interceptor{ic, &mock.Interceptor{}}),
		interceptor.NewChain([]interceptor.Interceptor{&mock.Interceptor{}, ic}),
	} {
		out := readOnce(t, chain, &interceptor.StreamInfo{}, fixedPacket)
		assert.Equal(t, fixedPacketMapped, out)
	}

	// RTCP paths are passed through untouched.
	rtcpReader := &noopRTCPReader{}
	assert.Same(t, rtcpReader, ic.BindRTCPReader(rtcpReader))
}

type noopRTCPReader struct{}

func (n *noopRTCPReader) Read(b []byte, a interceptor.Attributes) (int, interceptor.Attributes, error) {
	return 0, a, nil
}
