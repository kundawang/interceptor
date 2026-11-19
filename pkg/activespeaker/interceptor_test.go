// SPDX-FileCopyrightText: 2026 The Pion community <https://pion.ly>
// SPDX-License-Identifier: MIT

package activespeaker

import (
	"io"
	"sync"
	"testing"
	"time"

	"github.com/pion/interceptor"
	"github.com/pion/interceptor/internal/test"
	"github.com/pion/interceptor/pkg/mock"
	"github.com/pion/rtp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testExtID     = 7
	testClockRate = 48000
	// ticksPer20ms is the RTP timestamp step of 20ms of 48kHz audio.
	ticksPer20ms = 960
)

func testBaseTime() time.Time {
	return time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
}

type harness struct {
	interceptor *Interceptor
	now         *test.MockTime
	ticker      *test.MockTicker

	mu     sync.Mutex
	events []Event
}

func newHarness(t *testing.T, opts ...Option) *harness {
	t.Helper()
	hr := &harness{
		now:    &test.MockTime{},
		ticker: &test.MockTicker{C: make(chan time.Time, 100)},
	}
	hr.now.SetNow(testBaseTime())
	opts = append(opts,
		WithNowFunc(hr.now.Now),
		WithTicker(hr.ticker),
		WithEventHandler(hr.record),
	)
	factory, err := NewInterceptor(opts...)
	require.NoError(t, err)
	ic, err := factory.NewInterceptor("")
	require.NoError(t, err)
	var ok bool
	hr.interceptor, ok = ic.(*Interceptor)
	require.True(t, ok)
	t.Cleanup(func() {
		assert.NoError(t, hr.interceptor.Close())
	})

	return hr
}

func (hr *harness) record(ev Event) {
	hr.mu.Lock()
	defer hr.mu.Unlock()
	hr.events = append(hr.events, ev)
}

func (hr *harness) recordedEvents() []Event {
	hr.mu.Lock()
	defer hr.mu.Unlock()

	return append([]Event(nil), hr.events...)
}

// bind registers a remote stream and returns functions to synchronously feed
// it RTP packets (processed at the current mock time).
func (hr *harness) bind(t *testing.T, info *interceptor.StreamInfo) (send func(*rtp.Packet), sendRaw func([]byte)) {
	t.Helper()
	var queue [][]byte
	reader := hr.interceptor.BindRemoteStream(info, interceptor.RTPReaderFunc(
		func(b []byte, a interceptor.Attributes) (int, interceptor.Attributes, error) {
			if len(queue) == 0 {
				return 0, nil, io.EOF
			}
			n := copy(b, queue[0])
			queue = queue[1:]

			return n, a, nil
		}))
	sendRaw = func(raw []byte) {
		queue = append(queue, raw)
		buf := make([]byte, 1500)
		_, _, err := reader.Read(buf, interceptor.Attributes{})
		require.NoError(t, err)
	}
	send = func(p *rtp.Packet) {
		raw, err := p.Marshal()
		require.NoError(t, err)
		sendRaw(raw)
	}

	return send, sendRaw
}

// sendBurst sends count packets 20ms of media apart, advancing the mock
// clock by 20ms after each packet.
func (hr *harness) sendBurst(
	t *testing.T, send func(*rtp.Packet), ssrc uint32, seq *uint16, ts *uint32, level uint8, count int,
) {
	t.Helper()
	for range count {
		send(levelPacket(t, ssrc, *seq, *ts, level, true))
		(*seq)++
		*ts += ticksPer20ms
		hr.now.SetNow(hr.now.Now().Add(20 * time.Millisecond))
	}
}

func audioStreamInfo(ssrc uint32) *interceptor.StreamInfo {
	return &interceptor.StreamInfo{
		SSRC:      ssrc,
		ClockRate: testClockRate,
		RTPHeaderExtensions: []interceptor.RTPHeaderExtension{
			{URI: AudioLevelURI, ID: testExtID},
		},
	}
}

func levelPacket(t *testing.T, ssrc uint32, seq uint16, ts uint32, level uint8, voice bool) *rtp.Packet {
	t.Helper()
	payload, err := (rtp.AudioLevelExtension{Level: level, Voice: voice}).Marshal()
	require.NoError(t, err)
	header := rtp.Header{
		Version:        2,
		PayloadType:    111,
		SequenceNumber: seq,
		Timestamp:      ts,
		SSRC:           ssrc,
	}
	require.NoError(t, header.SetExtension(testExtID, payload))

	return &rtp.Packet{Header: header, Payload: []byte{0x00}}
}

// TestTwoAlternatingSpeakers feeds a fixed sequence where speaker A talks
// for 1s, stops, and speaker B takes over. The highlight must not jump
// during A's short pause: B only becomes active once A's silence timeout
// expires.
func TestTwoAlternatingSpeakers(t *testing.T) {
	hr := newHarness(t)
	sendA, _ := hr.bind(t, audioStreamInfo(1))
	sendB, _ := hr.bind(t, audioStreamInfo(2))

	t0 := testBaseTime()
	seqA, seqB := uint16(0), uint16(0)
	tsA, tsB := uint32(0), uint32(0)

	// A speaks alone for 1s (50 packets of 20ms).
	hr.sendBurst(t, sendA, 1, &seqA, &tsA, 30, 50)
	require.Equal(t, []Event{{Type: EventEnter, SSRC: 1, At: t0}}, hr.recordedEvents())
	active, ok := hr.interceptor.ActiveSpeaker()
	require.True(t, ok)
	require.Equal(t, uint32(1), active)

	// A stops, B speaks for 1.2s, equally loud. A keeps the role until its
	// silence timeout (1s after A's last packet at t0+980ms) expires.
	hr.sendBurst(t, sendB, 2, &seqB, &tsB, 30, 60)

	want := []Event{
		{Type: EventEnter, SSRC: 1, At: t0},
		{Type: EventExit, SSRC: 1, At: t0.Add(2 * time.Second)},
		{Type: EventEnter, SSRC: 2, At: t0.Add(2 * time.Second)},
	}
	require.Equal(t, want, hr.recordedEvents())
	active, ok = hr.interceptor.ActiveSpeaker()
	require.True(t, ok)
	require.Equal(t, uint32(2), active)
}

// TestNoiseSpike verifies that a single loud packet among quiet ones does
// not cross the smoothed active threshold, while sustained speech does.
func TestNoiseSpike(t *testing.T) {
	hr := newHarness(t)
	send, _ := hr.bind(t, audioStreamInfo(1))

	t0 := testBaseTime()
	seq, ts := uint16(0), uint32(0)

	// Quiet background (level 100), one loud spike (level 5) in the middle.
	hr.sendBurst(t, send, 1, &seq, &ts, 100, 15)
	hr.sendBurst(t, send, 1, &seq, &ts, 5, 1)
	hr.sendBurst(t, send, 1, &seq, &ts, 100, 15)
	require.Empty(t, hr.recordedEvents())
	_, ok := hr.interceptor.ActiveSpeaker()
	require.False(t, ok)

	// Sustained loud speech crosses the threshold once the window fills.
	hr.sendBurst(t, send, 1, &seq, &ts, 20, 15)
	want := []Event{{Type: EventEnter, SSRC: 1, At: t0.Add(840 * time.Millisecond)}}
	require.Equal(t, want, hr.recordedEvents())
	active, ok := hr.interceptor.ActiveSpeaker()
	require.True(t, ok)
	require.Equal(t, uint32(1), active)
}

// TestSilenceTimeout verifies that a speaker that stops sending packets is
// exited after the silence timeout, leaving no active speaker.
func TestSilenceTimeout(t *testing.T) {
	hr := newHarness(t)
	send, _ := hr.bind(t, audioStreamInfo(1))

	t0 := testBaseTime()
	seq, ts := uint16(0), uint32(0)
	hr.sendBurst(t, send, 1, &seq, &ts, 30, 20)
	require.Equal(t, []Event{{Type: EventEnter, SSRC: 1, At: t0}}, hr.recordedEvents())

	// A's last packet was at t0+380ms; advance past the 1s silence timeout.
	silentAt := t0.Add(2 * time.Second)
	hr.now.SetNow(silentAt)
	hr.interceptor.evaluate(silentAt)

	want := []Event{
		{Type: EventEnter, SSRC: 1, At: t0},
		{Type: EventExit, SSRC: 1, At: silentAt},
	}
	require.Equal(t, want, hr.recordedEvents())
	_, ok := hr.interceptor.ActiveSpeaker()
	require.False(t, ok)
}

// TestTickerDrivesEvaluation verifies that the periodic ticker triggers
// evaluation (and thus silence detection) when no packets arrive.
func TestTickerDrivesEvaluation(t *testing.T) {
	hr := newHarness(t)
	send, _ := hr.bind(t, audioStreamInfo(1))

	t0 := testBaseTime()
	seq, ts := uint16(0), uint32(0)
	hr.sendBurst(t, send, 1, &seq, &ts, 30, 20)

	silentAt := t0.Add(2 * time.Second)
	hr.now.SetNow(silentAt)
	hr.ticker.Tick(silentAt)

	assert.Eventually(t, func() bool {
		events := hr.recordedEvents()

		return len(events) == 2 && events[1] == Event{Type: EventExit, SSRC: 1, At: silentAt}
	}, time.Second, 5*time.Millisecond)
}

// TestAllMuted verifies that when every stream goes silent there is no
// active speaker.
func TestAllMuted(t *testing.T) {
	hr := newHarness(t)
	sendA, _ := hr.bind(t, audioStreamInfo(1))
	sendB, _ := hr.bind(t, audioStreamInfo(2))

	t0 := testBaseTime()
	seqA, seqB := uint16(0), uint16(0)
	tsA, tsB := uint32(0), uint32(0)
	hr.sendBurst(t, sendA, 1, &seqA, &tsA, 30, 20)
	hr.sendBurst(t, sendB, 2, &seqB, &tsB, 30, 20)
	require.Equal(t, []Event{{Type: EventEnter, SSRC: 1, At: t0}}, hr.recordedEvents())

	silentAt := t0.Add(3 * time.Second)
	hr.now.SetNow(silentAt)
	hr.interceptor.evaluate(silentAt)

	want := []Event{
		{Type: EventEnter, SSRC: 1, At: t0},
		{Type: EventExit, SSRC: 1, At: silentAt},
	}
	require.Equal(t, want, hr.recordedEvents())
	_, ok := hr.interceptor.ActiveSpeaker()
	require.False(t, ok)
}

// TestMissingExtension verifies that streams without a negotiated audio
// level extension, and packets not carrying it, never become active.
func TestMissingExtension(t *testing.T) {
	hr := newHarness(t)

	// Stream without the negotiated extension.
	infoNoExt := &interceptor.StreamInfo{SSRC: 1, ClockRate: testClockRate}
	sendNoExt, _ := hr.bind(t, infoNoExt)
	// Stream with the extension negotiated.
	sendExt, _ := hr.bind(t, audioStreamInfo(2))

	seq, ts := uint16(0), uint32(0)
	hr.sendBurst(t, sendNoExt, 1, &seq, &ts, 10, 20)

	// Packets without the extension payload on a negotiated stream.
	for range 20 {
		sendExt(&rtp.Packet{Header: rtp.Header{
			Version: 2, PayloadType: 111, SequenceNumber: seq, Timestamp: ts, SSRC: 2,
		}, Payload: []byte{0x00}})
		seq++
		ts += ticksPer20ms
	}

	require.Empty(t, hr.recordedEvents())
	_, ok := hr.interceptor.ActiveSpeaker()
	require.False(t, ok)
}

// TestInvalidLevelPayload verifies that a packet whose audio level extension
// payload cannot be parsed is ignored for speaker detection.
func TestInvalidLevelPayload(t *testing.T) {
	hr := newHarness(t)
	_, sendRaw := hr.bind(t, audioStreamInfo(1))

	// Two-byte header extension profile, extension ID 7 with zero-length
	// (unparseable) payload.
	raw := []byte{
		0x90, 111, // V=2, X=1, PT=111
		0x00, 0x01, // sequence number
		0x00, 0x00, 0x00, 0x00, // timestamp
		0x00, 0x00, 0x00, 0x01, // SSRC
		0x10, 0x00, // two-byte extension profile
		0x00, 0x01, // extension length: one 32-bit word
		0x07, 0x00, 0x00, 0x00, // ID 7, length 0, padding
		0x00, // RTP payload
	}
	sendRaw(raw)

	require.Empty(t, hr.recordedEvents())
	_, ok := hr.interceptor.ActiveSpeaker()
	require.False(t, ok)
}

// TestReorderingDeterministic verifies that delivering the same packets in a
// different order yields the same events and final state, because samples
// are windowed by RTP timestamp.
func TestReorderingDeterministic(t *testing.T) {
	type result struct {
		events    []Event
		active    uint32
		hasActive bool
		speaking  bool
		avg       float64
	}
	const packetCount = 30
	run := func(order []int) result {
		hr := newHarness(t)
		send, _ := hr.bind(t, audioStreamInfo(1))
		pkts := make([]*rtp.Packet, 0, packetCount)
		for i := range packetCount {
			pkts = append(pkts, levelPacket(t, 1, uint16(i), uint32(i)*ticksPer20ms, 30, true)) //nolint:gosec
		}
		for _, idx := range order {
			send(pkts[idx])
			hr.now.SetNow(hr.now.Now().Add(20 * time.Millisecond))
		}
		st := hr.interceptor.streams[1]
		avg, _ := st.averageLevel()
		active, hasActive := hr.interceptor.ActiveSpeaker()

		return result{
			events:    hr.recordedEvents(),
			active:    active,
			hasActive: hasActive,
			speaking:  st.speaking,
			avg:       avg,
		}
	}

	forward := make([]int, packetCount)
	reverse := make([]int, packetCount)
	shuffled := make([]int, 0, packetCount)
	for i := range packetCount {
		forward[i] = i
		reverse[i] = packetCount - 1 - i
	}
	for i := 1; i < packetCount; i += 2 {
		shuffled = append(shuffled, i)
	}
	for i := 0; i < packetCount; i += 2 {
		shuffled = append(shuffled, i)
	}

	want := run(forward)
	require.Equal(t, want, run(reverse))
	require.Equal(t, want, run(shuffled))
}

// TestTimestampRollback verifies that an RTP timestamp far in the past
// (e.g. a restarted stream reusing the SSRC) resets the smoothing window.
func TestTimestampRollback(t *testing.T) {
	hr := newHarness(t)
	send, _ := hr.bind(t, audioStreamInfo(1))

	t0 := testBaseTime()
	seq, ts := uint16(0), uint32(0)
	hr.sendBurst(t, send, 1, &seq, &ts, 30, 20)
	require.Equal(t, []Event{{Type: EventEnter, SSRC: 1, At: t0}}, hr.recordedEvents())

	// The stream restarts with timestamps far in the past. The window is
	// reset, so this single quiet packet drops the smoothed level below the
	// release threshold and the speaker exits.
	send(levelPacket(t, 1, seq, 1000, 100, true))
	seq++
	exitAt := t0.Add(400 * time.Millisecond)
	want := []Event{
		{Type: EventEnter, SSRC: 1, At: t0},
		{Type: EventExit, SSRC: 1, At: exitAt},
	}
	require.Equal(t, want, hr.recordedEvents())

	// The restarted stream can become active again.
	hr.sendBurst(t, send, 1, &seq, &ts, 30, 1)
	want = append(want, Event{Type: EventEnter, SSRC: 1, At: exitAt})
	require.Equal(t, want, hr.recordedEvents())
}

// TestSSRCRebind verifies that binding a new stream for an SSRC that is
// still registered drops the stale state (SSRC reuse).
func TestSSRCRebind(t *testing.T) {
	hr := newHarness(t)
	send, _ := hr.bind(t, audioStreamInfo(1))

	t0 := testBaseTime()
	seq, ts := uint16(0), uint32(0)
	hr.sendBurst(t, send, 1, &seq, &ts, 30, 20)
	require.Equal(t, []Event{{Type: EventEnter, SSRC: 1, At: t0}}, hr.recordedEvents())

	// Rebinding the same SSRC exits the stale active speaker immediately.
	rebindAt := t0.Add(400 * time.Millisecond)
	sendNew, _ := hr.bind(t, audioStreamInfo(1))
	want := []Event{
		{Type: EventEnter, SSRC: 1, At: t0},
		{Type: EventExit, SSRC: 1, At: rebindAt},
	}
	require.Equal(t, want, hr.recordedEvents())

	// The new stream starts fresh and can become active again.
	seqNew, tsNew := uint16(0), uint32(0)
	hr.sendBurst(t, sendNew, 1, &seqNew, &tsNew, 30, 1)
	want = append(want, Event{Type: EventEnter, SSRC: 1, At: rebindAt})
	require.Equal(t, want, hr.recordedEvents())
}

// TestUnbind verifies that unbinding the active stream emits an exit event
// and promotes the loudest remaining speaker.
func TestUnbind(t *testing.T) {
	hr := newHarness(t)
	infoA := audioStreamInfo(1)
	infoB := audioStreamInfo(2)
	sendA, _ := hr.bind(t, infoA)
	sendB, _ := hr.bind(t, infoB)

	t0 := testBaseTime()
	seqA, seqB := uint16(0), uint16(0)
	tsA, tsB := uint32(0), uint32(0)
	hr.sendBurst(t, sendA, 1, &seqA, &tsA, 30, 20)
	hr.sendBurst(t, sendB, 2, &seqB, &tsB, 30, 20)

	unbindAt := t0.Add(800 * time.Millisecond)
	hr.interceptor.UnbindRemoteStream(infoA)
	want := []Event{
		{Type: EventEnter, SSRC: 1, At: t0},
		{Type: EventExit, SSRC: 1, At: unbindAt},
		{Type: EventEnter, SSRC: 2, At: unbindAt},
	}
	require.Equal(t, want, hr.recordedEvents())

	hr.interceptor.UnbindRemoteStream(infoB)
	want = append(want, Event{Type: EventExit, SSRC: 2, At: unbindAt})
	require.Equal(t, want, hr.recordedEvents())
	_, ok := hr.interceptor.ActiveSpeaker()
	require.False(t, ok)
}

// TestHangover verifies that a louder challenger cannot take over before the
// hangover of the current active speaker expires.
func TestHangover(t *testing.T) {
	hr := newHarness(t)
	sendA, _ := hr.bind(t, audioStreamInfo(1))
	sendB, _ := hr.bind(t, audioStreamInfo(2))

	t0 := testBaseTime()
	seqA, seqB := uint16(0), uint16(0)
	tsA, tsB := uint32(0), uint32(0)

	// A (level 30) becomes active at t0. B (louder, level 10) joins at
	// t0+200ms; the switch only happens once A's 500ms hangover expired.
	hr.sendBurst(t, sendA, 1, &seqA, &tsA, 30, 10)
	for range 40 {
		sendB(levelPacket(t, 2, seqB, tsB, 10, true))
		seqB++
		tsB += ticksPer20ms
		sendA(levelPacket(t, 1, seqA, tsA, 30, true))
		seqA++
		tsA += ticksPer20ms
		hr.now.SetNow(hr.now.Now().Add(20 * time.Millisecond))
	}

	want := []Event{
		{Type: EventEnter, SSRC: 1, At: t0},
		{Type: EventExit, SSRC: 1, At: t0.Add(500 * time.Millisecond)},
		{Type: EventEnter, SSRC: 2, At: t0.Add(500 * time.Millisecond)},
	}
	require.Equal(t, want, hr.recordedEvents())
}

// TestVoiceActivityRequired verifies that with the V bit required, packets
// without it never make a stream speak.
func TestVoiceActivityRequired(t *testing.T) {
	hr := newHarness(t, WithVoiceActivityRequired(true))
	send, _ := hr.bind(t, audioStreamInfo(1))

	t0 := testBaseTime()
	for i := range 20 {
		send(levelPacket(t, 1, uint16(i), uint32(i)*ticksPer20ms, 20, false)) //nolint:gosec
		hr.now.SetNow(hr.now.Now().Add(20 * time.Millisecond))
	}
	require.Empty(t, hr.recordedEvents())

	send(levelPacket(t, 1, 20, 20*ticksPer20ms, 20, true))
	require.Equal(t, []Event{{Type: EventEnter, SSRC: 1, At: t0.Add(400 * time.Millisecond)}}, hr.recordedEvents())
}

// TestConcurrentAccess exercises concurrent packet processing, evaluation
// and ActiveSpeaker reads. Run with -race.
func TestConcurrentAccess(t *testing.T) {
	hr := newHarness(t)
	send, _ := hr.bind(t, audioStreamInfo(1))
	pkts := make([]*rtp.Packet, 0, 100)
	for i := range 100 {
		pkts = append(pkts, levelPacket(t, 1, uint16(i), uint32(i)*ticksPer20ms, 30, true)) //nolint:gosec
	}

	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		defer wg.Done()
		for _, p := range pkts {
			send(p)
		}
	}()
	go func() {
		defer wg.Done()
		for range 100 {
			hr.interceptor.evaluate(hr.now.Now())
		}
	}()
	go func() {
		defer wg.Done()
		for range 100 {
			hr.interceptor.ActiveSpeaker() //nolint:dogsled
		}
	}()
	wg.Wait()
}

// TestChain verifies that the interceptor passes packets through unmodified
// when combined with other interceptors in a chain.
func TestChain(t *testing.T) {
	factory, err := NewInterceptor()
	require.NoError(t, err)
	speakerInterceptor, err := factory.NewInterceptor("")
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, speakerInterceptor.Close())
	})

	bound := false
	chain := interceptor.NewChain([]interceptor.Interceptor{
		speakerInterceptor,
		&mock.Interceptor{
			BindRemoteStreamFn: func(_ *interceptor.StreamInfo, reader interceptor.RTPReader) interceptor.RTPReader {
				bound = true

				return reader
			},
		},
	})

	stream := test.NewMockStream(audioStreamInfo(1), chain)
	defer func() {
		assert.NoError(t, stream.Close())
	}()

	pkt := levelPacket(t, 1, 1, 0, 30, true)
	stream.ReceiveRTP(pkt)
	select {
	case received := <-stream.ReadRTP():
		require.NoError(t, received.Err)
		require.Equal(t, pkt.Header.SSRC, received.Packet.Header.SSRC)
		require.Equal(t, pkt.Header.SequenceNumber, received.Packet.Header.SequenceNumber)
		require.Equal(t, pkt.Payload, received.Packet.Payload)
		ext := rtp.AudioLevelExtension{}
		require.NoError(t, ext.Unmarshal(received.Packet.Header.GetExtension(testExtID)))
		require.Equal(t, uint8(30), ext.Level)
		require.True(t, ext.Voice)
	case <-time.After(time.Second):
		require.FailNow(t, "expected to read packet through the chain")
	}
	require.True(t, bound)
}

// TestOptions verifies option validation.
func TestOptions(t *testing.T) {
	for _, tc := range []struct {
		name string
		opt  Option
		err  error
	}{
		{"active threshold out of range", WithActiveThreshold(200), ErrThresholdOutOfRange},
		{"release threshold out of range", WithReleaseThreshold(200), ErrThresholdOutOfRange},
		{"release below active", func(i *Interceptor) error {
			i.activeThreshold = 50
			i.releaseThreshold = 40

			return nil
		}, ErrReleaseBelowActive},
		{"zero window", WithWindowDuration(0), ErrInvalidDuration},
		{"zero silence timeout", WithSilenceTimeout(0), ErrInvalidDuration},
		{"negative hangover", WithHangover(-time.Second), ErrInvalidDuration},
		{"zero evaluation interval", WithEvaluationInterval(0), ErrInvalidDuration},
		{"zero extension ID", WithAudioLevelExtensionID(0), ErrInvalidExtensionID},
	} {
		t.Run(tc.name, func(t *testing.T) {
			factory, err := NewInterceptor(tc.opt)
			require.NoError(t, err)
			ic, err := factory.NewInterceptor("")
			if err == nil {
				assert.NoError(t, ic.Close())
			}
			require.ErrorIs(t, err, tc.err)
		})
	}
}

// TestExtensionIDOverride verifies that the negotiated extension ID can be
// overridden for all streams.
func TestExtensionIDOverride(t *testing.T) {
	hr := newHarness(t, WithAudioLevelExtensionID(testExtID))
	info := &interceptor.StreamInfo{SSRC: 1, ClockRate: testClockRate}
	send, _ := hr.bind(t, info)

	t0 := testBaseTime()
	seq, ts := uint16(0), uint32(0)
	hr.sendBurst(t, send, 1, &seq, &ts, 30, 1)
	require.Equal(t, []Event{{Type: EventEnter, SSRC: 1, At: t0}}, hr.recordedEvents())
}

func TestEventTypeString(t *testing.T) {
	require.Equal(t, "Enter", EventEnter.String())
	require.Equal(t, "Exit", EventExit.String())
	require.Equal(t, "Unknown", EventType(-1).String())
}
