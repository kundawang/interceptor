// SPDX-FileCopyrightText: 2026 The Pion community <https://pion.ly>
// SPDX-License-Identifier: MIT

package liveliness

import (
	"sync"
	"testing"
	"time"

	"github.com/pion/interceptor"
	"github.com/pion/rtcp"
	"github.com/pion/rtp"
	"github.com/stretchr/testify/require"
)

// virtualClock is a manually advanced clock for deterministic tests.
type virtualClock struct {
	mu  sync.Mutex
	now time.Time
}

func newVirtualClock() *virtualClock {
	return &virtualClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
}

func (c *virtualClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.now
}

func (c *virtualClock) Set(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = t
}

func (c *virtualClock) Advance(d time.Duration) time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)

	return c.now
}

// eventLog collects events emitted by the interceptor.
type eventLog struct {
	mu     sync.Mutex
	events []Event
}

func (l *eventLog) add(e Event) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.events = append(l.events, e)
}

func (l *eventLog) all() []Event {
	l.mu.Lock()
	defer l.mu.Unlock()

	return append([]Event(nil), l.events...)
}

// typesFor returns the event types recorded for one SSRC, in order.
func (l *eventLog) typesFor(ssrc uint32) []EventType {
	l.mu.Lock()
	defer l.mu.Unlock()
	var types []EventType
	for _, e := range l.events {
		if e.SSRC == ssrc {
			types = append(types, e.Type)
		}
	}

	return types
}

func (l *eventLog) count() int {
	l.mu.Lock()
	defer l.mu.Unlock()

	return len(l.events)
}

// fixture wires an interceptor to a virtual clock and an event log. The
// check interval is set to an hour so the internal ticker never fires and
// tests drive check() explicitly.
type fixture struct {
	interceptor *Interceptor
	clock       *virtualClock
	events      *eventLog
}

func newFixture(t *testing.T, opts ...Option) *fixture {
	t.Helper()
	clock := newVirtualClock()
	events := &eventLog{}
	factory, err := NewInterceptor(append([]Option{
		WithSilenceTimeout(10 * time.Second),
		WithEndTimeout(30 * time.Second),
		WithCheckInterval(time.Hour),
		WithNowFunc(clock.Now),
		OnEvent(events.add),
	}, opts...)...)
	require.NoError(t, err)
	ic, err := factory.NewInterceptor("")
	require.NoError(t, err)
	inter, ok := ic.(*Interceptor)
	require.True(t, ok)
	t.Cleanup(func() { require.NoError(t, inter.Close()) })

	return &fixture{interceptor: inter, clock: clock, events: events}
}

// readRTP delivers one incoming RTP packet for ssrc through the interceptor.
func readRTP(t *testing.T, i *Interceptor, ssrc uint32, sequenceNumber uint16) {
	t.Helper()
	pkt := &rtp.Packet{
		Header:  rtp.Header{SSRC: ssrc, SequenceNumber: sequenceNumber},
		Payload: []byte{0x01},
	}
	raw, err := pkt.Marshal()
	require.NoError(t, err)
	reader := i.BindRemoteStream(&interceptor.StreamInfo{SSRC: ssrc}, interceptor.RTPReaderFunc(
		func(b []byte, a interceptor.Attributes) (int, interceptor.Attributes, error) {
			return copy(b, raw), a, nil
		},
	))
	n, _, err := reader.Read(make([]byte, 1500), nil)
	require.NoError(t, err)
	require.Equal(t, len(raw), n)
}

// readRTCP delivers one incoming RTCP batch through the interceptor.
func readRTCP(t *testing.T, i *Interceptor, pkts ...rtcp.Packet) {
	t.Helper()
	raw, err := rtcp.Marshal(pkts)
	require.NoError(t, err)
	reader := i.BindRTCPReader(interceptor.RTCPReaderFunc(
		func(b []byte, a interceptor.Attributes) (int, interceptor.Attributes, error) {
			return copy(b, raw), a, nil
		},
	))
	n, _, err := reader.Read(make([]byte, 1500), nil)
	require.NoError(t, err)
	require.Equal(t, len(raw), n)
}

// TestThreeStreamStates drives three streams through the full state machine:
// stream 1 stays active, stream 2 goes silent and recovers, then ends;
// stream 3 goes silent and ends.
//
//nolint:cyclop
func TestThreeStreamStates(t *testing.T) {
	f := newFixture(t)
	i := f.interceptor
	start := f.clock.Now()

	// t=0: all three streams appear.
	readRTP(t, i, 1, 1)
	readRTP(t, i, 2, 1)
	readRTP(t, i, 3, 1)
	require.Equal(t, []EventType{EventFirstSeen}, f.events.typesFor(1))
	require.Equal(t, []EventType{EventFirstSeen}, f.events.typesFor(2))
	require.Equal(t, []EventType{EventFirstSeen}, f.events.typesFor(3))

	state := i.Get(1)
	require.NotNil(t, state)
	require.Equal(t, StateActive, state.State)
	require.Equal(t, start, state.FirstSeen)
	require.Equal(t, start, state.LastRTP)
	require.True(t, state.LastRTCP.IsZero())

	// t=5: stream 1 stays alive.
	f.clock.Advance(5 * time.Second)
	readRTP(t, i, 1, 2)

	// t=11: streams 2 and 3 cross the silence threshold, stream 1 does not.
	f.clock.Advance(6 * time.Second)
	i.check(f.clock.Now())
	require.Equal(t, []EventType{EventFirstSeen}, f.events.typesFor(1))
	require.Equal(t, []EventType{EventFirstSeen, EventSilent}, f.events.typesFor(2))
	require.Equal(t, []EventType{EventFirstSeen, EventSilent}, f.events.typesFor(3))
	require.Equal(t, StateActive, i.Get(1).State)
	require.Equal(t, StateSilent, i.Get(2).State)

	// Repeated checks must not re-emit the silent events.
	i.check(f.clock.Now())
	f.clock.Advance(time.Second)
	i.check(f.clock.Now())
	require.Equal(t, 5, f.events.count())

	// t=12: stream 2 recovers.
	readRTP(t, i, 2, 2)
	require.Equal(t,
		[]EventType{EventFirstSeen, EventSilent, EventRecovered},
		f.events.typesFor(2))
	require.Equal(t, StateActive, i.Get(2).State)
	require.Equal(t, f.clock.Now(), i.Get(2).LastActivity)

	// Stream 1 keeps sending.
	f.clock.Advance(3 * time.Second) // t=15
	readRTP(t, i, 1, 3)
	f.clock.Advance(10 * time.Second) // t=25
	readRTP(t, i, 1, 4)

	// t=31: stream 3 ends (31s idle), stream 2 goes silent again (19s idle),
	// stream 1 is still active (6s idle).
	f.clock.Advance(6 * time.Second)
	i.check(f.clock.Now())
	require.Equal(t, []EventType{EventFirstSeen, EventSilent, EventEnded}, f.events.typesFor(3))
	require.Equal(t,
		[]EventType{EventFirstSeen, EventSilent, EventRecovered, EventSilent},
		f.events.typesFor(2))
	require.Nil(t, i.Get(3), "ended stream state must be released")
	require.Equal(t, StateSilent, i.Get(2).State)
	require.Equal(t, StateActive, i.Get(1).State)

	// t=35: stream 1 stays alive.
	f.clock.Advance(4 * time.Second)
	readRTP(t, i, 1, 5)

	// t=43: stream 2 ends, stream 1 remains active.
	f.clock.Advance(8 * time.Second)
	i.check(f.clock.Now())
	require.Equal(t,
		[]EventType{EventFirstSeen, EventSilent, EventRecovered, EventSilent, EventEnded},
		f.events.typesFor(2))
	require.Nil(t, i.Get(2))
	require.Equal(t, StateActive, i.Get(1).State)

	// Further checks emit nothing more.
	i.check(f.clock.Now())
	require.Equal(t, 9, f.events.count())
}

func TestEventDedup(t *testing.T) {
	f := newFixture(t)
	i := f.interceptor

	readRTP(t, i, 1, 1)
	f.clock.Advance(11 * time.Second)
	for range 5 {
		i.check(f.clock.Now())
	}
	require.Equal(t, []EventType{EventFirstSeen, EventSilent}, f.events.typesFor(1))

	f.clock.Advance(20 * time.Second)
	for range 5 {
		i.check(f.clock.Now())
	}
	require.Equal(t,
		[]EventType{EventFirstSeen, EventSilent, EventEnded},
		f.events.typesFor(1))
}

func TestRTCPOnlyStream(t *testing.T) {
	f := newFixture(t)
	i := f.interceptor
	start := f.clock.Now()

	// A stream that only ever sends RTCP is tracked like an RTP stream.
	readRTCP(t, i, &rtcp.ReceiverReport{Reports: []rtcp.ReceptionReport{{SSRC: 7}}})
	require.Equal(t, []EventType{EventFirstSeen}, f.events.typesFor(7))
	state := i.Get(7)
	require.NotNil(t, state)
	require.Equal(t, start, state.LastRTCP)
	require.True(t, state.LastRTP.IsZero())

	// A single RTCP batch referencing the SSRC multiple times records once.
	readRTCP(t, i,
		&rtcp.ReceiverReport{Reports: []rtcp.ReceptionReport{{SSRC: 7}}},
		&rtcp.PictureLossIndication{MediaSSRC: 7},
	)
	require.Equal(t, []EventType{EventFirstSeen}, f.events.typesFor(7))

	f.clock.Advance(11 * time.Second)
	i.check(f.clock.Now())
	require.Equal(t, []EventType{EventFirstSeen, EventSilent}, f.events.typesFor(7))

	readRTCP(t, i, &rtcp.ReceiverReport{Reports: []rtcp.ReceptionReport{{SSRC: 7}}})
	require.Equal(t,
		[]EventType{EventFirstSeen, EventSilent, EventRecovered},
		f.events.typesFor(7))
}

func TestClockBackwards(t *testing.T) {
	f := newFixture(t)
	i := f.interceptor

	readRTP(t, i, 1, 1)
	seen := f.clock.Now()

	// The clock jumps backwards: no events, last activity is kept.
	f.clock.Set(seen.Add(-5 * time.Second))
	i.check(f.clock.Now())
	readRTP(t, i, 1, 2)
	require.Equal(t, []EventType{EventFirstSeen}, f.events.typesFor(1))
	require.Equal(t, seen, i.Get(1).LastActivity)
	require.Equal(t, seen, i.Get(1).LastRTP)

	// Silence is measured against the kept (newer) timestamp.
	f.clock.Set(seen.Add(5 * time.Second))
	i.check(f.clock.Now())
	require.Equal(t, StateActive, i.Get(1).State)
	f.clock.Set(seen.Add(11 * time.Second))
	i.check(f.clock.Now())
	require.Equal(t, []EventType{EventFirstSeen, EventSilent}, f.events.typesFor(1))
}

func TestSSRCReuse(t *testing.T) {
	f := newFixture(t)
	i := f.interceptor
	info := &interceptor.StreamInfo{SSRC: 9}

	readRTP(t, i, 9, 1)
	require.NotNil(t, i.Get(9))

	// Unbinding drops the state immediately, without an event.
	i.UnbindRemoteStream(info)
	require.Nil(t, i.Get(9))
	require.Equal(t, []EventType{EventFirstSeen}, f.events.typesFor(9))

	// A reused SSRC starts over with a fresh first-seen event.
	f.clock.Advance(time.Second)
	readRTP(t, i, 9, 2)
	require.Equal(t, []EventType{EventFirstSeen, EventFirstSeen}, f.events.typesFor(9))
	require.Equal(t, f.clock.Now(), i.Get(9).FirstSeen)
}

func TestStreamReappearsAfterEnd(t *testing.T) {
	f := newFixture(t)
	i := f.interceptor

	readRTP(t, i, 1, 1)
	f.clock.Advance(31 * time.Second)
	i.check(f.clock.Now())
	require.Equal(t,
		[]EventType{EventFirstSeen, EventSilent, EventEnded},
		f.events.typesFor(1))
	require.Nil(t, i.Get(1))

	// A stream that comes back after it ended is a new stream.
	readRTP(t, i, 1, 2)
	require.Equal(t,
		[]EventType{EventFirstSeen, EventSilent, EventEnded, EventFirstSeen},
		f.events.typesFor(1))
	require.Equal(t, StateActive, i.Get(1).State)
}

func TestOutOfOrderPackets(t *testing.T) {
	f := newFixture(t)
	i := f.interceptor

	// Reordered and duplicated packets only refresh the arrival time; the
	// sequence numbers and RTP timestamps are irrelevant to liveness.
	readRTP(t, i, 1, 100)
	f.clock.Advance(time.Second)
	readRTP(t, i, 1, 3)
	f.clock.Advance(time.Second)
	readRTP(t, i, 1, 3)
	f.clock.Advance(time.Second)
	readRTP(t, i, 1, 42)
	require.Equal(t, []EventType{EventFirstSeen}, f.events.typesFor(1))
	require.Equal(t, StateActive, i.Get(1).State)

	f.clock.Advance(9 * time.Second)
	i.check(f.clock.Now())
	require.Equal(t, StateActive, i.Get(1).State)
}

func TestInvalidOptions(t *testing.T) {
	for _, opts := range [][]Option{
		{WithSilenceTimeout(0)},
		{WithSilenceTimeout(-time.Second)},
		{WithEndTimeout(0)},
		{WithCheckInterval(0)},
		{WithCheckInterval(-time.Second)},
		{WithMaxStreams(0)},
		{WithNowFunc(nil)},
		{WithSilenceTimeout(10 * time.Second), WithEndTimeout(10 * time.Second)},
		{WithSilenceTimeout(10 * time.Second), WithEndTimeout(5 * time.Second)},
	} {
		factory, err := NewInterceptor(opts...)
		require.NoError(t, err)
		ic, err := factory.NewInterceptor("")
		require.Error(t, err)
		require.Nil(t, ic)
	}
}

func TestMaxStreamsEviction(t *testing.T) {
	f := newFixture(t, WithMaxStreams(2))
	i := f.interceptor

	readRTP(t, i, 1, 1)
	f.clock.Advance(time.Second)
	readRTP(t, i, 2, 1)
	f.clock.Advance(time.Second)
	// The table is full: the least recently active stream (1) is evicted.
	readRTP(t, i, 3, 1)
	require.Nil(t, i.Get(1))
	require.NotNil(t, i.Get(2))
	require.NotNil(t, i.Get(3))
	// Eviction emits no events.
	require.Equal(t, []EventType{EventFirstSeen}, f.events.typesFor(1))
	require.Equal(t, 3, f.events.count())

	// Refreshing stream 2 makes stream 3 the eviction candidate.
	readRTP(t, i, 2, 2)
	readRTP(t, i, 4, 1)
	require.Nil(t, i.Get(3))
	require.NotNil(t, i.Get(2))
	require.NotNil(t, i.Get(4))
}

func TestCloseReleasesResources(t *testing.T) {
	f := newFixture(t)
	i := f.interceptor

	readRTP(t, i, 1, 1)
	readRTP(t, i, 2, 1)
	require.NoError(t, i.Close())

	require.Nil(t, i.Get(1))
	require.Empty(t, i.streams)

	// Recording and checking after Close are no-ops and emit nothing.
	before := f.events.count()
	readRTP(t, i, 3, 1)
	i.check(f.clock.Now())
	require.Equal(t, before, f.events.count())
	require.Empty(t, i.streams)

	// Close is idempotent.
	require.NoError(t, i.Close())
}

func TestConcurrentStreamsAndClose(t *testing.T) {
	factory, err := NewInterceptor(
		WithSilenceTimeout(5*time.Millisecond),
		WithEndTimeout(20*time.Millisecond),
		WithCheckInterval(time.Millisecond),
		OnEvent(func(Event) {}),
	)
	require.NoError(t, err)
	ic, err := factory.NewInterceptor("")
	require.NoError(t, err)
	i, ok := ic.(*Interceptor)
	require.True(t, ok)

	const streams = 8
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for ssrc := uint32(1); ssrc <= streams; ssrc++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			pkt := &rtp.Packet{Header: rtp.Header{SSRC: ssrc}, Payload: []byte{0x01}}
			raw, mErr := pkt.Marshal()
			require.NoError(t, mErr)
			reader := i.BindRemoteStream(&interceptor.StreamInfo{SSRC: ssrc}, interceptor.RTPReaderFunc(
				func(b []byte, a interceptor.Attributes) (int, interceptor.Attributes, error) {
					return copy(b, raw), a, nil
				},
			))
			for {
				select {
				case <-stop:
					return
				default:
					_, _, _ = reader.Read(make([]byte, 1500), nil) //nolint:errcheck
					_ = i.Get(ssrc)
				}
			}
		}()
	}

	time.Sleep(50 * time.Millisecond)
	require.NoError(t, i.Close())
	close(stop)
	wg.Wait()

	// After Close, no stream state remains and queries return nil.
	require.Empty(t, i.streams)
	for ssrc := uint32(1); ssrc <= streams; ssrc++ {
		require.Nil(t, i.Get(ssrc))
	}
}

// TestCloseWaitsForCallbacks verifies that Close does not return while an
// event callback is still running, and that no callback runs afterwards.
func TestCloseWaitsForCallbacks(t *testing.T) {
	inCallback := make(chan struct{})
	finishCallback := make(chan struct{})
	var completed sync.WaitGroup

	factory, err := NewInterceptor(
		WithCheckInterval(time.Hour),
		OnEvent(func(Event) {
			close(inCallback)
			<-finishCallback
			completed.Done()
		}),
	)
	require.NoError(t, err)
	ic, err := factory.NewInterceptor("")
	require.NoError(t, err)
	i, ok := ic.(*Interceptor)
	require.True(t, ok)

	completed.Add(1)
	go readRTP(t, i, 1, 1)

	// Wait until the first-seen callback is running, then close.
	<-inCallback
	closed := make(chan struct{})
	go func() {
		require.NoError(t, i.Close())
		close(closed)
	}()

	select {
	case <-closed:
		t.Fatal("Close returned while a callback was still running")
	case <-time.After(20 * time.Millisecond):
	}

	close(finishCallback)
	completed.Wait()
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("Close did not return after the callback finished")
	}
}
