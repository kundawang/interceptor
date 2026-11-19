// SPDX-FileCopyrightText: 2026 The Pion community <https://pion.ly>
// SPDX-License-Identifier: MIT

// Package liveliness provides an interceptor that detects whether incoming
// RTP streams are alive, silent, or ended. It records the most recent
// RTP/RTCP arrival per SSRC and emits events when a stream is first seen,
// goes silent, recovers, or is considered ended.
//
// The detection rules are deliberately simple and wall-clock based:
//   - Liveness is derived from packet arrival time only. RTP sequence
//     numbers and RTP timestamps are ignored, so reordered or duplicated
//     packets never affect the state machine.
//   - Both RTP and RTCP arrivals count as activity. A stream that only
//     sends RTCP is tracked like any other stream.
//   - If the injected clock moves backwards, the recorded last-activity
//     time is kept (never moved backwards) and the elapsed time is clamped
//     to zero, so a clock jump never produces spurious events.
//   - Unbinding a remote stream removes its state immediately without an
//     event. If the same SSRC is bound again later it is treated as a brand
//     new stream (SSRC reuse starts fresh).
//   - A stream whose entry was removed after EventEnded and that sends
//     packets again is treated as a new stream and emits EventFirstSeen.
//   - The state table is bounded by WithMaxStreams. When it is full and a
//     new SSRC arrives, the least recently active entry is evicted silently
//     (no event) to make room.
package liveliness

import (
	"errors"
	"sync"
	"time"

	"github.com/pion/interceptor"
)

// Default configuration values.
const (
	DefaultSilenceTimeout = 5 * time.Second
	DefaultEndTimeout     = 30 * time.Second
	DefaultCheckInterval  = time.Second
	DefaultMaxStreams     = 1024
)

var (
	errInvalidSilenceTimeout = errors.New("liveliness: silence timeout must be greater than zero")
	errInvalidEndTimeout     = errors.New("liveliness: end timeout must be greater than silence timeout")
	errInvalidCheckInterval  = errors.New("liveliness: check interval must be greater than zero")
	errInvalidMaxStreams     = errors.New("liveliness: max streams must be greater than zero")
	errNilNowFunc            = errors.New("liveliness: now func must not be nil")
)

// State is the liveness state of a single stream.
type State int

const (
	// StateActive means packets arrived within the silence timeout.
	StateActive State = iota
	// StateSilent means no packets arrived for at least the silence timeout.
	StateSilent
	// StateEnded means no packets arrived for at least the end timeout. The
	// stream entry is released when this state is reached.
	StateEnded
)

// String returns a human readable name of the state.
func (s State) String() string {
	switch s {
	case StateActive:
		return "active"
	case StateSilent:
		return "silent"
	case StateEnded:
		return "ended"
	default:
		return "unknown"
	}
}

// EventType identifies the kind of a liveness Event.
type EventType int

const (
	// EventFirstSeen is emitted when the first packet of a stream arrives.
	EventFirstSeen EventType = iota
	// EventSilent is emitted when a stream has been inactive for the
	// configured silence timeout.
	EventSilent
	// EventRecovered is emitted when a packet of a silent stream arrives.
	EventRecovered
	// EventEnded is emitted when a stream has been inactive for the
	// configured end timeout. The stream state is released afterwards.
	EventEnded
)

// String returns a human readable name of the event type.
func (t EventType) String() string {
	switch t {
	case EventFirstSeen:
		return "first_seen"
	case EventSilent:
		return "silent"
	case EventRecovered:
		return "recovered"
	case EventEnded:
		return "ended"
	default:
		return "unknown"
	}
}

// Event describes a single liveness state transition of a stream.
type Event struct {
	// SSRC is the stream the event belongs to.
	SSRC uint32
	// Type is the kind of transition.
	Type EventType
	// State is the state the stream is in after the transition.
	State State
	// At is the time the transition was detected.
	At time.Time
	// LastActivity is the most recent RTP/RTCP arrival time of the stream.
	LastActivity time.Time
}

// StreamState is a snapshot of the liveness state of a single stream.
type StreamState struct {
	SSRC         uint32
	State        State
	FirstSeen    time.Time
	LastRTP      time.Time
	LastRTCP     time.Time
	LastActivity time.Time
}

// EventCallback is called for every liveness event. It is invoked without
// the interceptor lock held, so it may call Get. It must not call Close.
type EventCallback func(Event)

// Option can be used to configure the Interceptor.
type Option func(*Interceptor) error

// WithSilenceTimeout sets how long a stream may be inactive before it is
// marked silent. It must be greater than zero.
func WithSilenceTimeout(d time.Duration) Option {
	return func(i *Interceptor) error {
		if d <= 0 {
			return errInvalidSilenceTimeout
		}
		i.silenceTimeout = d

		return nil
	}
}

// WithEndTimeout sets how long a stream may be inactive before it is
// considered ended and its state is released. It must be greater than the
// silence timeout.
func WithEndTimeout(d time.Duration) Option {
	return func(i *Interceptor) error {
		if d <= 0 {
			return errInvalidEndTimeout
		}
		i.endTimeout = d

		return nil
	}
}

// WithCheckInterval sets how often stream activity is evaluated.
// It must be greater than zero.
func WithCheckInterval(d time.Duration) Option {
	return func(i *Interceptor) error {
		if d <= 0 {
			return errInvalidCheckInterval
		}
		i.checkInterval = d

		return nil
	}
}

// WithMaxStreams bounds the number of streams tracked at the same time.
// When the table is full the least recently active stream is evicted
// silently. It must be greater than zero.
func WithMaxStreams(n int) Option {
	return func(i *Interceptor) error {
		if n <= 0 {
			return errInvalidMaxStreams
		}
		i.maxStreams = n

		return nil
	}
}

// WithNowFunc sets the function used to obtain the current time. This is
// mostly useful for testing with a virtual clock.
func WithNowFunc(now func() time.Time) Option {
	return func(i *Interceptor) error {
		if now == nil {
			return errNilNowFunc
		}
		i.now = now

		return nil
	}
}

// OnEvent sets the callback invoked for every liveness event.
func OnEvent(cb EventCallback) Option {
	return func(i *Interceptor) error {
		i.onEvent = cb

		return nil
	}
}

// InterceptorFactory is an interceptor.Factory for the liveness Interceptor.
type InterceptorFactory struct {
	opts []Option
}

// NewInterceptor creates a new InterceptorFactory.
func NewInterceptor(opts ...Option) (*InterceptorFactory, error) {
	return &InterceptorFactory{opts: opts}, nil
}

// NewInterceptor creates a new Interceptor.
func (f *InterceptorFactory) NewInterceptor(_ string) (interceptor.Interceptor, error) {
	i := &Interceptor{
		now:            time.Now,
		silenceTimeout: DefaultSilenceTimeout,
		endTimeout:     DefaultEndTimeout,
		checkInterval:  DefaultCheckInterval,
		maxStreams:     DefaultMaxStreams,
		streams:        map[uint32]*stream{},
		stopCh:         make(chan struct{}),
	}
	for _, opt := range f.opts {
		if err := opt(i); err != nil {
			return nil, err
		}
	}
	if i.endTimeout <= i.silenceTimeout {
		return nil, errInvalidEndTimeout
	}

	i.wg.Add(1)
	go i.run()

	return i, nil
}

type stream struct {
	state        State
	firstSeen    time.Time
	lastRTP      time.Time
	lastRTCP     time.Time
	lastActivity time.Time
}

// Interceptor detects silent and ended incoming RTP streams.
type Interceptor struct {
	interceptor.NoOp

	now            func() time.Time
	silenceTimeout time.Duration
	endTimeout     time.Duration
	checkInterval  time.Duration
	maxStreams     int
	onEvent        EventCallback

	lock    sync.Mutex
	streams map[uint32]*stream
	closed  bool

	stopCh chan struct{}
	once   sync.Once
	wg     sync.WaitGroup // ticker goroutine
	cbWg   sync.WaitGroup // in-flight event callbacks
}

// Get returns the current liveness state of the stream with ssrc, or nil if
// the stream is not tracked (never seen, unbound, ended, or evicted).
func (i *Interceptor) Get(ssrc uint32) *StreamState {
	i.lock.Lock()
	defer i.lock.Unlock()

	st, ok := i.streams[ssrc]
	if !ok {
		return nil
	}

	return &StreamState{
		SSRC:         ssrc,
		State:        st.state,
		FirstSeen:    st.firstSeen,
		LastRTP:      st.lastRTP,
		LastRTCP:     st.lastRTCP,
		LastActivity: st.lastActivity,
	}
}

// Close stops the periodic checker, releases all stream state, and waits
// for in-flight event callbacks to return. No events are emitted after
// Close has returned.
func (i *Interceptor) Close() error {
	i.once.Do(func() {
		close(i.stopCh)
	})
	i.lock.Lock()
	i.closed = true
	i.streams = map[uint32]*stream{}
	i.lock.Unlock()

	i.wg.Wait()
	i.cbWg.Wait()

	return nil
}

// BindRemoteStream lets you modify any incoming RTP packets. It records the
// arrival of every incoming RTP packet of the stream.
func (i *Interceptor) BindRemoteStream(
	info *interceptor.StreamInfo, reader interceptor.RTPReader,
) interceptor.RTPReader {
	ssrc := info.SSRC

	return interceptor.RTPReaderFunc(
		func(bytes []byte, attributes interceptor.Attributes) (int, interceptor.Attributes, error) {
			n, attributes, err := reader.Read(bytes, attributes)
			if err != nil {
				return n, attributes, err
			}
			i.record(ssrc, true)

			return n, attributes, nil
		},
	)
}

// UnbindRemoteStream removes the liveness state of the stream. No event is
// emitted; if the SSRC is seen again it is treated as a new stream.
func (i *Interceptor) UnbindRemoteStream(info *interceptor.StreamInfo) {
	i.lock.Lock()
	defer i.lock.Unlock()
	delete(i.streams, info.SSRC)
}

// BindRTCPReader records the arrival of incoming RTCP packets for every
// SSRC they reference, so RTCP-only streams are tracked as well.
func (i *Interceptor) BindRTCPReader(reader interceptor.RTCPReader) interceptor.RTCPReader {
	return interceptor.RTCPReaderFunc(
		func(bytes []byte, attributes interceptor.Attributes) (int, interceptor.Attributes, error) {
			n, attributes, err := reader.Read(bytes, attributes)
			if err != nil {
				return n, attributes, err
			}
			if attributes == nil {
				attributes = make(interceptor.Attributes)
			}
			pkts, err := attributes.GetRTCPPackets(bytes[:n])
			if err != nil {
				return n, attributes, nil //nolint:nilerr // unparseable RTCP is not a liveness concern
			}
			seen := make(map[uint32]bool)
			for _, pkt := range pkts {
				for _, ssrc := range pkt.DestinationSSRC() {
					if !seen[ssrc] {
						seen[ssrc] = true
						i.record(ssrc, false)
					}
				}
			}

			return n, attributes, nil
		},
	)
}

// record notes activity of a stream. isRTP distinguishes RTP from RTCP
// arrivals for the LastRTP/LastRTCP timestamps.
func (i *Interceptor) record(ssrc uint32, isRTP bool) {
	now := i.now()

	i.lock.Lock()
	if i.closed {
		i.lock.Unlock()

		return
	}
	st, ok := i.streams[ssrc]
	var event *Event
	if !ok {
		if len(i.streams) >= i.maxStreams {
			i.evictLocked()
		}
		st = &stream{state: StateActive, firstSeen: now, lastActivity: now}
		i.streams[ssrc] = st
		event = &Event{SSRC: ssrc, Type: EventFirstSeen, State: StateActive, At: now, LastActivity: now}
	} else {
		// A clock that moved backwards must never move the recorded
		// timestamps backwards.
		if now.After(st.lastActivity) {
			st.lastActivity = now
		}
		if st.state == StateSilent {
			st.state = StateActive
			event = &Event{SSRC: ssrc, Type: EventRecovered, State: StateActive, At: now, LastActivity: st.lastActivity}
		}
	}
	if isRTP {
		if now.After(st.lastRTP) {
			st.lastRTP = now
		}
	} else if now.After(st.lastRTCP) {
		st.lastRTCP = now
	}
	cb := i.onEvent
	if event != nil && cb != nil {
		i.cbWg.Add(1)
	}
	i.lock.Unlock()

	if event != nil && cb != nil {
		cb(*event)
		i.cbWg.Done()
	}
}

// check evaluates all tracked streams against the configured timeouts at
// time now. Events are only emitted on state transitions, so calling check
// repeatedly never emits the same event twice.
func (i *Interceptor) check(now time.Time) {
	var events []Event

	i.lock.Lock()
	if i.closed {
		i.lock.Unlock()

		return
	}
	for ssrc, st := range i.streams {
		elapsed := now.Sub(st.lastActivity)
		if elapsed < 0 {
			// The clock moved backwards; treat the stream as just seen.
			elapsed = 0
		}
		switch st.state {
		case StateActive:
			if elapsed >= i.endTimeout {
				// Both thresholds passed since the last check: report the
				// full transition, silent first, then ended.
				events = append(events,
					Event{SSRC: ssrc, Type: EventSilent, State: StateSilent, At: now, LastActivity: st.lastActivity},
					Event{SSRC: ssrc, Type: EventEnded, State: StateEnded, At: now, LastActivity: st.lastActivity},
				)
				delete(i.streams, ssrc)
			} else if elapsed >= i.silenceTimeout {
				st.state = StateSilent
				events = append(events,
					Event{SSRC: ssrc, Type: EventSilent, State: StateSilent, At: now, LastActivity: st.lastActivity})
			}
		case StateSilent:
			if elapsed >= i.endTimeout {
				events = append(events,
					Event{SSRC: ssrc, Type: EventEnded, State: StateEnded, At: now, LastActivity: st.lastActivity})
				delete(i.streams, ssrc)
			}
		case StateEnded:
			// Unreachable: ended streams are removed from the table.
		}
	}
	cb := i.onEvent
	if cb != nil && len(events) > 0 {
		i.cbWg.Add(len(events))
	}
	i.lock.Unlock()

	for _, event := range events {
		cb(event)
		i.cbWg.Done()
	}
}

// evictLocked removes the least recently active stream to keep the state
// table bounded. No event is emitted for the evicted stream.
// The caller must hold i.lock.
func (i *Interceptor) evictLocked() {
	var oldest uint32
	var oldestActivity time.Time
	first := true
	for ssrc, st := range i.streams {
		if first || st.lastActivity.Before(oldestActivity) {
			oldest, oldestActivity, first = ssrc, st.lastActivity, false
		}
	}
	delete(i.streams, oldest)
}

// run periodically evaluates stream activity until Close is called.
func (i *Interceptor) run() {
	defer i.wg.Done()

	ticker := time.NewTicker(i.checkInterval)
	defer ticker.Stop()
	for {
		select {
		case <-i.stopCh:
			return
		case now := <-ticker.C:
			i.check(now)
		}
	}
}
