// SPDX-FileCopyrightText: 2026 The Pion community <https://pion.ly>
// SPDX-License-Identifier: MIT

// Package jitterbuffer implements a buffer for RTP packets designed to help
// counteract non-deterministic sources of latency
package jitterbuffer

import (
	"errors"
	"sync"
	"time"

	"github.com/pion/rtp"
)

// State tracks a JitterBuffer as either Buffering or Emitting.
type State uint16

// Event represents all events a JitterBuffer can emit.
type Event string

var (
	// ErrBufferUnderrun is returned when the buffer has no items.
	ErrBufferUnderrun = errors.New("invalid Peek: Empty jitter buffer")
	// ErrPopWhileBuffering is returned if a jitter buffer is not in a playback state.
	ErrPopWhileBuffering = errors.New("attempt to pop while buffering")
	// ErrPopNotReady is returned while the adaptive target delay has not yet
	// elapsed for the packet at the playout head. The caller should retry later.
	ErrPopNotReady = errors.New("packet is not ready for playout yet")
)

const (
	// Buffering is the state when the jitter buffer has not started emitting yet,
	// or has hit an underflow and needs to re-buffer packets.
	Buffering State = iota
	//  Emitting is the state when the jitter buffer is operating nominally
	Emitting
)

const (
	// StartBuffering is emitted when the buffer receives its first packet.
	StartBuffering Event = "startBuffering"
	// BeginPlayback is emitted when the buffer has satisfied its buffer length.
	BeginPlayback = "playing"
	// BufferUnderflow is emitted when the buffer does not have enough packets to Pop.
	BufferUnderflow = "underflow"
	// BufferOverflow is emitted when the buffer has exceeded its limit.
	BufferOverflow = "overflow"
)

func (jbs State) String() string {
	switch jbs {
	case Buffering:
		return "Buffering"
	case Emitting:
		return "Emitting"
	}

	return "unknown"
}

type (
	// Option will Override JitterBuffer's defaults.
	Option func(jb *JitterBuffer)
	// EventListener will be called when the corresponding Event occurs.
	EventListener func(event Event, jb *JitterBuffer)
)

// A JitterBuffer will accept Pushed packets, put them in sequence number
// order, and allows removing in either sequence number order or via a
// provided timestamp.
type JitterBuffer struct {
	packets       *PriorityQueue
	delay         delayController
	minStartCount uint16
	overflowLen   uint16
	lastSequence  uint16
	playoutHead   uint16
	playoutReady  bool
	state         State
	stats         Stats
	listeners     map[Event][]EventListener
	mutex         sync.Mutex
}

// Stats Track interesting statistics for the life of this JitterBuffer
// outOfOrderCount will provide the number of times a packet was Pushed
//
//	without its predecessor being present
//
// underflowCount will provide the count of attempts to Pop an empty buffer
// overflowCount will track the number of times the jitter buffer exceeds its limit.
// lateCount will track packets dropped because they arrived behind the playout
// head once adaptive playout was emitting.
type Stats struct {
	outOfOrderCount uint32
	underflowCount  uint32
	overflowCount   uint32
	lateCount       uint32
}

// New will initialize a jitter buffer and its associated statistics.
func New(opts ...Option) *JitterBuffer {
	jb := &JitterBuffer{
		state:         Buffering,
		stats:         Stats{},
		minStartCount: 50,
		overflowLen:   100,
		packets:       NewQueue(),
		listeners:     make(map[Event][]EventListener),
		delay:         newDelayController(),
	}

	for _, o := range opts {
		o(jb)
	}

	jb.delay.minDelay, jb.delay.maxDelay = normalizeDelayBounds(jb.delay.minDelay, jb.delay.maxDelay)
	jb.delay.targetDelay = jb.delay.minDelay

	return jb
}

// WithMinimumPacketCount will set the required number of packets to be received before
// any attempt to pop a packet can succeed.
func WithMinimumPacketCount(count uint16) Option {
	return func(jb *JitterBuffer) {
		jb.minStartCount = count
	}
}

// WithAdaptiveDelay enables adaptive playout delay and bounds the target between
// minDelay and maxDelay. Once enabled, the playout target is derived from the
// estimated interarrival jitter and a packet is handed out only after it has
// been buffered for the current target. Negative bounds are clamped to zero and
// a maximum below the minimum is raised to the minimum, so the bounds are always
// usable.
func WithAdaptiveDelay(minDelay, maxDelay time.Duration) Option {
	return func(jb *JitterBuffer) {
		minDelay, maxDelay = normalizeDelayBounds(minDelay, maxDelay)
		jb.delay.enabled = true
		jb.delay.minDelay = minDelay
		jb.delay.maxDelay = maxDelay
		jb.delay.targetDelay = minDelay
	}
}

// WithClockRate sets the RTP media clock rate used to convert timestamp deltas
// into wall time. Zero selects the default of 90000 Hz. A changed rate resets
// the jitter estimate because earlier samples used the previous clock.
func WithClockRate(rate uint32) Option {
	return func(jb *JitterBuffer) {
		jb.delay.setClockRate(rate)
	}
}

// WithOverflowSize sets how many packets the buffer may hold. With adaptive
// playout enabled the queue is strictly bounded: when it grows past this length
// the packet that arrived earliest is evicted. The default is 100. Zero leaves
// the default unchanged.
func WithOverflowSize(size uint16) Option {
	return func(jb *JitterBuffer) {
		if size > 0 {
			jb.overflowLen = size
		}
	}
}

// WithNow replaces the clock used for arrival, jitter estimation and playout
// readiness. It is mainly useful for deterministic tests.
func WithNow(now func() time.Time) Option {
	return func(jb *JitterBuffer) {
		if now != nil {
			jb.delay.now = now
		}
	}
}

// WithStallThreshold sets the arrival or media-time gap that is treated as a
// stream discontinuity rather than jitter. The estimate is restarted at such a
// gap so a pause cannot inflate the target delay. A non-positive value disables
// the check.
func WithStallThreshold(threshold time.Duration) Option {
	return func(jb *JitterBuffer) {
		jb.delay.stallThreshold = threshold
	}
}

// WithReleaseHold sets how long after the last target increase the target must
// stay unchallenged before it may shrink. It prevents the delay from
// oscillating on short quiet periods. A non-positive value allows shrinking on
// the next packet.
func WithReleaseHold(hold time.Duration) Option {
	return func(jb *JitterBuffer) {
		jb.delay.releaseHold = hold
	}
}

// Listen will register an event listener
// The jitter buffer may emit events correspnding, interested listerns should
// look at Event for available events.
func (jb *JitterBuffer) Listen(event Event, cb EventListener) {
	jb.listeners[event] = append(jb.listeners[event], cb)
}

// PlayoutHead returns the SequenceNumber that will be attempted to Pop next.
func (jb *JitterBuffer) PlayoutHead() uint16 {
	jb.mutex.Lock()
	defer jb.mutex.Unlock()

	return jb.playoutHead
}

// State returns the current Buffering or Emitting state.
func (jb *JitterBuffer) State() State {
	jb.mutex.Lock()
	defer jb.mutex.Unlock()

	return jb.state
}

// SetPlayoutHead allows you to manually specify the packet you wish to pop next
// If you have encountered a packet that hasn't resolved you can skip it.
func (jb *JitterBuffer) SetPlayoutHead(playoutHead uint16) {
	jb.mutex.Lock()
	defer jb.mutex.Unlock()

	jb.playoutHead = playoutHead
}

func (jb *JitterBuffer) updateStats(lastPktSeqNo uint16) {
	// If we have at least one packet, and the next packet being pushed in is not
	// at the expected sequence number increment the out of order count
	if jb.packets.Length() > 0 && lastPktSeqNo != (jb.lastSequence+1) {
		jb.stats.outOfOrderCount++
	}
	jb.lastSequence = lastPktSeqNo
}

// Push an RTP packet into the jitter buffer, this does not clone
// the data so if the memory is expected to be reused, the caller should
// take this in to account and pass a copy of the packet they wish to buffer.
func (jb *JitterBuffer) Push(packet *rtp.Packet) {
	jb.mutex.Lock()
	defer jb.mutex.Unlock()

	arrival := jb.delay.now()

	if jb.packets.Length() == 0 {
		jb.emit(StartBuffering)
	}

	if !jb.playoutReady && jb.packets.Length() == 0 {
		jb.playoutHead = packet.SequenceNumber
	}

	if jb.dropLate(packet) {
		return
	}

	// Original behavior when adaptive playout is disabled: report the overflow
	// before buffering, but let the caller control the queue length.
	if !jb.delay.adapts() && jb.packets.Length() > jb.overflowLen {
		jb.stats.overflowCount++
		jb.emit(BufferOverflow)
	}

	jb.delay.observe(arrival, packet.Timestamp)
	jb.updateStats(packet.SequenceNumber)
	jb.packets.PushArrival(packet, packet.SequenceNumber, arrival)

	if jb.delay.adapts() {
		jb.enforceOverflow()
	}

	jb.updateState()
}

// dropLate reports whether packet has already been played out. A packet behind
// the playout head can never be emitted, so it is dropped instead of buffered.
// The comparison is wrap-safe and only applies to adaptive playout, leaving the
// original Push behavior unchanged otherwise.
func (jb *JitterBuffer) dropLate(packet *rtp.Packet) bool {
	if !jb.delay.adapts() || !jb.playoutReady || jb.state != Emitting {
		return false
	}
	if !IsBefore(packet.SequenceNumber, jb.playoutHead) {
		return false
	}
	jb.stats.lateCount++

	return true
}

// enforceOverflow bounds the queue under adaptive playout: while it exceeds
// overflowLen, the earliest arrivals are evicted so a sustained burst cannot
// grow memory without limit.
func (jb *JitterBuffer) enforceOverflow() {
	if jb.packets.Length() <= jb.overflowLen {
		return
	}
	jb.stats.overflowCount++
	jb.emit(BufferOverflow)
	jb.evict()
}

// evict removes packets until the queue is within overflowLen. The packet that
// arrived earliest is removed first so a sustained burst cannot grow the queue
// without bound. The evicted packet is also dropped from playout accounting: if
// it sat at the playout head, the head advances so a lost packet cannot stall
// the queue forever.
func (jb *JitterBuffer) evict() {
	for jb.packets.Length() > jb.overflowLen {
		dropped, err := jb.packets.PopOldestArrival()
		if err != nil {
			return
		}
		jb.advancePlayoutHeadPast(dropped.SequenceNumber)
	}
}

// advancePlayoutHeadPast moves the playout head to the next packet after sq
// when sq was at the head, so an evicted packet cannot stall playout. This also
// applies while still buffering: the head is fixed at the first packet, and if
// that packet is evicted before playback starts the head must move with it.
// When no later packet is buffered the head lands on sq+1 and the usual
// underflow path applies until the stream catches up.
func (jb *JitterBuffer) advancePlayoutHeadPast(sq uint16) {
	if jb.playoutHead != sq {
		return
	}
	if next, ok := jb.packets.NextFrom(sq + 1); ok {
		jb.playoutHead = next

		return
	}
	jb.playoutHead = sq + 1
}

func (jb *JitterBuffer) emit(event Event) {
	for _, l := range jb.listeners[event] {
		l(event, jb)
	}
}

func (jb *JitterBuffer) updateState() {
	// For now, we only look at the number of packets captured in the play buffer
	if jb.packets.Length() >= jb.minStartCount && jb.state == Buffering {
		jb.state = Emitting
		jb.playoutReady = true
		jb.emit(BeginPlayback)
	}
}

// Peek at the packet which is either:
//
//	At the playout head when we are emitting, and the playoutHead flag is true
//
// or else
//
//	At the last sequence received
func (jb *JitterBuffer) Peek(playoutHead bool) (*rtp.Packet, error) {
	jb.mutex.Lock()
	defer jb.mutex.Unlock()
	if jb.packets.Length() < 1 {
		return nil, ErrBufferUnderrun
	}
	if playoutHead && jb.state == Emitting {
		return jb.packets.Find(jb.playoutHead)
	}

	return jb.packets.Find(jb.lastSequence)
}

// Pop an RTP packet from the jitter buffer at the current playout head.
func (jb *JitterBuffer) Pop() (*rtp.Packet, error) {
	jb.mutex.Lock()
	defer jb.mutex.Unlock()
	if jb.state != Emitting {
		return nil, ErrPopWhileBuffering
	}
	if !jb.ready(jb.playoutHead) {
		return nil, ErrPopNotReady
	}
	packet, err := jb.packets.PopAt(jb.playoutHead)
	if err != nil {
		jb.stats.underflowCount++
		jb.emit(BufferUnderflow)

		return nil, err
	}
	jb.playoutHead = (jb.playoutHead + 1)
	jb.updateState()

	return packet, nil
}

// ready reports whether the packet currently at sq may be emitted. With the
// adaptive delay disabled it is always true, preserving the original behavior.
// A packet with no recorded arrival is considered ready so the buffer can never
// stall on bookkeeping.
func (jb *JitterBuffer) ready(sq uint16) bool {
	if !jb.delay.adapts() {
		return true
	}
	arrival, ok := jb.packets.Arrival(sq)
	if !ok {
		// Preserve the existing underflow path: the packet simply is not here.
		return true
	}

	return jb.delay.ready(arrival, jb.delay.now())
}

// readyAtTimestamp is ready for timestamp-addressed playout. When no packet
// carries the timestamp the existing underflow path is preserved.
func (jb *JitterBuffer) readyAtTimestamp(ts uint32) bool {
	if !jb.delay.adapts() {
		return true
	}
	arrival, ok := jb.packets.ArrivalAtTimestamp(ts)
	if !ok {
		return true
	}

	return jb.delay.ready(arrival, jb.delay.now())
}

// PopAtSequence will pop an RTP packet from the jitter buffer at the specified Sequence.
func (jb *JitterBuffer) PopAtSequence(sq uint16) (*rtp.Packet, error) {
	jb.mutex.Lock()
	defer jb.mutex.Unlock()
	if jb.state != Emitting {
		return nil, ErrPopWhileBuffering
	}
	if !jb.ready(sq) {
		return nil, ErrPopNotReady
	}
	packet, err := jb.packets.PopAt(sq)
	if err != nil {
		jb.stats.underflowCount++
		jb.emit(BufferUnderflow)

		return nil, err
	}
	jb.playoutHead = (jb.playoutHead + 1)
	jb.updateState()

	return packet, nil
}

// PeekAtSequence will return an RTP packet from the jitter buffer at the specified Sequence
// without removing it from the buffer.
func (jb *JitterBuffer) PeekAtSequence(sq uint16) (*rtp.Packet, error) {
	jb.mutex.Lock()
	defer jb.mutex.Unlock()
	packet, err := jb.packets.Find(sq)
	if err != nil {
		return nil, err
	}

	return packet, nil
}

// PopAtTimestamp pops an RTP packet from the jitter buffer with the provided timestamp
// Call this method repeatedly to drain the buffer at the timestamp.
func (jb *JitterBuffer) PopAtTimestamp(ts uint32) (*rtp.Packet, error) {
	jb.mutex.Lock()
	defer jb.mutex.Unlock()
	if jb.state != Emitting {
		return nil, ErrPopWhileBuffering
	}
	if !jb.readyAtTimestamp(ts) {
		return nil, ErrPopNotReady
	}
	packet, err := jb.packets.PopAtTimestamp(ts)
	if err != nil {
		jb.stats.underflowCount++
		jb.emit(BufferUnderflow)

		return nil, err
	}
	jb.updateState()

	return packet, nil
}

// Clear will empty the buffer and optionally reset the state.
func (jb *JitterBuffer) Clear(resetState bool) {
	jb.mutex.Lock()
	defer jb.mutex.Unlock()
	jb.packets.Clear()

	if resetState {
		jb.lastSequence = 0
		jb.state = Buffering
		jb.playoutReady = false
		jb.stats = Stats{}
		jb.minStartCount = 50
		jb.delay.reset()
	}
}

// TargetDelay returns the playout delay currently applied by the adaptive
// delay controller. It is the configured minimum when adaptive playout is
// disabled.
func (jb *JitterBuffer) TargetDelay() time.Duration {
	jb.mutex.Lock()
	defer jb.mutex.Unlock()

	return jb.delay.targetDelay
}

// Jitter returns the smoothed interarrival jitter estimate.
func (jb *JitterBuffer) Jitter() time.Duration {
	jb.mutex.Lock()
	defer jb.mutex.Unlock()

	return jb.delay.jitter
}

// SetClockRate updates the RTP media clock rate used by the adaptive delay
// controller. A changed rate discards the existing jitter estimate because it
// was measured against the previous clock.
func (jb *JitterBuffer) SetClockRate(rate uint32) {
	jb.mutex.Lock()
	defer jb.mutex.Unlock()
	jb.delay.setClockRate(rate)
}

// ResetForStream prepares the buffer for a newly bound stream: buffered packets
// and the jitter baseline are discarded, while the configured bounds and clock
// rate are kept.
func (jb *JitterBuffer) ResetForStream() {
	jb.mutex.Lock()
	defer jb.mutex.Unlock()
	jb.packets.Clear()
	jb.state = Buffering
	jb.playoutReady = false
	jb.lastSequence = 0
	jb.stats = Stats{}
	jb.delay.reset()
}
