// SPDX-FileCopyrightText: 2026 The Pion community <https://pion.ly>
// SPDX-License-Identifier: MIT

package jitterbuffer

import (
	"sync"
	"testing"
	"time"

	"github.com/pion/rtp"
	"github.com/stretchr/testify/assert"
)

const testClockRate = 90000

// stepClock is a deterministic clock: it never advances on its own and only
// moves when the test says so. It is safe for concurrent use because the
// interceptor tests read it from the mock stream goroutine.
type stepClock struct {
	mu  sync.Mutex
	now time.Time
}

func newStepClock() *stepClock {
	return &stepClock{now: time.Unix(1_700_000_000, 0)}
}

func (c *stepClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.now
}

func (c *stepClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// arrivalPacket builds a packet whose timestamp corresponds to offset from the
// stream origin at the given clock rate.
func arrivalPacket(seq uint16, origin uint32, offset time.Duration, clockRate uint32) *rtp.Packet {
	samples := uint32(int64(offset) * int64(clockRate) / int64(time.Second)) //nolint:gosec // G115
	pkt := &rtp.Packet{Header: rtp.Header{SequenceNumber: seq, Timestamp: origin + samples}}
	pkt.Payload = []byte{0x00}

	return pkt
}

func adaptiveBuffer(clock *stepClock, minDelay, maxDelay time.Duration) *JitterBuffer {
	return New(
		WithMinimumPacketCount(1),
		WithClockRate(testClockRate),
		WithNow(clock.Now),
		WithAdaptiveDelay(minDelay, maxDelay),
	)
}

// feedRegular pushes count packets with steady 20 ms arrivals and matching
// media timestamps.
func feedRegular(jb *JitterBuffer, clock *stepClock, startSeq uint16, count int) {
	const step = 20 * time.Millisecond
	feed(jb, clock, startSeq, 0, count, step, step)
}

// feed pushes count packets starting at startSeq whose timestamps advance by
// mediaStep each, with arrivalGap of wall time between arrivals. When the two
// differ the traffic carries jitter.
func feed(
	jb *JitterBuffer, clock *stepClock, startSeq uint16, origin uint32,
	count int, mediaStep, arrivalGap time.Duration,
) {
	for i := range count {
		clock.Advance(arrivalGap)
		jb.Push(arrivalPacket(startSeq+uint16(i), origin, time.Duration(i)*mediaStep, testClockRate)) //nolint:gosec // G115
	}
}

func TestAdaptiveDelayBoundsValidation(t *testing.T) {
	t.Run("negative bounds are clamped", func(t *testing.T) {
		jb := New(WithAdaptiveDelay(-time.Second, -2*time.Second))
		assert.Equal(t, time.Duration(0), jb.delay.minDelay)
		assert.Equal(t, time.Duration(0), jb.delay.maxDelay)
		assert.Equal(t, time.Duration(0), jb.TargetDelay())
	})

	t.Run("max below min is raised to min", func(t *testing.T) {
		jb := New(WithAdaptiveDelay(200*time.Millisecond, 50*time.Millisecond))
		assert.Equal(t, 200*time.Millisecond, jb.delay.minDelay)
		assert.Equal(t, 200*time.Millisecond, jb.delay.maxDelay)
		assert.Equal(t, 200*time.Millisecond, jb.TargetDelay())
	})

	t.Run("zero clock rate falls back to the default", func(t *testing.T) {
		jb := New(WithClockRate(0))
		assert.Equal(t, defaultClockRate, jb.delay.clockRate)
	})
}

func TestAdaptiveDelayTargetBounds(t *testing.T) {
	t.Run("steady arrivals keep the minimum delay", func(t *testing.T) {
		clock := newStepClock()
		jb := adaptiveBuffer(clock, 40*time.Millisecond, 200*time.Millisecond)

		feedRegular(jb, clock, 1000, 200)

		assert.Equal(t, 40*time.Millisecond, jb.TargetDelay())
		assert.LessOrEqual(t, jb.Jitter(), time.Millisecond)
	})

	t.Run("a jitter burst raises the target within bounds", func(t *testing.T) {
		clock := newStepClock()
		jb := adaptiveBuffer(clock, 20*time.Millisecond, 150*time.Millisecond)

		feedRegular(jb, clock, 0, 20)
		baseline := jb.TargetDelay()

		// The next packets arrive late in alternating amounts. Their media
		// timestamps stay on the nominal grid, so the lateness is pure jitter.
		seq := uint16(20)
		origin := uint32(0)
		mediaOffset := 20 * 20 * time.Millisecond
		for i := range 60 {
			jitter := 60 * time.Millisecond
			if i%2 == 0 {
				jitter = 10 * time.Millisecond
			}
			clock.Advance(20*time.Millisecond + jitter)
			pkt := arrivalPacket(seq, origin, mediaOffset, testClockRate)
			seq++
			mediaOffset += 20 * time.Millisecond
			jb.Push(pkt)
		}

		raised := jb.TargetDelay()
		assert.Greater(t, raised, baseline)
		assert.GreaterOrEqual(t, raised, jb.delay.minDelay)
		assert.LessOrEqual(t, raised, jb.delay.maxDelay)
	})

	t.Run("sustained jitter saturates at the maximum", func(t *testing.T) {
		clock := newStepClock()
		jb := adaptiveBuffer(clock, 10*time.Millisecond, 60*time.Millisecond)

		origin := uint32(0)
		mediaOffset := time.Duration(0)
		seq := uint16(0)
		for i := range 400 {
			// Alternate between on-time and very late, well outside the bound.
			gap := 20 * time.Millisecond
			if i%2 == 0 {
				gap = 120 * time.Millisecond
			}
			clock.Advance(gap)
			jb.Push(arrivalPacket(seq, origin, mediaOffset, testClockRate))
			seq++
			mediaOffset += 20 * time.Millisecond
		}

		assert.Equal(t, 60*time.Millisecond, jb.TargetDelay(), "target %v", jb.TargetDelay())
	})

	t.Run("target shrinks only after the release hold", func(t *testing.T) {
		clock := newStepClock()
		jb := adaptiveBuffer(clock, 10*time.Millisecond, 500*time.Millisecond)

		// Raise the target with a burst of jitter.
		seq := uint16(0)
		mediaOffset := time.Duration(0)
		for i := range 120 {
			gap := 20 * time.Millisecond
			if i%2 == 0 {
				gap = 200 * time.Millisecond
			}
			clock.Advance(gap)
			jb.Push(arrivalPacket(seq, 0, mediaOffset, testClockRate))
			seq++
			mediaOffset += 20 * time.Millisecond
		}
		peak := jb.TargetDelay()
		assert.Greater(t, peak, 10*time.Millisecond)

		// Clean arrivals: the target must not move until the hold elapses.
		for i := range 10 {
			clock.Advance(20 * time.Millisecond)
			jb.Push(arrivalPacket(seq, 0, mediaOffset, testClockRate))
			seq++
			mediaOffset += 20 * time.Millisecond
			if time.Duration(i+1)*20*time.Millisecond < jb.delay.releaseHold {
				assert.Equal(t, peak, jb.TargetDelay(), "target moved before release hold")
			}
		}

		// Well past the hold it decays smoothly and stays above the minimum.
		previous := jb.TargetDelay()
		for range 200 {
			clock.Advance(20 * time.Millisecond)
			jb.Push(arrivalPacket(seq, 0, mediaOffset, testClockRate))
			seq++
			mediaOffset += 20 * time.Millisecond
			current := jb.TargetDelay()
			assert.LessOrEqual(t, current, previous)
			previous = current
		}
		assert.Less(t, jb.TargetDelay(), peak)
		assert.GreaterOrEqual(t, jb.TargetDelay(), jb.delay.minDelay)
	})
}

func TestAdaptiveDelayPlayoutReadiness(t *testing.T) {
	t.Run("packets are not emitted before the target elapses", func(t *testing.T) {
		clock := newStepClock()
		jb := adaptiveBuffer(clock, 50*time.Millisecond, 500*time.Millisecond)
		feedRegular(jb, clock, 0, 3)

		assert.Equal(t, Emitting, jb.State())
		// The first packet arrived 40 ms ago (three 20 ms arrivals), so it is
		// still inside its 50 ms playout delay.
		_, err := jb.Pop()
		assert.ErrorIs(t, err, ErrPopNotReady)

		clock.Advance(9 * time.Millisecond)
		_, err = jb.Pop()
		assert.ErrorIs(t, err, ErrPopNotReady)

		clock.Advance(time.Millisecond)
		pkt, err := jb.Pop()
		assert.NoError(t, err)
		assert.Equal(t, uint16(0), pkt.SequenceNumber)
	})

	t.Run("disabled adaptive delay keeps the original behavior", func(t *testing.T) {
		jb := New(WithMinimumPacketCount(1))
		jb.Push(&rtp.Packet{Header: rtp.Header{SequenceNumber: 0}})
		jb.Push(&rtp.Packet{Header: rtp.Header{SequenceNumber: 1}})

		pkt, err := jb.Pop()
		assert.NoError(t, err)
		assert.Equal(t, uint16(0), pkt.SequenceNumber)
	})
}

func TestAdaptiveDelayReorderingAndWrap(t *testing.T) {
	t.Run("reordered packets do not raise jitter", func(t *testing.T) {
		clock := newStepClock()
		jb := adaptiveBuffer(clock, 20*time.Millisecond, 400*time.Millisecond)

		feedRegular(jb, clock, 0, 40)
		before := jb.Jitter()

		// Deliver the next packets out of order: a late duplicate of an older
		// timestamp must be ignored by the estimator.
		clock.Advance(20 * time.Millisecond)
		jb.Push(arrivalPacket(41, 0, 39*20*time.Millisecond, testClockRate))
		clock.Advance(20 * time.Millisecond)
		jb.Push(arrivalPacket(40, 0, 40*20*time.Millisecond, testClockRate))
		clock.Advance(20 * time.Millisecond)
		jb.Push(arrivalPacket(42, 0, 41*20*time.Millisecond, testClockRate))

		assert.LessOrEqual(t, jb.Jitter(), before+5*time.Millisecond)
	})

	t.Run("timestamp wrap is handled as forward progress", func(t *testing.T) {
		clock := newStepClock()
		jb := adaptiveBuffer(clock, 20*time.Millisecond, 400*time.Millisecond)

		origin := uint32(1<<32 - 30000) // 1/3 s before wrap at 90 kHz
		feedRegularLikeWrap(jb, clock, 100, origin, 60, 3000)

		// Wrapping must not be misread as a huge backward jump; the estimate
		// stays small on a steady sequence.
		assert.Less(t, jb.Jitter(), 5*time.Millisecond)
		assert.Equal(t, 20*time.Millisecond, jb.TargetDelay())
	})

	t.Run("sequence number wrap does not disturb playout", func(t *testing.T) {
		clock := newStepClock()
		jb := adaptiveBuffer(clock, 10*time.Millisecond, 200*time.Millisecond)

		// The first packet decides the playout head; start just before the
		// 16-bit sequence number wraps.
		start := uint16(65530)
		feedSamples(jb, clock, start, 0, 30, 900) // 10 ms at 90 kHz
		assert.Equal(t, Emitting, jb.State())
		assert.Equal(t, start, jb.PlayoutHead())

		clock.Advance(10 * time.Millisecond)
		pkt, err := jb.Pop()
		assert.NoError(t, err)
		assert.Equal(t, start, pkt.SequenceNumber)
	})
}

// feedRegularLikeWrap pushes count packets whose timestamps advance by samples
// each, starting at startSeq and origin, with arrival spacing matching the
// media step at the test clock rate.
func feedRegularLikeWrap(
	jb *JitterBuffer, clock *stepClock, startSeq uint16, origin uint32, count int, samples uint32,
) {
	feedSamples(jb, clock, startSeq, origin, count, samples)
}

// feedSamples is feedRegularLikeWrap with an explicit first sequence number,
// used to start just below the 16-bit wrap point.
func feedSamples(
	jb *JitterBuffer, clock *stepClock, startSeq uint16, origin uint32, count int, samples uint32,
) {
	interval := samplesToDuration(int64(samples), testClockRate)
	for i := range count {
		clock.Advance(interval)
		ts := origin + uint32(i)*samples                                                              //nolint:gosec // G115
		jb.Push(&rtp.Packet{Header: rtp.Header{SequenceNumber: startSeq + uint16(i), Timestamp: ts}}) //nolint:gosec // G115
	}
}

func TestAdaptiveDelayStall(t *testing.T) {
	t.Run("long silence resets the estimate without spiking", func(t *testing.T) {
		clock := newStepClock()
		jb := adaptiveBuffer(clock, 20*time.Millisecond, 400*time.Millisecond)

		feedRegular(jb, clock, 0, 50)
		assert.Equal(t, 20*time.Millisecond, jb.TargetDelay())

		// A multi-second pause is a stall, not jitter.
		clock.Advance(5 * time.Second)
		jb.Push(arrivalPacket(50, 0, 50*20*time.Millisecond, testClockRate))

		assert.Equal(t, 20*time.Millisecond, jb.TargetDelay())
		assert.Less(t, jb.Jitter(), time.Millisecond)

		// The next packet continues from the new baseline.
		clock.Advance(20 * time.Millisecond)
		jb.Push(arrivalPacket(51, 0, 51*20*time.Millisecond, testClockRate))
		assert.Equal(t, 20*time.Millisecond, jb.TargetDelay())
	})

	t.Run("a forward timestamp jump beyond the stall threshold is not jitter", func(t *testing.T) {
		clock := newStepClock()
		jb := adaptiveBuffer(clock, 20*time.Millisecond, 400*time.Millisecond)

		feedRegular(jb, clock, 0, 20)
		clock.Advance(20 * time.Millisecond)
		// Timestamp jumps forward by 5 seconds of media time.
		jb.Push(&rtp.Packet{Header: rtp.Header{SequenceNumber: 20, Timestamp: 5 * testClockRate}})

		assert.Equal(t, 20*time.Millisecond, jb.TargetDelay())
	})
}

func TestAdaptiveDelayClockRateChange(t *testing.T) {
	clock := newStepClock()
	jb := adaptiveBuffer(clock, 20*time.Millisecond, 400*time.Millisecond)

	// Build up jitter at 90 kHz.
	seq := uint16(0)
	mediaOffset := time.Duration(0)
	for i := range 60 {
		gap := 20 * time.Millisecond
		if i%2 == 0 {
			gap = 120 * time.Millisecond
		}
		clock.Advance(gap)
		jb.Push(arrivalPacket(seq, 0, mediaOffset, testClockRate))
		seq++
		mediaOffset += 20 * time.Millisecond
	}
	raised := jb.TargetDelay()
	assert.Greater(t, raised, 20*time.Millisecond)

	// Switching the clock rate invalidates the estimate built on the old clock.
	jb.SetClockRate(48000)
	assert.Equal(t, time.Duration(0), jb.Jitter())
	assert.Equal(t, raised, jb.TargetDelay()) // target decays smoothly, not instantly

	// Samples continue with the new rate; steady 48 kHz media at 20 ms leaves
	// the estimate near zero.
	for range 50 {
		clock.Advance(20 * time.Millisecond)
		jb.Push(arrivalPacket(seq, 0, mediaOffset, 48000))
		seq++
		mediaOffset += 20 * time.Millisecond
	}
	assert.Less(t, jb.Jitter(), 5*time.Millisecond)
}

func TestAdaptiveDelayBoundedMemory(t *testing.T) {
	t.Run("a sustained burst never exceeds the configured size", func(t *testing.T) {
		clock := newStepClock()
		jb := New(
			WithMinimumPacketCount(1),
			WithClockRate(testClockRate),
			WithNow(clock.Now),
			WithAdaptiveDelay(50*time.Millisecond, 100*time.Millisecond),
			WithOverflowSize(16),
		)

		// Push packets far faster than the target can drain; nothing is popped.
		for i := range 500 {
			clock.Advance(time.Millisecond)
			jb.Push(arrivalPacket(uint16(i), 0, time.Duration(i)*time.Millisecond, testClockRate)) //nolint:gosec // G115
			assert.LessOrEqual(t, jb.packets.Length(), uint16(16))
		}
		assert.Greater(t, jb.stats.overflowCount, uint32(0))
	})

	t.Run("eviction preserves arrival order of survivors", func(t *testing.T) {
		clock := newStepClock()
		jb := New(
			WithMinimumPacketCount(1),
			WithClockRate(testClockRate),
			WithNow(clock.Now),
			WithAdaptiveDelay(0, time.Second),
			WithOverflowSize(4),
		)

		for i := range 10 {
			clock.Advance(time.Millisecond)
			jb.Push(arrivalPacket(uint16(i), 0, time.Duration(i)*time.Millisecond, testClockRate)) //nolint:gosec // G115
		}

		// The six earliest packets were evicted; the remaining four are the
		// latest arrivals, still in sequence order.
		head, err := jb.Pop()
		assert.NoError(t, err)
		assert.Equal(t, uint16(6), head.SequenceNumber)
	})
}

func TestAdaptiveDelayRepeatedRunsAreDeterministic(t *testing.T) {
	run := func() (time.Duration, []uint16) {
		clock := newStepClock()
		jb := adaptiveBuffer(clock, 10*time.Millisecond, 300*time.Millisecond)

		// A fixed pseudo-random-looking but literal arrival pattern.
		delays := []time.Duration{
			20 * time.Millisecond, 20 * time.Millisecond, 45 * time.Millisecond,
			5 * time.Millisecond, 20 * time.Millisecond, 80 * time.Millisecond,
			10 * time.Millisecond, 20 * time.Millisecond, 20 * time.Millisecond,
			30 * time.Millisecond, 120 * time.Millisecond, 10 * time.Millisecond,
			20 * time.Millisecond, 20 * time.Millisecond, 5 * time.Millisecond,
			60 * time.Millisecond, 20 * time.Millisecond, 20 * time.Millisecond,
			20 * time.Millisecond, 20 * time.Millisecond,
		}
		seq := uint16(0)
		mediaOffset := time.Duration(0)
		for range 5 {
			for _, extra := range delays {
				clock.Advance(extra)
				jb.Push(arrivalPacket(seq, 0, mediaOffset, testClockRate))
				seq++
				mediaOffset += 20 * time.Millisecond
			}
		}

		order := make([]uint16, 0, 100)
		for {
			pkt, err := jb.Pop()
			if err != nil {
				break
			}
			order = append(order, pkt.SequenceNumber)
		}

		return jb.TargetDelay(), order
	}

	firstDelay, firstOrder := run()
	secondDelay, secondOrder := run()

	assert.Equal(t, firstDelay, secondDelay)
	assert.Equal(t, firstOrder, secondOrder)
}

func TestAdaptiveDelayPlayoutOrder(t *testing.T) {
	clock := newStepClock()
	jb := adaptiveBuffer(clock, 25*time.Millisecond, 300*time.Millisecond)

	// Push in a shuffled order with deterministic arrival times. The first
	// packet determines the playout head, so start with the lowest sequence.
	order := []uint16{0, 3, 5, 1, 4, 2, 7, 6}
	for _, seq := range order {
		clock.Advance(20 * time.Millisecond)
		jb.Push(arrivalPacket(seq, 0, time.Duration(seq)*20*time.Millisecond, testClockRate))
	}

	// Give every queued packet more than the target delay.
	clock.Advance(time.Second)

	popped := make([]uint16, 0, len(order))
	for range order {
		pkt, err := jb.Pop()
		assert.NoError(t, err)
		popped = append(popped, pkt.SequenceNumber)
	}

	assert.Equal(t, []uint16{0, 1, 2, 3, 4, 5, 6, 7}, popped)
}

func TestAdaptiveDelayLatePackets(t *testing.T) {
	t.Run("a packet behind the playout head is dropped", func(t *testing.T) {
		clock := newStepClock()
		jb := adaptiveBuffer(clock, 10*time.Millisecond, 200*time.Millisecond)

		feedRegular(jb, clock, 0, 4)
		clock.Advance(20 * time.Millisecond)
		pkt, err := jb.Pop()
		assert.NoError(t, err)
		assert.Equal(t, uint16(0), pkt.SequenceNumber)
		assert.Equal(t, uint16(1), jb.PlayoutHead())

		// A packet for the already played slot is refused, not queued.
		clock.Advance(time.Millisecond)
		jb.Push(arrivalPacket(0, 0, 0, testClockRate))

		assert.Equal(t, uint16(1), jb.PlayoutHead())
		assert.Equal(t, uint32(1), jb.stats.lateCount)
		assert.Equal(t, uint16(3), jb.packets.Length())
	})

	t.Run("late packets are kept when adaptive delay is disabled", func(t *testing.T) {
		jb := New(WithMinimumPacketCount(1))
		for seq := range uint16(4) {
			jb.Push(&rtp.Packet{Header: rtp.Header{SequenceNumber: seq}})
		}
		_, err := jb.Pop()
		assert.NoError(t, err)
		jb.Push(&rtp.Packet{Header: rtp.Header{SequenceNumber: 0}})

		// The original Push contract is unchanged: nothing is dropped.
		assert.Equal(t, uint16(4), jb.packets.Length())
		assert.Equal(t, uint32(0), jb.stats.lateCount)
	})
}

func TestAdaptiveDelayEvictionKeepsPlayoutMoving(t *testing.T) {
	clock := newStepClock()
	jb := New(
		WithMinimumPacketCount(1),
		WithClockRate(testClockRate),
		WithNow(clock.Now),
		WithAdaptiveDelay(0, time.Second),
		WithOverflowSize(4),
	)

	// One packet starts buffering; the burst then overflows the queue.
	jb.Push(arrivalPacket(0, 0, 0, testClockRate))
	clock.Advance(time.Millisecond)
	feed(jb, clock, 1, 0, 10, time.Millisecond, time.Millisecond)

	// The head must have advanced past every evicted packet rather than
	// pointing at a hole the queue no longer holds.
	head := jb.PlayoutHead()
	for range 10 {
		pkt, err := jb.Pop()
		if err != nil {
			break
		}
		assert.GreaterOrEqual(t, pkt.SequenceNumber, head)
		head = jb.PlayoutHead()
	}
}

func TestAdaptiveDelayOptions(t *testing.T) {
	t.Run("stall threshold is configurable", func(t *testing.T) {
		clock := newStepClock()
		jb := New(
			WithMinimumPacketCount(1),
			WithClockRate(testClockRate),
			WithNow(clock.Now),
			WithAdaptiveDelay(10*time.Millisecond, 500*time.Millisecond),
			WithStallThreshold(30*time.Millisecond),
		)

		feedRegular(jb, clock, 0, 10)
		assert.Equal(t, 10*time.Millisecond, jb.TargetDelay())

		// A 50 ms gap now exceeds the lowered threshold and is a stall.
		clock.Advance(50 * time.Millisecond)
		jb.Push(arrivalPacket(10, 0, 10*20*time.Millisecond, testClockRate))
		assert.Less(t, jb.Jitter(), time.Millisecond)
	})

	t.Run("release hold is configurable", func(t *testing.T) {
		clock := newStepClock()
		jb := New(
			WithMinimumPacketCount(1),
			WithClockRate(testClockRate),
			WithNow(clock.Now),
			WithAdaptiveDelay(10*time.Millisecond, 500*time.Millisecond),
			WithReleaseHold(0),
		)

		seq := uint16(0)
		mediaOffset := time.Duration(0)
		for i := range 80 {
			gap := 20 * time.Millisecond
			if i%2 == 0 {
				gap = 150 * time.Millisecond
			}
			clock.Advance(gap)
			jb.Push(arrivalPacket(seq, 0, mediaOffset, testClockRate))
			seq++
			mediaOffset += 20 * time.Millisecond
		}
		peak := jb.TargetDelay()
		assert.Greater(t, peak, 10*time.Millisecond)

		// With no hold the target may shrink on the very next clean packet.
		clock.Advance(20 * time.Millisecond)
		jb.Push(arrivalPacket(seq, 0, mediaOffset, testClockRate))
		assert.Less(t, jb.TargetDelay(), peak)
	})

	t.Run("zero overflow size keeps the default", func(t *testing.T) {
		jb := New(WithOverflowSize(0))
		assert.Equal(t, uint16(100), jb.overflowLen)
	})
}

func TestAdaptiveDelayPopAtSequenceAndTimestamp(t *testing.T) {
	t.Run("PopAtSequence respects the target delay", func(t *testing.T) {
		clock := newStepClock()
		jb := adaptiveBuffer(clock, 50*time.Millisecond, 500*time.Millisecond)
		feedRegular(jb, clock, 0, 3)

		// Packet 2 is the newest arrival: none of its 50 ms target has elapsed.
		_, err := jb.PopAtSequence(2)
		assert.ErrorIs(t, err, ErrPopNotReady)

		clock.Advance(50 * time.Millisecond)
		pkt, err := jb.PopAtSequence(2)
		assert.NoError(t, err)
		assert.Equal(t, uint16(2), pkt.SequenceNumber)
	})

	t.Run("PopAtTimestamp respects the target delay", func(t *testing.T) {
		clock := newStepClock()
		jb := adaptiveBuffer(clock, 50*time.Millisecond, 500*time.Millisecond)
		feedRegular(jb, clock, 0, 3)

		// The last packet has only just arrived.
		ts := arrivalPacket(2, 0, 40*time.Millisecond, testClockRate).Timestamp
		_, err := jb.PopAtTimestamp(ts)
		assert.ErrorIs(t, err, ErrPopNotReady)

		clock.Advance(50 * time.Millisecond)
		pkt, err := jb.PopAtTimestamp(ts)
		assert.NoError(t, err)
		assert.Equal(t, uint16(2), pkt.SequenceNumber)
	})
}

func TestAdaptiveDelaySequenceHelpers(t *testing.T) {
	assert.True(t, IsAfter(5, 4))
	assert.True(t, IsAfter(0, 65535))
	assert.False(t, IsAfter(4, 4))
	assert.False(t, IsAfter(4, 5))

	assert.True(t, IsBefore(4, 5))
	assert.True(t, IsBefore(65535, 0))
	assert.False(t, IsBefore(5, 5))
	assert.False(t, IsBefore(5, 4))
}
