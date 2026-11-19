// SPDX-FileCopyrightText: 2026 The Pion community <https://pion.ly>
// SPDX-License-Identifier: MIT

package jitterbuffer

import "time"

const (
	// defaultClockRate is used when no media clock rate has been configured.
	defaultClockRate = uint32(90000)
	// defaultStallThreshold is the arrival gap above which a pause is treated as
	// a stream discontinuity instead of jitter.
	defaultStallThreshold = 2 * time.Second
	// defaultReleaseHold is how long the target delay must stay unchallenged
	// before it is allowed to shrink.
	defaultReleaseHold = time.Second

	// jitterGain is the RFC 3550 interarrival jitter gain (1/16).
	jitterGain = int64(16)
	// targetJitterMultiplier scales the smoothed jitter into a playout delay.
	targetJitterMultiplier = int64(4)
	// targetIncreaseDivisor controls how quickly the target delay follows an
	// increase: half of the remaining distance per packet.
	targetIncreaseDivisor = int64(2)
	// targetDecreaseDivisor controls how quickly the target delay shrinks:
	// 1/32 of the remaining distance per packet.
	targetDecreaseDivisor = int64(32)

	// halfSequenceSpace is half of the 16-bit sequence number space. A forward
	// distance larger than this actually points backwards.
	halfSequenceSpace = uint16(1 << 15)
)

// delayController estimates network jitter from packet arrival times and RTP
// timestamps and maintains the playout target delay within configured bounds.
//
// It is not safe for concurrent use; the owning JitterBuffer holds its mutex.
type delayController struct {
	enabled        bool
	minDelay       time.Duration
	maxDelay       time.Duration
	now            func() time.Time
	clockRate      uint32
	stallThreshold time.Duration
	releaseHold    time.Duration

	jitter        time.Duration
	targetDelay   time.Duration
	haveBaseline  bool
	lastArrival   time.Time
	lastTimestamp uint32
	lastIncrease  time.Time
}

func newDelayController() delayController {
	return delayController{
		now:            time.Now,
		clockRate:      defaultClockRate,
		stallThreshold: defaultStallThreshold,
		releaseHold:    defaultReleaseHold,
	}
}

// normalizeDelayBounds makes invalid target delay bounds deterministic:
// negative values are clamped to zero and the maximum is never below the
// minimum.
func normalizeDelayBounds(minDelay, maxDelay time.Duration) (time.Duration, time.Duration) {
	if minDelay < 0 {
		minDelay = 0
	}
	if maxDelay < 0 {
		maxDelay = 0
	}
	if maxDelay < minDelay {
		maxDelay = minDelay
	}

	return minDelay, maxDelay
}

// setClockRate updates the media clock rate. A changed rate invalidates the
// jitter estimate because previous samples were measured with another clock.
func (d *delayController) setClockRate(rate uint32) {
	if rate == 0 {
		rate = defaultClockRate
	}
	if rate == d.clockRate {
		return
	}
	d.clockRate = rate
	d.resetBaseline()
	d.jitter = 0
}

func (d *delayController) resetBaseline() {
	d.haveBaseline = false
	d.lastArrival = time.Time{}
	d.lastTimestamp = 0
}

// reset forgets the jitter estimate and restores the configured minimum delay.
func (d *delayController) reset() {
	d.resetBaseline()
	d.jitter = 0
	d.targetDelay = d.minDelay
	d.lastIncrease = time.Time{}
}

// observe feeds one packet arrival into the RFC 3550 style jitter estimator and
// updates the playout target.
//
// Samples are only taken for packets that advance the RTP timestamp. Reordered
// and duplicate packets neither disturb the estimate nor move the baseline. A
// backward timestamp jump is treated as reordering and ignored; a forward jump
// longer than the stall threshold is treated as a source discontinuity. An
// arrival gap longer than the stall threshold is treated as a stream stall: the
// baseline is restarted and the gap itself is not sampled as jitter.
func (d *delayController) observe(arrival time.Time, timestamp uint32) {
	if !d.enabled {
		return
	}
	if d.clockRate == 0 {
		d.clockRate = defaultClockRate
	}

	sample, ok := d.sample(arrival, timestamp)
	if !ok {
		return
	}

	deviation := sample.gap - sample.media
	if deviation < 0 {
		deviation = -deviation
	}
	d.jitter += time.Duration((int64(deviation) - int64(d.jitter)) / jitterGain)
	d.updateTarget(arrival)
}

// timingSample is one packet pair considered by the jitter estimator.
type timingSample struct {
	gap   time.Duration
	media time.Duration
}

// sample validates a packet against the previous baseline and, when usable,
// advances the baseline and returns the arrival gap and media duration. It
// returns false for gestures that must not influence the estimate: the very
// first packet, reordered or duplicate packets, and stream discontinuities.
func (d *delayController) sample(arrival time.Time, timestamp uint32) (timingSample, bool) {
	if !d.haveBaseline || arrival.Before(d.lastArrival) {
		// The first packet, or a non-monotonic clock: establish a baseline.
		d.startBaseline(arrival, timestamp)

		return timingSample{}, false
	}

	gap := arrival.Sub(d.lastArrival)
	if d.stallThreshold > 0 && gap > d.stallThreshold {
		// A stalled stream carries no timing information. Restart the estimate
		// instead of treating the pause as jitter, and let the target begin
		// decaying immediately rather than waiting out the release hold.
		d.discontinue(arrival, timestamp)

		return timingSample{}, false
	}

	// Unsigned subtraction handles RTP timestamp wrap; the result is the number
	// of samples forward, or a signed backward jump when it exceeds half the
	// timestamp space.
	delta := int64(timestamp - d.lastTimestamp)
	if delta == 0 || delta > 1<<31-1 {
		// A duplicate timestamp (for example another packet of the same video
		// frame) or a backward jump. Neither carries a usable timing sample.
		return timingSample{}, false
	}

	media := samplesToDuration(delta, d.clockRate)
	if d.stallThreshold > 0 && media > d.stallThreshold {
		// The sender skipped ahead by more than a stall: treat it as a
		// discontinuity rather than as a giant jitter sample.
		d.discontinue(arrival, timestamp)

		return timingSample{}, false
	}

	d.lastArrival = arrival
	d.lastTimestamp = timestamp

	return timingSample{gap: gap, media: media}, true
}

// discontinue restarts the estimate after a stream discontinuity.
func (d *delayController) discontinue(arrival time.Time, timestamp uint32) {
	d.startBaseline(arrival, timestamp)
	d.jitter = 0
	d.lastIncrease = time.Time{}
}

func (d *delayController) startBaseline(arrival time.Time, timestamp uint32) {
	d.haveBaseline = true
	d.lastArrival = arrival
	d.lastTimestamp = timestamp
}

// updateTarget moves the target delay towards the value implied by the current
// jitter estimate. Growth is fast so a rising jitter is absorbed before packets
// are lost; shrinkage is slow and only starts after the release hold, so the
// delay does not oscillate with short quiet periods.
func (d *delayController) updateTarget(now time.Time) {
	desired := time.Duration(int64(d.jitter) * targetJitterMultiplier)
	desired = clampDuration(desired, d.minDelay, d.maxDelay)

	switch {
	case desired > d.targetDelay:
		step := time.Duration(int64(desired-d.targetDelay) / targetIncreaseDivisor)
		if step == 0 {
			step = desired - d.targetDelay
		}
		d.targetDelay += step
		d.lastIncrease = now
	case desired < d.targetDelay && (d.lastIncrease.IsZero() || now.Sub(d.lastIncrease) >= d.releaseHold):
		step := time.Duration(int64(d.targetDelay-desired) / targetDecreaseDivisor)
		if step == 0 {
			step = d.targetDelay - desired
		}
		d.targetDelay -= step
	}

	d.targetDelay = clampDuration(d.targetDelay, d.minDelay, d.maxDelay)
}

// ready reports whether a packet that arrived at arrival may be played now.
func (d *delayController) ready(arrival, now time.Time) bool {
	if !d.enabled {
		return true
	}
	if arrival.IsZero() || now.Before(arrival) {
		return d.targetDelay <= 0
	}

	return now.Sub(arrival) >= d.targetDelay
}

func (d *delayController) adapts() bool {
	return d.enabled
}

func clampDuration(value, lower, upper time.Duration) time.Duration {
	if value < lower {
		return lower
	}
	if value > upper {
		return upper
	}

	return value
}

// samplesToDuration converts an RTP timestamp delta in samples to wall time.
// Integer arithmetic keeps the conversion exact, so identical inputs always
// yield identical durations. The delta is bounded by half the 32-bit timestamp
// space, which cannot overflow when scaled to nanoseconds.
func samplesToDuration(samples int64, clockRate uint32) time.Duration {
	if samples == 0 || clockRate == 0 {
		return 0
	}

	return time.Duration(samples) * time.Second / time.Duration(clockRate)
}
