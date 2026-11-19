// SPDX-FileCopyrightText: 2026 The Pion community <https://pion.ly>
// SPDX-License-Identifier: MIT

package activespeaker

import "time"

// sample is a single audio level observation taken from one RTP packet.
type sample struct {
	rtpTimestamp uint32
	sequence     uint16
	level        uint8
}

// streamState holds the detection state of a single bound stream. All
// fields are guarded by the interceptor's mutex.
type streamState struct {
	ssrc         uint32
	extID        uint8
	hasExt       bool
	windowTicks  uint32
	samples      []sample
	maxTS        uint32
	hasMaxTS     bool
	speaking     bool
	lastPacketAt time.Time
}

// addSample records one packet. Samples are windowed by RTP (media)
// timestamp, so packets arriving out of order produce the same smoothed
// level as in-order delivery; duplicates (retransmissions) are dropped. A
// timestamp far in the past is treated as a stream restart (e.g. SSRC
// reuse) and resets the window.
func (s *streamState) addSample(sequence uint16, ts uint32, level uint8) {
	if s.hasMaxTS && int32(s.maxTS-ts) > int32(s.windowTicks) { //nolint:gosec // windowTicks is small
		s.reset()
	}
	for _, existing := range s.samples {
		if existing.sequence == sequence {
			return
		}
	}
	if !s.hasMaxTS || int32(ts-s.maxTS) > 0 { //nolint:gosec // wrap-around aware RTP timestamp delta
		s.maxTS = ts
		s.hasMaxTS = true
	}
	s.samples = append(s.samples, sample{rtpTimestamp: ts, sequence: sequence, level: level})
	s.evict()
}

// reset drops all samples after a detected stream restart.
func (s *streamState) reset() {
	s.samples = s.samples[:0]
	s.hasMaxTS = false
}

// evict drops samples that fell out of the window ending at maxTS.
func (s *streamState) evict() {
	cutoff := s.maxTS - s.windowTicks
	kept := s.samples[:0]
	for _, sm := range s.samples {
		if int32(sm.rtpTimestamp-cutoff) >= 0 { //nolint:gosec // wrap-around aware RTP timestamp delta
			kept = append(kept, sm)
		}
	}
	s.samples = kept
}

// averageLevel returns the smoothed audio level over the current window, or
// false if the window holds no samples.
func (s *streamState) averageLevel() (float64, bool) {
	if len(s.samples) == 0 {
		return 0, false
	}
	sum := 0
	for _, sm := range s.samples {
		sum += int(sm.level)
	}

	return float64(sum) / float64(len(s.samples)), true
}
