// SPDX-FileCopyrightText: 2026 The Pion community <https://pion.ly>
// SPDX-License-Identifier: MIT

// Package activespeaker provides an interceptor that detects the active
// speaker among incoming RTP audio streams. It parses the RFC 6464 audio
// level header extension (urn:ietf:params:rtp-hdrext:ssrc-audio-level) using
// the extension ID negotiated for each stream, smooths the reported levels
// over a media time window and reports the currently active SSRC together
// with enter/exit events. It is intended for UIs such as voice rooms that
// highlight the avatar of the current speaker.
package activespeaker

import "time"

// EventType describes the kind of an Event.
type EventType int

const (
	// EventEnter is emitted when a stream becomes the active speaker.
	EventEnter EventType = iota
	// EventExit is emitted when a stream stops being the active speaker,
	// either because it fell silent, was unbound or was replaced by a
	// louder stream.
	EventExit
)

// String returns a human readable representation of the EventType.
func (t EventType) String() string {
	switch t {
	case EventEnter:
		return "Enter"
	case EventExit:
		return "Exit"
	default:
		return "Unknown"
	}
}

// Event describes a change of the active speaker. At is the time the change
// was detected, taken from the interceptor's clock (see WithNowFunc).
type Event struct {
	Type EventType
	SSRC uint32
	At   time.Time
}

// Ticker is an interface for *time.Ticker, used with the WithTicker option
// to drive periodic evaluation (e.g. silence detection) in tests.
type Ticker interface {
	Ch() <-chan time.Time
	Stop()
}

type timeTicker struct {
	*time.Ticker
}

func (t *timeTicker) Ch() <-chan time.Time {
	return t.C
}
