// SPDX-FileCopyrightText: 2026 The Pion community <https://pion.ly>
// SPDX-License-Identifier: MIT

package activespeaker

import (
	"errors"
	"time"
)

var (
	// ErrThresholdOutOfRange is returned when an audio level threshold outside [0, 127] is configured.
	ErrThresholdOutOfRange = errors.New("activespeaker: audio level threshold must be in range [0, 127]")
	// ErrReleaseBelowActive is returned when the release threshold is lower than the active threshold.
	ErrReleaseBelowActive = errors.New("activespeaker: release threshold must be >= active threshold")
	// ErrInvalidDuration is returned when a non-positive duration is configured.
	ErrInvalidDuration = errors.New("activespeaker: duration must be greater than zero")
	// ErrInvalidExtensionID is returned when a header extension ID outside [1, 255] is configured.
	ErrInvalidExtensionID = errors.New("activespeaker: header extension ID must be in range [1, 255]")
)

// Option can be used to configure the Interceptor.
type Option func(*Interceptor) error

// WithActiveThreshold sets the smoothed audio level (in -dBov, 0 = loudest,
// 127 = silence) at or below which a stream is considered to be speaking.
// Default is 40.
func WithActiveThreshold(level uint8) Option {
	return func(i *Interceptor) error {
		if level > 127 {
			return ErrThresholdOutOfRange
		}
		i.activeThreshold = level

		return nil
	}
}

// WithReleaseThreshold sets the smoothed audio level above which a speaking
// stream is considered to have stopped. It must be greater than or equal to
// the active threshold; the gap between the two provides hysteresis so a
// speaker hovering near the threshold does not flap. Default is 50.
func WithReleaseThreshold(level uint8) Option {
	return func(i *Interceptor) error {
		if level > 127 {
			return ErrThresholdOutOfRange
		}
		i.releaseThreshold = level

		return nil
	}
}

// WithVoiceActivityRequired controls whether only packets with the RFC 6464
// V (voice activity) bit set contribute to the smoothed level. Default is
// false, because many senders do not implement the V bit.
func WithVoiceActivityRequired(required bool) Option {
	return func(i *Interceptor) error {
		i.voiceRequired = required

		return nil
	}
}

// WithWindowDuration sets the media time window over which audio levels are
// averaged. Default is 300ms.
func WithWindowDuration(d time.Duration) Option {
	return func(i *Interceptor) error {
		if d <= 0 {
			return ErrInvalidDuration
		}
		i.window = d

		return nil
	}
}

// WithSilenceTimeout sets how long a stream may go without any RTP packets
// before it is considered silent. Default is 1s.
func WithSilenceTimeout(d time.Duration) Option {
	return func(i *Interceptor) error {
		if d <= 0 {
			return ErrInvalidDuration
		}
		i.silenceTimeout = d

		return nil
	}
}

// WithHangover sets the minimum time the active speaker keeps its role
// before a louder stream may take over. It prevents the highlight from
// jumping between participants during short pauses. A zero duration allows
// immediate switches. Default is 500ms.
func WithHangover(d time.Duration) Option {
	return func(i *Interceptor) error {
		if d < 0 {
			return ErrInvalidDuration
		}
		i.hangover = d

		return nil
	}
}

// WithEvaluationInterval sets the interval at which the interceptor
// re-evaluates speaker state (e.g. to detect silence) when no packets
// arrive. Default is 100ms.
func WithEvaluationInterval(d time.Duration) Option {
	return func(i *Interceptor) error {
		if d <= 0 {
			return ErrInvalidDuration
		}
		i.evalInterval = d

		return nil
	}
}

// WithAudioLevelExtensionID overrides the audio level header extension ID
// for all streams instead of looking it up in StreamInfo.RTPHeaderExtensions.
func WithAudioLevelExtensionID(id uint8) Option {
	return func(i *Interceptor) error {
		if id == 0 {
			return ErrInvalidExtensionID
		}
		i.extIDOverride = int(id)

		return nil
	}
}

// WithEventHandler sets the callback invoked with enter/exit events whenever
// the active speaker changes. The handler is called from the goroutine that
// processed the triggering packet or tick, after the interceptor's lock has
// been released, so it may call ActiveSpeaker.
func WithEventHandler(handler func(Event)) Option {
	return func(i *Interceptor) error {
		i.handler = handler

		return nil
	}
}

// WithTicker replaces the ticker used for periodic evaluation. This is
// mostly useful for testing.
func WithTicker(ticker Ticker) Option {
	return func(i *Interceptor) error {
		i.ticker = ticker

		return nil
	}
}

// WithNowFunc sets the function the interceptor uses to get the current
// time. This is mostly useful for testing.
func WithNowFunc(now func() time.Time) Option {
	return func(i *Interceptor) error {
		i.now = now

		return nil
	}
}
