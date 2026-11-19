// SPDX-FileCopyrightText: 2026 The Pion community <https://pion.ly>
// SPDX-License-Identifier: MIT

package activespeaker

import (
	"sync"
	"time"

	"github.com/pion/interceptor"
	"github.com/pion/rtp"
)

// AudioLevelURI is the URI of the RFC 6464 audio level header extension. The
// interceptor looks it up in StreamInfo.RTPHeaderExtensions to find the
// extension ID negotiated for each stream.
const AudioLevelURI = "urn:ietf:params:rtp-hdrext:ssrc-audio-level"

const (
	defaultActiveThreshold  = 40
	defaultReleaseThreshold = 50
	defaultWindow           = 300 * time.Millisecond
	defaultSilenceTimeout   = time.Second
	defaultHangover         = 500 * time.Millisecond
	defaultEvalInterval     = 100 * time.Millisecond
	maxExtensionID          = 255
)

// InterceptorFactory is an interceptor.Factory for the active speaker
// Interceptor.
type InterceptorFactory struct {
	opts []Option
}

// NewInterceptor creates a new InterceptorFactory.
func NewInterceptor(opts ...Option) (*InterceptorFactory, error) {
	return &InterceptorFactory{opts: opts}, nil
}

// NewInterceptor creates a new Interceptor.
func (f *InterceptorFactory) NewInterceptor(_ string) (interceptor.Interceptor, error) {
	ic := &Interceptor{
		now:              time.Now,
		activeThreshold:  defaultActiveThreshold,
		releaseThreshold: defaultReleaseThreshold,
		window:           defaultWindow,
		silenceTimeout:   defaultSilenceTimeout,
		hangover:         defaultHangover,
		evalInterval:     defaultEvalInterval,
		extIDOverride:    -1,
		streams:          map[uint32]*streamState{},
		stop:             make(chan struct{}),
	}
	for _, opt := range f.opts {
		if err := opt(ic); err != nil {
			return nil, err
		}
	}
	if ic.releaseThreshold < ic.activeThreshold {
		return nil, ErrReleaseBelowActive
	}
	ic.start()

	return ic, nil
}

// Interceptor detects the active speaker among the bound remote streams. It
// passes all packets through unmodified.
//
// Behavior in edge cases is deliberately simple and deterministic:
//   - streams without a negotiated audio level extension, packets without
//     the extension and packets whose extension payload fails to parse are
//     ignored for speaker detection;
//   - samples are windowed by RTP timestamp, so packet reordering does not
//     change the outcome; an RTP timestamp far in the past (e.g. SSRC reuse)
//     resets the stream's window;
//   - a stream that sends no packets for the silence timeout stops speaking;
//     when every stream is silent there is no active speaker;
//   - unbinding a stream removes it, emitting an exit event if it was the
//     active speaker.
type Interceptor struct {
	interceptor.NoOp

	now              func() time.Time
	activeThreshold  uint8
	releaseThreshold uint8
	voiceRequired    bool
	window           time.Duration
	silenceTimeout   time.Duration
	hangover         time.Duration
	evalInterval     time.Duration
	extIDOverride    int
	handler          func(Event)
	ticker           Ticker

	mu          sync.Mutex
	streams     map[uint32]*streamState
	active      uint32
	hasActive   bool
	activeSince time.Time

	closeOnce sync.Once
	stop      chan struct{}
	wg        sync.WaitGroup
}

// ActiveSpeaker returns the SSRC of the current active speaker, or false if
// there is none (e.g. everyone is silent).
func (i *Interceptor) ActiveSpeaker() (uint32, bool) {
	i.mu.Lock()
	defer i.mu.Unlock()

	return i.active, i.hasActive
}

// BindRemoteStream lets you modify any incoming RTP packets. It is called
// once per RemoteStream. The returned method is called once per RTP packet.
func (i *Interceptor) BindRemoteStream(
	info *interceptor.StreamInfo, reader interceptor.RTPReader,
) interceptor.RTPReader {
	st := i.addStream(info)

	return interceptor.RTPReaderFunc(
		func(buf []byte, attributes interceptor.Attributes) (int, interceptor.Attributes, error) {
			n, attributes, err := reader.Read(buf, attributes)
			if err != nil {
				return 0, attributes, err
			}
			i.processPacket(st, buf[:n], attributes)

			return n, attributes, nil
		},
	)
}

// UnbindRemoteStream is called when the stream is removed. It drops the
// stream's detection state, emitting an exit event if it was the active
// speaker.
func (i *Interceptor) UnbindRemoteStream(info *interceptor.StreamInfo) {
	i.mu.Lock()
	delete(i.streams, info.SSRC)
	events := i.evaluateLocked(i.now())
	i.mu.Unlock()
	i.emit(events)
}

// Close stops the periodic evaluation goroutine.
func (i *Interceptor) Close() error {
	i.closeOnce.Do(func() {
		close(i.stop)
	})
	i.wg.Wait()

	return nil
}

func (i *Interceptor) start() {
	ticker := i.ticker
	if ticker == nil {
		ticker = &timeTicker{time.NewTicker(i.evalInterval)}
	}
	i.wg.Add(1)
	go func() {
		defer i.wg.Done()
		for {
			select {
			case <-i.stop:
				ticker.Stop()

				return
			case <-ticker.Ch():
				i.evaluate(i.now())
			}
		}
	}()
}

func (i *Interceptor) addStream(info *interceptor.StreamInfo) *streamState {
	extID, hasExt := i.extensionID(info)
	windowTicks := uint32(uint64(i.window) * uint64(info.ClockRate) / uint64(time.Second)) //nolint:gosec // positive by validation
	if windowTicks == 0 {
		windowTicks = 1
	}
	st := &streamState{
		ssrc:         info.SSRC,
		extID:        extID,
		hasExt:       hasExt,
		windowTicks:  windowTicks,
		lastPacketAt: i.now(),
	}

	i.mu.Lock()
	// Binding a stream for an SSRC that is still registered means the SSRC
	// was reused: drop the stale state so the new stream starts fresh.
	delete(i.streams, info.SSRC)
	i.streams[info.SSRC] = st
	events := i.evaluateLocked(i.now())
	i.mu.Unlock()
	i.emit(events)

	return st
}

func (i *Interceptor) extensionID(info *interceptor.StreamInfo) (uint8, bool) {
	if i.extIDOverride > 0 {
		return uint8(i.extIDOverride), true //nolint:gosec // validated to be in [1, 255]
	}
	for _, ext := range info.RTPHeaderExtensions {
		if ext.URI == AudioLevelURI && ext.ID >= 1 && ext.ID <= maxExtensionID {
			return uint8(ext.ID), true
		}
	}

	return 0, false
}

func (i *Interceptor) processPacket(st *streamState, raw []byte, attributes interceptor.Attributes) {
	if !st.hasExt {
		return
	}
	header, err := attributes.GetRTPHeader(raw)
	if err != nil {
		return
	}
	payload := header.GetExtension(st.extID)
	if payload == nil {
		return
	}
	ext := rtp.AudioLevelExtension{}
	if err := ext.Unmarshal(payload); err != nil {
		return
	}

	now := i.now()
	i.mu.Lock()
	st.lastPacketAt = now
	if !i.voiceRequired || ext.Voice {
		st.addSample(header.SequenceNumber, header.Timestamp, ext.Level)
	}
	events := i.evaluateLocked(now)
	i.mu.Unlock()
	i.emit(events)
}

func (i *Interceptor) evaluate(now time.Time) {
	i.mu.Lock()
	events := i.evaluateLocked(now)
	i.mu.Unlock()
	i.emit(events)
}

// evaluateLocked recomputes the speaking state of every stream and the
// active speaker. It returns the events that resulted from the change; the
// caller must emit them after releasing the lock.
func (i *Interceptor) evaluateLocked(now time.Time) []Event {
	for _, st := range i.streams {
		i.updateSpeakingLocked(st, now)
	}

	var events []Event
	if i.hasActive && !i.activeSpeakingLocked() {
		events = append(events, Event{Type: EventExit, SSRC: i.active, At: now})
		i.hasActive = false
	}

	return i.selectActiveLocked(now, events)
}

func (i *Interceptor) activeSpeakingLocked() bool {
	current, ok := i.streams[i.active]

	return ok && current.speaking
}

// selectActiveLocked picks the active speaker among the speaking streams:
// the loudest one if there is no incumbent, otherwise the incumbent keeps
// its role until its hangover expired and a strictly louder stream
// challenges it.
func (i *Interceptor) selectActiveLocked(now time.Time, events []Event) []Event {
	loudest, loudestAvg, found := i.loudestLocked()
	if !i.hasActive {
		if found {
			i.setActiveLocked(loudest, now)
			events = append(events, Event{Type: EventEnter, SSRC: loudest, At: now})
		}

		return events
	}
	if !found || loudest == i.active || now.Sub(i.activeSince) < i.hangover {
		return events
	}
	if currentAvg, ok := i.streams[i.active].averageLevel(); ok && loudestAvg < currentAvg {
		events = append(events, Event{Type: EventExit, SSRC: i.active, At: now})
		i.setActiveLocked(loudest, now)
		events = append(events, Event{Type: EventEnter, SSRC: loudest, At: now})
	}

	return events
}

// updateSpeakingLocked applies the enter/release hysteresis and the silence
// timeout to a single stream.
func (i *Interceptor) updateSpeakingLocked(st *streamState, now time.Time) {
	avg, ok := st.averageLevel()
	switch {
	case !ok:
		st.speaking = false
	case st.speaking && avg > float64(i.releaseThreshold):
		st.speaking = false
	case !st.speaking && avg <= float64(i.activeThreshold):
		st.speaking = true
	}
	if st.speaking && now.Sub(st.lastPacketAt) > i.silenceTimeout {
		st.speaking = false
	}
}

// loudestLocked returns the speaking stream with the lowest (loudest)
// smoothed level. Ties are broken by SSRC so the result is deterministic.
func (i *Interceptor) loudestLocked() (ssrc uint32, avg float64, found bool) {
	for id, st := range i.streams {
		if !st.speaking {
			continue
		}
		level, ok := st.averageLevel()
		if !ok {
			continue
		}
		if !found || level < avg || (level == avg && id < ssrc) {
			ssrc, avg, found = id, level, true
		}
	}

	return ssrc, avg, found
}

func (i *Interceptor) setActiveLocked(ssrc uint32, now time.Time) {
	i.active = ssrc
	i.hasActive = true
	i.activeSince = now
}

func (i *Interceptor) emit(events []Event) {
	if i.handler == nil {
		return
	}
	for _, ev := range events {
		i.handler(ev)
	}
}
