// SPDX-FileCopyrightText: 2026 The Pion community <https://pion.ly>
// SPDX-License-Identifier: MIT

package jitterbuffer

import (
	"errors"
	"time"

	"github.com/pion/rtp"
)

// PriorityQueue provides a linked list sorting of RTP packets by SequenceNumber.
type PriorityQueue struct {
	next   *node
	length uint16
}

type node struct {
	val      *rtp.Packet
	next     *node
	prev     *node
	priority uint16
	arrival  time.Time
}

var (
	// ErrInvalidOperation may be returned if a Pop or Find operation is performed on an empty queue.
	ErrInvalidOperation = errors.New("attempt to find or pop on an empty list")
	// ErrNotFound will be returned if the packet cannot be found in the queue.
	ErrNotFound = errors.New("priority not found")
)

// NewQueue will create a new PriorityQueue whose order relies on monotonically
// increasing Sequence Number, wrapping at MaxUint16, so
// a packet with sequence number MaxUint16 - 1 will be after 0.
func NewQueue() *PriorityQueue {
	return &PriorityQueue{
		next:   nil,
		length: 0,
	}
}

func newNode(val *rtp.Packet, priority uint16) *node {
	return &node{
		val:      val,
		prev:     nil,
		next:     nil,
		priority: priority,
	}
}

// Find a packet in the queue with the provided sequence number,
// regardless of position (the packet is retained in the queue).
func (q *PriorityQueue) Find(sqNum uint16) (*rtp.Packet, error) {
	next := q.next
	for next != nil {
		if next.priority == sqNum {
			return next.val, nil
		}
		next = next.next
	}

	return nil, ErrNotFound
}

// Arrival returns the recorded arrival time of the packet with the provided
// sequence number.
func (q *PriorityQueue) Arrival(sqNum uint16) (time.Time, bool) {
	for next := q.next; next != nil; next = next.next {
		if next.priority == sqNum {
			return next.arrival, true
		}
	}

	return time.Time{}, false
}

// PopOldestArrival removes the packet that arrived earliest. Packets without a
// recorded arrival carry the zero time, so they count as the oldest. When
// several packets tie, the one closest to the head of the queue is removed,
// keeping eviction deterministic. It returns ErrInvalidOperation for an empty
// queue.
func (q *PriorityQueue) PopOldestArrival() (*rtp.Packet, error) {
	if q.next == nil {
		return nil, ErrInvalidOperation
	}

	victim := q.next
	for next := q.next.next; next != nil; next = next.next {
		if next.arrival.Before(victim.arrival) {
			victim = next
		}
	}

	return q.removeNode(victim)
}

// IsAfter reports whether sq lies strictly ahead of ref in 16-bit sequence
// order, treating a forward distance of half the sequence space or more as
// behind.
func IsAfter(sq, ref uint16) bool {
	distance := sq - ref

	return distance != 0 && distance < halfSequenceSpace
}

// IsBefore reports whether sq lies strictly behind ref in 16-bit sequence
// order. A distance of exactly half the sequence space is treated as behind.
func IsBefore(sq, ref uint16) bool {
	distance := ref - sq

	return distance != 0 && distance < halfSequenceSpace
}

// ArrivalAtTimestamp returns the recorded arrival time of the first packet
// (in queue order) carrying the given RTP timestamp.
func (q *PriorityQueue) ArrivalAtTimestamp(timestamp uint32) (time.Time, bool) {
	for next := q.next; next != nil; next = next.next {
		if next.val.Timestamp == timestamp {
			return next.arrival, true
		}
	}

	return time.Time{}, false
}

// NextFrom returns the sequence number closest ahead of sq (in sequence order,
// wrapping at 16 bits) that is present in the queue. A packet whose signed
// distance from sq is negative lies behind sq and is not considered.
func (q *PriorityQueue) NextFrom(sq uint16) (uint16, bool) {
	var (
		found bool
		best  uint16
		bestD uint16
	)
	for next := q.next; next != nil; next = next.next {
		distance := next.priority - sq
		if distance >= halfSequenceSpace {
			// Lies behind sq.
			continue
		}
		if !found || distance < bestD {
			found = true
			best = next.priority
			bestD = distance
		}
	}

	return best, found
}

// removeNode unlinks and returns the packet held by n.
func (q *PriorityQueue) removeNode(n *node) (*rtp.Packet, error) {
	if n.prev != nil {
		n.prev.next = n.next
	} else {
		q.next = n.next
	}
	if n.next != nil {
		n.next.prev = n.prev
	}
	n.next = nil
	n.prev = nil
	val := n.val
	n.val = nil
	q.length--

	return val, nil
}

// Push will insert a packet in to the queue in order of sequence number.
func (q *PriorityQueue) Push(val *rtp.Packet, priority uint16) {
	q.PushArrival(val, priority, time.Time{})
}

// PushArrival is Push with the packet's arrival time recorded. The arrival is
// used by adaptive playout to decide when the packet may be emitted and is
// returned by Arrival.
func (q *PriorityQueue) PushArrival(val *rtp.Packet, priority uint16, arrival time.Time) {
	newPq := newNode(val, priority)
	newPq.arrival = arrival

	// Insert at head: empty list or new node has smallest-or-equal priority.
	// Using <= ensures equal-priority elements are consistently placed before
	// all existing equals, matching the traversal loop's break-on-equal behavior.
	cur := q.next
	if cur == nil || priority <= cur.priority {
		newPq.next = cur
		if cur != nil {
			cur.prev = newPq
		}
		q.next = newPq
		q.length++

		return
	}

	// Traverse to find insertion point; cur is guaranteed non-nil here.
	prev := cur
	for cur = cur.next; cur != nil && priority > cur.priority; cur = cur.next {
		prev = cur
	}

	prev.next = newPq
	newPq.prev = prev
	newPq.next = cur
	if cur != nil {
		cur.prev = newPq
	}
	q.length++
}

// Length will get the total length of the queue.
func (q *PriorityQueue) Length() uint16 {
	return q.length
}

// Pop removes the first element from the queue, regardless
// sequence number.
func (q *PriorityQueue) Pop() (*rtp.Packet, error) {
	head := q.next
	if head == nil {
		return nil, ErrInvalidOperation
	}
	q.next = head.next
	if head.next != nil {
		head.next.prev = nil
	}
	head.next = nil
	val := head.val
	head.val = nil
	q.length--

	return val, nil
}

// PopAt removes an element at the specified sequence number (priority).
func (q *PriorityQueue) PopAt(sqNum uint16) (*rtp.Packet, error) {
	head := q.next
	if head == nil {
		return nil, ErrInvalidOperation
	}
	if head.priority == sqNum {
		q.next = head.next
		if head.next != nil {
			head.next.prev = nil
		}
		head.next = nil
		val := head.val
		head.val = nil
		q.length--

		return val, nil
	}
	// prev is guaranteed non-nil since head didn't match.
	prev := head
	for pos := head.next; pos != nil; pos = pos.next {
		if pos.priority == sqNum {
			prev.next = pos.next
			if pos.next != nil {
				pos.next.prev = prev
			}
			pos.next = nil
			pos.prev = nil
			val := pos.val
			pos.val = nil
			q.length--

			return val, nil
		}
		prev = pos
	}

	return nil, ErrNotFound
}

// PopAtTimestamp removes and returns a packet at the given RTP Timestamp, regardless
// sequence number order.
func (q *PriorityQueue) PopAtTimestamp(timestamp uint32) (*rtp.Packet, error) {
	head := q.next
	if head == nil {
		return nil, ErrInvalidOperation
	}
	if head.val.Timestamp == timestamp {
		q.next = head.next
		if head.next != nil {
			head.next.prev = nil
		}
		head.next = nil
		val := head.val
		head.val = nil
		q.length--

		return val, nil
	}
	// prev is guaranteed non-nil since head didn't match.
	prev := head
	for pos := head.next; pos != nil; pos = pos.next {
		if pos.val.Timestamp == timestamp {
			prev.next = pos.next
			if pos.next != nil {
				pos.next.prev = prev
			}
			pos.next = nil
			pos.prev = nil
			val := pos.val
			pos.val = nil
			q.length--

			return val, nil
		}
		prev = pos
	}

	return nil, ErrNotFound
}

// Clear will empty a PriorityQueue.
func (q *PriorityQueue) Clear() {
	q.next = nil
	q.length = 0
}
