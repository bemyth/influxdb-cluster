package query

import (
	"container/heap"
	"github.com/gogo/protobuf/proto"
	"github.com/influxdata/influxql"
	"io"
)

func (itr *bufUnsignedIterator) NextBatch() ([]*UnsignedPoint, error) {
	results := make([]*UnsignedPoint, 0)
	buf := itr.buf
	if buf != nil {
		itr.buf = nil
		results = append(results, buf)
		return results, nil
	}
	p, err := itr.itr.Next()
	results = append(results, p)
	return results, err
}

func (itr *unsignedMergeIterator) NextBatch() ([]*UnsignedPoint, error) {
	results := make([]*UnsignedPoint, 0)
	itr.mu.RLock()
	defer itr.mu.RUnlock()
	if itr.closed {
		return results, nil
	}

	// Initialize the heap. This needs to be done lazily on the first call to this iterator
	// so that iterator initialization done through the Select() call returns quickly.
	// Queries can only be interrupted after the Select() call completes so any operations
	// done during iterator creation cannot be interrupted, which is why we do it here
	// instead so an interrupt can happen while initializing the heap.
	if !itr.init {
		items := itr.heap.items
		itr.heap.items = make([]*unsignedMergeHeapItem, 0, len(items))
		for _, item := range items {
			if p, err := item.itr.peek(); err != nil {
				return results, err
			} else if p == nil {
				continue
			}
			itr.heap.items = append(itr.heap.items, item)
		}
		heap.Init(itr.heap)
		itr.init = true
	}

	for {
		// Retrieve the next iterator if we don't have one.
		if itr.curr == nil {
			if len(itr.heap.items) == 0 {
				return results, nil
			}
			itr.curr = heap.Pop(itr.heap).(*unsignedMergeHeapItem)

			// Read point and set current window.
			p, err := itr.curr.itr.Next()
			if err != nil {
				return results, err
			}
			tags := p.Tags.Subset(itr.heap.opt.Dimensions)
			itr.window.name, itr.window.tags = p.Name, tags.ID()
			itr.window.startTime, itr.window.endTime = itr.heap.opt.Window(p.Time)
			results = append(results, p)
			return results, nil
		}

		// Read the next point from the current iterator.
		p, err := itr.curr.itr.Next()
		if err != nil {
			return results, err
		}

		// If there are no more points then remove iterator from heap and find next.
		if p == nil {
			itr.curr = nil
			continue
		}

		// Check if the point is inside of our current window.
		inWindow := true
		if window := itr.window; window.name != p.Name {
			inWindow = false
		} else if tags := p.Tags.Subset(itr.heap.opt.Dimensions); window.tags != tags.ID() {
			inWindow = false
		} else if opt := itr.heap.opt; opt.Ascending && p.Time >= window.endTime {
			inWindow = false
		} else if !opt.Ascending && p.Time < window.startTime {
			inWindow = false
		}

		// If it's outside our window then push iterator back on the heap and find new iterator.
		if !inWindow {
			itr.curr.itr.unread(p)
			heap.Push(itr.heap, itr.curr)
			itr.curr = nil
			continue
		}

		results = append(results, p)
		return results, nil
	}
}

func (itr *unsignedSortedMergeIterator) NextBatch() ([]*UnsignedPoint, error) {
	results := make([]*UnsignedPoint, 0)
	p, err := itr.pop()
	results = append(results, p)
	return results, err
}

func (itr *unsignedParallelIterator) NextBatch() ([]*UnsignedPoint, error) {
	results := make([]*UnsignedPoint, 0)
	v, ok := <-itr.ch
	if !ok {
		return results, io.EOF
	}
	results = append(results, v.point)
	return results, v.err
}

func (itr *unsignedLimitIterator) NextBatch() ([]*UnsignedPoint, error) {
	results := make([]*UnsignedPoint, 0)
	for {
		p, err := itr.input.Next()
		if p == nil || err != nil {
			return results, err
		}

		// Reset window and counter if a new window is encountered.
		if p.Name != itr.prev.name || !p.Tags.Equals(&itr.prev.tags) {
			itr.prev.name = p.Name
			itr.prev.tags = p.Tags
			itr.n = 0
		}

		// Increment counter.
		itr.n++

		// Read next point if not beyond the offset.
		if itr.n <= itr.opt.Offset {
			continue
		}

		// Read next point if we're beyond the limit.
		if itr.opt.Limit > 0 && (itr.n-itr.opt.Offset) > itr.opt.Limit {
			continue
		}
		results = append(results, p)
		return results, nil
	}
}

func (itr *unsignedFillIterator) NextBatch() ([]*UnsignedPoint, error) {
	results := make([]*UnsignedPoint, 0)
	if !itr.init {
		p, err := itr.input.peek()
		if p == nil || err != nil {
			return results, err
		}
		itr.window.name, itr.window.tags = p.Name, p.Tags
		itr.window.time = itr.startTime
		if itr.startTime == influxql.MinTime {
			itr.window.time, _ = itr.opt.Window(p.Time)
		}
		if itr.opt.Location != nil {
			_, itr.window.offset = itr.opt.Zone(itr.window.time)
		}
		itr.init = true
	}

	p, err := itr.input.Next()
	if err != nil {
		return results, err
	}

	// Check if the next point is outside of our window or is nil.
	if p == nil || p.Name != itr.window.name || p.Tags.ID() != itr.window.tags.ID() {
		// If we are inside of an interval, unread the point and continue below to
		// constructing a new point.
		if itr.opt.Ascending && itr.window.time <= itr.endTime {
			itr.input.unread(p)
			p = nil
			goto CONSTRUCT
		} else if !itr.opt.Ascending && itr.window.time >= itr.endTime && itr.endTime != influxql.MinTime {
			itr.input.unread(p)
			p = nil
			goto CONSTRUCT
		}

		// We are *not* in a current interval. If there is no next point,
		// we are at the end of all intervals.
		if p == nil {
			return results, nil
		}

		// Set the new interval.
		itr.window.name, itr.window.tags = p.Name, p.Tags
		itr.window.time = itr.startTime
		if itr.window.time == influxql.MinTime {
			itr.window.time, _ = itr.opt.Window(p.Time)
		}
		if itr.opt.Location != nil {
			_, itr.window.offset = itr.opt.Zone(itr.window.time)
		}
		itr.prev = UnsignedPoint{Nil: true}
	}

	// Check if the point is our next expected point.
CONSTRUCT:
	if p == nil || (itr.opt.Ascending && p.Time > itr.window.time) || (!itr.opt.Ascending && p.Time < itr.window.time) {
		if p != nil {
			itr.input.unread(p)
		}

		p = &UnsignedPoint{
			Name: itr.window.name,
			Tags: itr.window.tags,
			Time: itr.window.time,
			Aux:  itr.auxFields,
		}

		switch itr.opt.Fill {
		case influxql.LinearFill:
			if !itr.prev.Nil {
				next, err := itr.input.peek()
				if err != nil {
					return results, err
				} else if next != nil && next.Name == itr.window.name && next.Tags.ID() == itr.window.tags.ID() {
					interval := int64(itr.opt.Interval.Duration)
					start := itr.window.time / interval
					p.Value = linearUnsigned(start, itr.prev.Time/interval, next.Time/interval, itr.prev.Value, next.Value)
				} else {
					p.Nil = true
				}
			} else {
				p.Nil = true
			}

		case influxql.NullFill:
			p.Nil = true
		case influxql.NumberFill:
			p.Value, _ = castToUnsigned(itr.opt.FillValue)
		case influxql.PreviousFill:
			if !itr.prev.Nil {
				p.Value = itr.prev.Value
				p.Nil = itr.prev.Nil
			} else {
				p.Nil = true
			}
		}
	} else {
		itr.prev = *p
	}

	// Advance the expected time. Do not advance to a new window here
	// as there may be lingering points with the same timestamp in the previous
	// window.
	if itr.opt.Ascending {
		itr.window.time += int64(itr.opt.Interval.Duration)
	} else {
		itr.window.time -= int64(itr.opt.Interval.Duration)
	}

	// Check to see if we have passed over an offset change and adjust the time
	// to account for this new offset.
	if itr.opt.Location != nil {
		if _, offset := itr.opt.Zone(itr.window.time - 1); offset != itr.window.offset {
			diff := itr.window.offset - offset
			if abs(diff) < int64(itr.opt.Interval.Duration) {
				itr.window.time += diff
			}
			itr.window.offset = offset
		}
	}
	results = append(results, p)
	return results, nil
}

func (itr *unsignedIntervalIterator) NextBatch() ([]*UnsignedPoint, error) {
	results := make([]*UnsignedPoint, 0)
	p, err := itr.input.Next()
	if p == nil || err != nil {
		return results, err
	}
	p.Time, _ = itr.opt.Window(p.Time)
	// If we see the minimum allowable time, set the time to zero so we don't
	// break the default returned time for aggregate queries without times.
	if p.Time == influxql.MinTime {
		p.Time = 0
	}
	results = append(results, p)
	return results, nil
}

func (itr *unsignedInterruptIterator) NextBatch() ([]*UnsignedPoint, error) {
	// Only check if the channel is closed every N points. This
	// intentionally checks on both 0 and N so that if the iterator
	// has been interrupted before the first point is emitted it will
	// not emit any points.
	results := make([]*UnsignedPoint, 0)
	if itr.count&0xFF == 0xFF {
		select {
		case <-itr.closing:
			return results, itr.Close()
		default:
			// Reset iterator count to zero and fall through to emit the next point.
			itr.count = 0
		}
	}

	// Increment the counter for every point read.
	itr.count++
	p, err := itr.input.Next()
	results = append(results, p)
	return results, err
}

func (itr *unsignedCloseInterruptIterator) NextBatch() ([]*UnsignedPoint, error) {
	results := make([]*UnsignedPoint, 0)
	p, err := itr.input.Next()
	if err != nil {
		// Check if the iterator was closed.
		select {
		case <-itr.done:
			return results, nil
		default:
			return results, err
		}
	}
	results = append(results, p)
	return results, nil
}

func (itr *unsignedReduceFloatIterator) NextBatch() ([]*FloatPoint, error) {
	results := make([]*FloatPoint, 0)
	// Calculate next window if we have no more points.
	if len(itr.points) == 0 {
		var err error
		itr.points, err = itr.reduce()
		if len(itr.points) == 0 {
			return results, err
		}
	}

	// Pop next point off the stack.
	p := &itr.points[len(itr.points)-1]
	itr.points = itr.points[:len(itr.points)-1]
	results = append(results, p)
	return results, nil
}

func (itr *unsignedStreamFloatIterator) NextBatch() ([]*FloatPoint, error) {
	results := make([]*FloatPoint, 0)
	// Calculate next window if we have no more points.
	if len(itr.points) == 0 {
		var err error
		itr.points, err = itr.reduce()
		if len(itr.points) == 0 {
			return results, err
		}
	}

	// Pop next point off the stack.
	p := &itr.points[len(itr.points)-1]
	itr.points = itr.points[:len(itr.points)-1]
	results = append(results, p)
	return results, nil
}

func (itr *unsignedReduceIntegerIterator) NextBatch() ([]*IntegerPoint, error) {
	results := make([]*IntegerPoint, 0)
	// Calculate next window if we have no more points.
	if len(itr.points) == 0 {
		var err error
		itr.points, err = itr.reduce()
		if len(itr.points) == 0 {
			return results, err
		}
	}

	// Pop next point off the stack.
	p := &itr.points[len(itr.points)-1]
	itr.points = itr.points[:len(itr.points)-1]
	results = append(results, p)
	return results, nil
}

func (itr *unsignedStreamIntegerIterator) NextBatch() ([]*IntegerPoint, error) {
	results := make([]*IntegerPoint, 0)
	// Calculate next window if we have no more points.
	if len(itr.points) == 0 {
		var err error
		itr.points, err = itr.reduce()
		if len(itr.points) == 0 {
			return results, err
		}
	}

	// Pop next point off the stack.
	p := &itr.points[len(itr.points)-1]
	itr.points = itr.points[:len(itr.points)-1]
	results = append(results, p)
	return results, nil
}

func (itr *unsignedReduceUnsignedIterator) NextBatch() ([]*UnsignedPoint, error) {
	results := make([]*UnsignedPoint, 0)
	// Calculate next window if we have no more points.
	if len(itr.points) == 0 {
		var err error
		itr.points, err = itr.reduce()
		if len(itr.points) == 0 {
			return results, err
		}
	}

	// Pop next point off the stack.
	p := &itr.points[len(itr.points)-1]
	itr.points = itr.points[:len(itr.points)-1]
	results = append(results, p)
	return results, nil
}

func (itr *unsignedStreamUnsignedIterator) NextBatch() ([]*UnsignedPoint, error) {
	results := make([]*UnsignedPoint, 0)
	// Calculate next window if we have no more points.
	if len(itr.points) == 0 {
		var err error
		itr.points, err = itr.reduce()
		if len(itr.points) == 0 {
			return results, err
		}
	}

	// Pop next point off the stack.
	p := &itr.points[len(itr.points)-1]
	itr.points = itr.points[:len(itr.points)-1]
	results = append(results, p)
	return results, nil
}

func (itr *unsignedReduceStringIterator) NextBatch() ([]*StringPoint, error) {
	results := make([]*StringPoint, 0)
	// Calculate next window if we have no more points.
	if len(itr.points) == 0 {
		var err error
		itr.points, err = itr.reduce()
		if len(itr.points) == 0 {
			return results, err
		}
	}

	// Pop next point off the stack.
	p := &itr.points[len(itr.points)-1]
	itr.points = itr.points[:len(itr.points)-1]
	results = append(results, p)
	return results, nil
}

func (itr *unsignedStreamStringIterator) NextBatch() ([]*StringPoint, error) {
	results := make([]*StringPoint, 0)
	// Calculate next window if we have no more points.
	if len(itr.points) == 0 {
		var err error
		itr.points, err = itr.reduce()
		if len(itr.points) == 0 {
			return results, err
		}
	}

	// Pop next point off the stack.
	p := &itr.points[len(itr.points)-1]
	itr.points = itr.points[:len(itr.points)-1]
	results = append(results, p)
	return results, nil
}

func (itr *unsignedReduceBooleanIterator) NextBatch() ([]*BooleanPoint, error) {
	results := make([]*BooleanPoint, 0)
	// Calculate next window if we have no more points.
	if len(itr.points) == 0 {
		var err error
		itr.points, err = itr.reduce()
		if len(itr.points) == 0 {
			return results, err
		}
	}

	// Pop next point off the stack.
	p := &itr.points[len(itr.points)-1]
	itr.points = itr.points[:len(itr.points)-1]
	results = append(results, p)
	return results, nil
}

func (itr *unsignedStreamBooleanIterator) NextBatch() ([]*BooleanPoint, error) {
	results := make([]*BooleanPoint, 0)
	// Calculate next window if we have no more points.
	if len(itr.points) == 0 {
		var err error
		itr.points, err = itr.reduce()
		if len(itr.points) == 0 {
			return results, err
		}
	}

	// Pop next point off the stack.
	p := &itr.points[len(itr.points)-1]
	itr.points = itr.points[:len(itr.points)-1]
	results = append(results, p)
	return results, nil
}

func (itr *unsignedIteratorMapper) NextBatch() ([]*UnsignedPoint, error) {
	results := make([]*UnsignedPoint, 0)
	if !itr.cur.Scan(&itr.row) {
		if err := itr.cur.Err(); err != nil {
			return results, err
		}
		return results, nil
	}

	itr.point.Time = itr.row.Time
	itr.point.Name = itr.row.Series.Name
	itr.point.Tags = itr.row.Series.Tags

	if itr.driver != nil {
		if v := itr.driver.Value(&itr.row); v != nil {
			if v, ok := castToUnsigned(v); ok {
				itr.point.Value = v
				itr.point.Nil = false
			} else {
				itr.point.Value = 0
				itr.point.Nil = true
			}
		} else {
			itr.point.Value = 0
			itr.point.Nil = true
		}
	}
	for i, f := range itr.fields {
		itr.point.Aux[i] = f.Value(&itr.row)
	}
	results = append(results, &itr.point)
	return results, nil
}

func (itr *unsignedFilterIterator) NextBatch() ([]*UnsignedPoint, error) {
	results := make([]*UnsignedPoint, 0)
	for {
		p, err := itr.input.Next()
		if err != nil || p == nil {
			return results, err
		}

		for i, ref := range itr.opt.Aux {
			itr.m[ref.Val] = p.Aux[i]
		}
		for k, v := range p.Tags.KeyValues() {
			itr.m[k] = v
		}

		if !influxql.EvalBool(itr.cond, itr.m) {
			continue
		}
		results = append(results, p)
		return results, nil
	}
}

func (itr *unsignedDedupeIterator) NextBatch() ([]*UnsignedPoint, error) {
	results := make([]*UnsignedPoint, 0)
	for {
		// Read next point.
		p, err := itr.input.Next()
		if p == nil || err != nil {
			return results, err
		}

		// Serialize to bytes to store in lookup.
		buf, err := proto.Marshal(encodeUnsignedPoint(p))
		if err != nil {
			return results, err
		}

		// If the point has already been output then move to the next point.
		if _, ok := itr.m[string(buf)]; ok {
			continue
		}

		// Otherwise mark it as emitted and return point.
		itr.m[string(buf)] = struct{}{}
		results = append(results, p)
		return results, nil
	}
}

func (itr *unsignedReaderIterator) NextBatch() ([]*UnsignedPoint, error) {
	// OPTIMIZE(benbjohnson): Reuse point on iterator.
	results := make([]*UnsignedPoint, 0)
	// Unmarshal next point.
	p := &UnsignedPoint{}
	if err := itr.dec.DecodeUnsignedPoint(p); err == io.EOF {
		return results, nil
	} else if err != nil {
		return results, err
	}
	results = append(results, p)
	return results, nil
}
