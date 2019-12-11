package query

import (
	"container/heap"
	"github.com/gogo/protobuf/proto"
	"github.com/influxdata/influxql"
	"io"
)

func (*nilFloatIterator) NextBatch() ([]*FloatPoint, error) {
	results := make([]*FloatPoint, 0)
	return results, nil
}

func (*nilFloatReaderIterator) NextBatch() ([]*FloatPoint, error) {
	results := make([]*FloatPoint, 0)
	return results, nil
}

func (itr *floatFastDedupeIterator) NextBatch() ([]*FloatPoint, error) {
	results := make([]*FloatPoint, 0)
	for {
		// Read next point.
		// Skip if there are not any aux fields.
		p, err := itr.input.Next()
		if p == nil || err != nil {
			return results, err
		} else if len(p.Aux) == 0 {
			continue
		}

		// If the point has already been output then move to the next point.
		key := fastDedupeKey{name: p.Name}
		key.values[0] = p.Aux[0]
		if len(p.Aux) > 1 {
			key.values[1] = p.Aux[1]
		}
		if _, ok := itr.m[key]; ok {
			continue
		}

		// Otherwise mark it as emitted and return point.
		itr.m[key] = struct{}{}
		results = append(results, p)
		return results, nil
	}
}

func (itr *bufFloatIterator) NextBatch() ([]*FloatPoint, error) {
	buf := itr.buf
	if buf != nil {
		itr.buf = nil
		results := make([]*FloatPoint, 0)
		results = append(results, buf)
		return results, nil
	}
	return itr.itr.NextBatch()
}

func (itr *floatMergeIterator) NextBatch() ([]*FloatPoint, error) {
	results := make([]*FloatPoint, 0)

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
		itr.heap.items = make([]*floatMergeHeapItem, 0, len(items))
		for _, item := range items {
			if p, err := item.itr.peek(); err != nil {
				return nil, err
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
				return nil, nil
			}
			itr.curr = heap.Pop(itr.heap).(*floatMergeHeapItem)

			// Read point and set current window.
			p, err := itr.curr.itr.Next()
			if err != nil {
				return nil, err
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
			return nil, err
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

func (itr *floatSortedMergeIterator) NextBatch() ([]*FloatPoint, error) {
	results := make([]*FloatPoint, 0)
	p, err := itr.pop()
	results = append(results, p)
	return results, err
}

func (itr *floatParallelIterator) NextBatch() ([]*FloatPoint, error) {
	v, ok := <-itr.ch
	results := make([]*FloatPoint, 0)
	if !ok {
		return results, io.EOF
	}
	results = append(results, v.point)
	return results, v.err
}

func (itr *floatLimitIterator) NextBatch() ([]*FloatPoint, error) {
	results := make([]*FloatPoint, 0)
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

func (itr *floatFillIterator) NextBatch() ([]*FloatPoint, error) {
	results := make([]*FloatPoint, 0)
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
		itr.prev = FloatPoint{Nil: true}
	}

	// Check if the point is our next expected point.
CONSTRUCT:
	if p == nil || (itr.opt.Ascending && p.Time > itr.window.time) || (!itr.opt.Ascending && p.Time < itr.window.time) {
		if p != nil {
			itr.input.unread(p)
		}

		p = &FloatPoint{
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
					return nil, err
				} else if next != nil && next.Name == itr.window.name && next.Tags.ID() == itr.window.tags.ID() {
					interval := int64(itr.opt.Interval.Duration)
					start := itr.window.time / interval
					p.Value = linearFloat(start, itr.prev.Time/interval, next.Time/interval, itr.prev.Value, next.Value)
				} else {
					p.Nil = true
				}
			} else {
				p.Nil = true
			}

		case influxql.NullFill:
			p.Nil = true
		case influxql.NumberFill:
			p.Value, _ = castToFloat(itr.opt.FillValue)
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

func (itr *floatIntervalIterator) NextBatch() ([]*FloatPoint, error) {
	results := make([]*FloatPoint, 0)
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

func (itr *floatInterruptIterator) NextBatch() ([]*FloatPoint, error) {
	// Only check if the channel is closed every N points. This
	// intentionally checks on both 0 and N so that if the iterator
	// has been interrupted before the first point is emitted it will
	// not emit any points.
	results := make([]*FloatPoint, 0)
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

func (itr *floatCloseInterruptIterator) NextBatch() ([]*FloatPoint, error) {
	results := make([]*FloatPoint, 0)
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

func (itr *floatReduceFloatIterator) NextBatch() ([]*FloatPoint, error) {
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

func (itr *floatStreamFloatIterator) NextBatch() ([]*FloatPoint, error) {
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

func (itr *floatReduceIntegerIterator) NextBatch() ([]*IntegerPoint, error) {
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

func (itr *floatStreamIntegerIterator) NextBatch() ([]*IntegerPoint, error) {
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

func (itr *floatReduceUnsignedIterator) NextBatch() ([]*UnsignedPoint, error) {
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

func (itr *floatStreamUnsignedIterator) NextBatch() ([]*UnsignedPoint, error) {
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

func (itr *floatReduceStringIterator) NextBatch() ([]*StringPoint, error) {
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

func (itr *floatStreamStringIterator) NextBatch() ([]*StringPoint, error) {
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

func (itr *floatReduceBooleanIterator) NextBatch() ([]*BooleanPoint, error) {
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

// Next returns the next value for the stream iterator.
func (itr *floatStreamBooleanIterator) NextBatch() ([]*BooleanPoint, error) {
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

func (itr *floatIteratorMapper) NextBatch() ([]*FloatPoint, error) {
	results := make([]*FloatPoint, 0)
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
			if v, ok := castToFloat(v); ok {
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

func (itr *floatFilterIterator) NextBatch() ([]*FloatPoint, error) {
	results := make([]*FloatPoint, 0)
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

func (itr *floatDedupeIterator) NextBatch() ([]*FloatPoint, error) {
	results := make([]*FloatPoint, 0)
	for {
		// Read next point.
		p, err := itr.input.Next()
		if p == nil || err != nil {
			return results, err
		}

		// Serialize to bytes to store in lookup.
		buf, err := proto.Marshal(encodeFloatPoint(p))
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

func (itr *floatReaderIterator) NextBatch() ([]*FloatPoint, error) {
	// OPTIMIZE(benbjohnson): Reuse point on iterator.
	results := make([]*FloatPoint, 0)
	// Unmarshal next point.
	p := &FloatPoint{}
	if err := itr.dec.DecodeFloatPoint(p); err == io.EOF {
		return results, nil
	} else if err != nil {
		return results, err
	}
	results = append(results, p)
	return results, nil
}
