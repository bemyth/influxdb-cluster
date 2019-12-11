package tsm1

import (
	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxql"
)

func (itr *floatIterator) NextBatch() ([]*query.FloatPoint, error) {
	results := make([]*query.FloatPoint, 0)
	for {
		seek := tsdb.EOF

		if itr.cur != nil {
			// Read from the main cursor if we have one.
			itr.point.Time, itr.point.Value = itr.cur.nextFloat()
			seek = itr.point.Time
		} else {
			// Otherwise find lowest aux timestamp.
			for i := range itr.aux {
				if k, _ := itr.aux[i].peek(); k != tsdb.EOF {
					if seek == tsdb.EOF || (itr.opt.Ascending && k < seek) || (!itr.opt.Ascending && k > seek) {
						seek = k
					}
				}
			}
			itr.point.Time = seek
		}

		// Exit if we have no more points or we are outside our time range.
		if itr.point.Time == tsdb.EOF {
			itr.copyStats()
			return results, nil
		} else if itr.opt.Ascending && itr.point.Time > itr.opt.EndTime {
			itr.copyStats()
			return results, nil
		} else if !itr.opt.Ascending && itr.point.Time < itr.opt.StartTime {
			itr.copyStats()
			return results, nil
		}

		// Read from each auxiliary cursor.
		for i := range itr.opt.Aux {
			itr.point.Aux[i] = itr.aux[i].nextAt(seek)
		}

		// Read from condition field cursors.
		for i := range itr.conds.curs {
			itr.m[itr.conds.names[i]] = itr.conds.curs[i].nextAt(seek)
		}

		// Evaluate condition, if one exists. Retry if it fails.
		valuer := influxql.ValuerEval{
			Valuer: influxql.MultiValuer(
				query.MathValuer{},
				influxql.MapValuer(itr.m),
			),
		}
		if itr.opt.Condition != nil && !valuer.EvalBool(itr.opt.Condition) {
			continue
		}

		// Track points returned.
		itr.statsBuf.PointN++

		// Copy buffer to stats periodically.
		if itr.statsBuf.PointN%statsBufferCopyIntervalN == 0 {
			itr.copyStats()
		}
		results = append(results, &itr.point)
		return results, nil
	}
}

func (itr *floatLimitIterator) NextBatch() ([]*query.FloatPoint, error) {
	results := make([]*query.FloatPoint, 0)
	// Check if we are beyond the limit.
	if (itr.n - itr.opt.Offset) > itr.opt.Limit {
		return results, nil
	}

	// Read the next point.
	p, err := itr.input.Next()
	if p == nil || err != nil {
		return results, err
	}

	// Increment counter.
	itr.n++

	// Offsets are handled by a higher level iterator so return all points.
	results = append(results, p)
	return results, nil
}

func (itr *integerIterator) NextBatch() ([]*query.IntegerPoint, error) {
	results := make([]*query.IntegerPoint, 0)
	for {
		seek := tsdb.EOF

		if itr.cur != nil {
			// Read from the main cursor if we have one.
			itr.point.Time, itr.point.Value = itr.cur.nextInteger()
			seek = itr.point.Time
		} else {
			// Otherwise find lowest aux timestamp.
			for i := range itr.aux {
				if k, _ := itr.aux[i].peek(); k != tsdb.EOF {
					if seek == tsdb.EOF || (itr.opt.Ascending && k < seek) || (!itr.opt.Ascending && k > seek) {
						seek = k
					}
				}
			}
			itr.point.Time = seek
		}

		// Exit if we have no more points or we are outside our time range.
		if itr.point.Time == tsdb.EOF {
			itr.copyStats()
			return results, nil
		} else if itr.opt.Ascending && itr.point.Time > itr.opt.EndTime {
			itr.copyStats()
			return results, nil
		} else if !itr.opt.Ascending && itr.point.Time < itr.opt.StartTime {
			itr.copyStats()
			return results, nil
		}

		// Read from each auxiliary cursor.
		for i := range itr.opt.Aux {
			itr.point.Aux[i] = itr.aux[i].nextAt(seek)
		}

		// Read from condition field cursors.
		for i := range itr.conds.curs {
			itr.m[itr.conds.names[i]] = itr.conds.curs[i].nextAt(seek)
		}

		// Evaluate condition, if one exists. Retry if it fails.
		valuer := influxql.ValuerEval{
			Valuer: influxql.MultiValuer(
				query.MathValuer{},
				influxql.MapValuer(itr.m),
			),
		}
		if itr.opt.Condition != nil && !valuer.EvalBool(itr.opt.Condition) {
			continue
		}

		// Track points returned.
		itr.statsBuf.PointN++

		// Copy buffer to stats periodically.
		if itr.statsBuf.PointN%statsBufferCopyIntervalN == 0 {
			itr.copyStats()
		}
		results = append(results, &itr.point)
		return results, nil
	}
}

func (itr *integerLimitIterator) NextBatch() ([]*query.IntegerPoint, error) {
	results := make([]*query.IntegerPoint, 0)
	// Check if we are beyond the limit.
	if (itr.n - itr.opt.Offset) > itr.opt.Limit {
		return results, nil
	}

	// Read the next point.
	p, err := itr.input.Next()
	if p == nil || err != nil {
		return results, err
	}

	// Increment counter.
	itr.n++

	// Offsets are handled by a higher level iterator so return all points.
	results = append(results, p)
	return results, nil
}

func (itr *unsignedIterator) NextBatch() ([]*query.UnsignedPoint, error) {
	results := make([]*query.UnsignedPoint, 0)
	for {
		seek := tsdb.EOF

		if itr.cur != nil {
			// Read from the main cursor if we have one.
			itr.point.Time, itr.point.Value = itr.cur.nextUnsigned()
			seek = itr.point.Time
		} else {
			// Otherwise find lowest aux timestamp.
			for i := range itr.aux {
				if k, _ := itr.aux[i].peek(); k != tsdb.EOF {
					if seek == tsdb.EOF || (itr.opt.Ascending && k < seek) || (!itr.opt.Ascending && k > seek) {
						seek = k
					}
				}
			}
			itr.point.Time = seek
		}

		// Exit if we have no more points or we are outside our time range.
		if itr.point.Time == tsdb.EOF {
			itr.copyStats()
			return results, nil
		} else if itr.opt.Ascending && itr.point.Time > itr.opt.EndTime {
			itr.copyStats()
			return results, nil
		} else if !itr.opt.Ascending && itr.point.Time < itr.opt.StartTime {
			itr.copyStats()
			return results, nil
		}

		// Read from each auxiliary cursor.
		for i := range itr.opt.Aux {
			itr.point.Aux[i] = itr.aux[i].nextAt(seek)
		}

		// Read from condition field cursors.
		for i := range itr.conds.curs {
			itr.m[itr.conds.names[i]] = itr.conds.curs[i].nextAt(seek)
		}

		// Evaluate condition, if one exists. Retry if it fails.
		valuer := influxql.ValuerEval{
			Valuer: influxql.MultiValuer(
				query.MathValuer{},
				influxql.MapValuer(itr.m),
			),
		}
		if itr.opt.Condition != nil && !valuer.EvalBool(itr.opt.Condition) {
			continue
		}

		// Track points returned.
		itr.statsBuf.PointN++

		// Copy buffer to stats periodically.
		if itr.statsBuf.PointN%statsBufferCopyIntervalN == 0 {
			itr.copyStats()
		}
		results = append(results, &itr.point)
		return results, nil
	}
}

func (itr *unsignedLimitIterator) NextBatch() ([]*query.UnsignedPoint, error) {
	results := make([]*query.UnsignedPoint, 0)
	// Check if we are beyond the limit.
	if (itr.n - itr.opt.Offset) > itr.opt.Limit {
		return results, nil
	}

	// Read the next point.
	p, err := itr.input.Next()
	if p == nil || err != nil {
		return results, err
	}

	// Increment counter.
	itr.n++

	// Offsets are handled by a higher level iterator so return all points.
	results = append(results, p)
	return results, nil
}

func (itr *stringIterator) NextBatch() ([]*query.StringPoint, error) {
	results := make([]*query.StringPoint, 0)
	for {
		seek := tsdb.EOF

		if itr.cur != nil {
			// Read from the main cursor if we have one.
			itr.point.Time, itr.point.Value = itr.cur.nextString()
			seek = itr.point.Time
		} else {
			// Otherwise find lowest aux timestamp.
			for i := range itr.aux {
				if k, _ := itr.aux[i].peek(); k != tsdb.EOF {
					if seek == tsdb.EOF || (itr.opt.Ascending && k < seek) || (!itr.opt.Ascending && k > seek) {
						seek = k
					}
				}
			}
			itr.point.Time = seek
		}

		// Exit if we have no more points or we are outside our time range.
		if itr.point.Time == tsdb.EOF {
			itr.copyStats()
			return results, nil
		} else if itr.opt.Ascending && itr.point.Time > itr.opt.EndTime {
			itr.copyStats()
			return results, nil
		} else if !itr.opt.Ascending && itr.point.Time < itr.opt.StartTime {
			itr.copyStats()
			return results, nil
		}

		// Read from each auxiliary cursor.
		for i := range itr.opt.Aux {
			itr.point.Aux[i] = itr.aux[i].nextAt(seek)
		}

		// Read from condition field cursors.
		for i := range itr.conds.curs {
			itr.m[itr.conds.names[i]] = itr.conds.curs[i].nextAt(seek)
		}

		// Evaluate condition, if one exists. Retry if it fails.
		valuer := influxql.ValuerEval{
			Valuer: influxql.MultiValuer(
				query.MathValuer{},
				influxql.MapValuer(itr.m),
			),
		}
		if itr.opt.Condition != nil && !valuer.EvalBool(itr.opt.Condition) {
			continue
		}

		// Track points returned.
		itr.statsBuf.PointN++

		// Copy buffer to stats periodically.
		if itr.statsBuf.PointN%statsBufferCopyIntervalN == 0 {
			itr.copyStats()
		}
		results = append(results, &itr.point)
		return results, nil
	}
}

func (itr *stringLimitIterator) NextBatch() ([]*query.StringPoint, error) {
	results := make([]*query.StringPoint, 0)
	// Check if we are beyond the limit.
	if (itr.n - itr.opt.Offset) > itr.opt.Limit {
		return results, nil
	}

	// Read the next point.
	p, err := itr.input.Next()
	if p == nil || err != nil {
		return results, err
	}

	// Increment counter.
	itr.n++

	// Offsets are handled by a higher level iterator so return all points.
	results = append(results, p)
	return results, nil
}

func (itr *booleanIterator) NextBatch() ([]*query.BooleanPoint, error) {
	results := make([]*query.BooleanPoint, 0)
	for {
		seek := tsdb.EOF

		if itr.cur != nil {
			// Read from the main cursor if we have one.
			itr.point.Time, itr.point.Value = itr.cur.nextBoolean()
			seek = itr.point.Time
		} else {
			// Otherwise find lowest aux timestamp.
			for i := range itr.aux {
				if k, _ := itr.aux[i].peek(); k != tsdb.EOF {
					if seek == tsdb.EOF || (itr.opt.Ascending && k < seek) || (!itr.opt.Ascending && k > seek) {
						seek = k
					}
				}
			}
			itr.point.Time = seek
		}

		// Exit if we have no more points or we are outside our time range.
		if itr.point.Time == tsdb.EOF {
			itr.copyStats()
			return results, nil
		} else if itr.opt.Ascending && itr.point.Time > itr.opt.EndTime {
			itr.copyStats()
			return results, nil
		} else if !itr.opt.Ascending && itr.point.Time < itr.opt.StartTime {
			itr.copyStats()
			return results, nil
		}

		// Read from each auxiliary cursor.
		for i := range itr.opt.Aux {
			itr.point.Aux[i] = itr.aux[i].nextAt(seek)
		}

		// Read from condition field cursors.
		for i := range itr.conds.curs {
			itr.m[itr.conds.names[i]] = itr.conds.curs[i].nextAt(seek)
		}

		// Evaluate condition, if one exists. Retry if it fails.
		valuer := influxql.ValuerEval{
			Valuer: influxql.MultiValuer(
				query.MathValuer{},
				influxql.MapValuer(itr.m),
			),
		}
		if itr.opt.Condition != nil && !valuer.EvalBool(itr.opt.Condition) {
			continue
		}

		// Track points returned.
		itr.statsBuf.PointN++

		// Copy buffer to stats periodically.
		if itr.statsBuf.PointN%statsBufferCopyIntervalN == 0 {
			itr.copyStats()
		}
		results = append(results, &itr.point)
		return results, nil
	}
}

func (itr *booleanLimitIterator) NextBatch() ([]*query.BooleanPoint, error) {
	results := make([]*query.BooleanPoint, 0)
	// Check if we are beyond the limit.
	if (itr.n - itr.opt.Offset) > itr.opt.Limit {
		return results, nil
	}

	// Read the next point.
	p, err := itr.input.Next()
	if p == nil || err != nil {
		return results, err
	}

	// Increment counter.
	itr.n++

	// Offsets are handled by a higher level iterator so return all points.
	results = append(results, p)
	return results, nil
}
