package tsdb

import (
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/query"
)

func (itr *seriesPointIterator) NextBatch() ([]*query.FloatPoint, error) {
	results := make([]*query.FloatPoint, 0)
	for {
		// Read series keys for next measurement if no more keys remaining.
		// Exit if there are no measurements remaining.
		if len(itr.keys) == 0 {
			m, err := itr.mitr.Next()
			if err != nil {
				return results, err
			} else if m == nil {
				return results, nil
			}

			if err := itr.readSeriesKeys(m); err != nil {
				return results, err
			}
			continue
		}

		name, tags := ParseSeriesKey(itr.keys[0])
		itr.keys = itr.keys[1:]

		// TODO(edd): It seems to me like this authorisation check should be
		// further down in the index. At this point we're going to be filtering
		// series that have already been materialised in the LogFiles and
		// IndexFiles.
		if itr.opt.Authorizer != nil && !itr.opt.Authorizer.AuthorizeSeriesRead(itr.indexSet.Database(), name, tags) {
			continue
		}

		// Convert to a key.
		key := string(models.MakeKey(name, tags))

		// Write auxiliary fields.
		for i, f := range itr.opt.Aux {
			switch f.Val {
			case "key":
				itr.point.Aux[i] = key
			}
		}
		results = append(results, &itr.point)
		return results, nil
	}
}
