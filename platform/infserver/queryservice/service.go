package queryservice

import "github.com/influxdata/influxdb/query"

type QueryService struct {
}

func New() *QueryService {
	return &QueryService{}
}
func (qs *QueryService) Open() error {
	return nil
}

func (qs *QueryService) Query(qa string) (rst query.Result, err error) {
	return query.Result{}, nil
}
