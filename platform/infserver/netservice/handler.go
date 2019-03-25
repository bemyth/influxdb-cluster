package netservice

import (
	"encoding/json"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/query"
	"net/http"
	"strings"
)

type Handler struct {
	InfService interface {
		Write(database,retentionPolicy string,pts models.Points) error
		Query(qa string) (query.Result, error)
	}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.Header.Get("method") {
	case "write":
		h.serveWrite(w, r)
	case "query":
		h.serveQuery(w, r)
	case "test":

	}
}

func (h *Handler) serveTest(w http.ResponseWriter, r *http.Request) {
	if strings.HasPrefix(r.URL.String(), "/raft") {
		if r.Method == "POST" {
			r.Header.Get("dataID")
		}
		if r.Method == "GET" {

		}
	}
}

func (h *Handler) serveWrite(w http.ResponseWriter, r *http.Request) {
	//err := h.InfService.Write("")

	return
}
func (h *Handler) serveQuery(w http.ResponseWriter, r *http.Request) {
	//res, err := h.InfService.Query()
	w.WriteHeader(http.StatusOK)
	return
}

func resultError(w http.ResponseWriter, result query.Result, code int) {
	w.Header().Add("content-type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(&result)
}
