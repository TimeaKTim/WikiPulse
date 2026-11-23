// Package event reads the few fields of a recentchange event that the
// collector and replay need. The raw JSON is always kept and passed on unchanged.
package event

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"
)

// SourceHeader is the Kafka header that says where a message came from: "live" or "replay".
const SourceHeader = "wp-source"

type Event struct {
	Meta struct {
		ID    string `json:"id"`
		Dt    string `json:"dt"`
		Topic string `json:"topic"` // eqiad.* or codfw.*: one per Wikimedia datacenter
	} `json:"meta"`
	Wiki  string `json:"wiki"`
	Title string `json:"title"`

	Time time.Time `json:"-"` // meta.dt, parsed, in UTC
}

func Parse(raw []byte) (Event, error) {
	var e Event
	if err := json.Unmarshal(raw, &e); err != nil {
		return e, err
	}
	t, err := time.Parse(time.RFC3339Nano, e.Meta.Dt)
	if err != nil {
		return e, fmt.Errorf("meta.dt: %w", err)
	}
	e.Time = t.UTC()
	return e, nil
}

// Key is the Kafka key wiki|title, so all edits of a page go to the same partition.
func (e Event) Key() []byte {
	return []byte(e.Wiki + "|" + e.Title)
}

// Filter keeps the events of the listed wikis. A nil Filter keeps everything.
type Filter map[string]bool

// ParseFilter reads a comma-separated list like "rowiki,enwiki". Empty or "*" means all wikis.
func ParseFilter(s string) Filter {
	s = strings.TrimSpace(s)
	if s == "" || s == "*" {
		return nil
	}
	f := Filter{}
	for _, w := range strings.Split(s, ",") {
		if w = strings.TrimSpace(w); w != "" {
			f[w] = true
		}
	}
	return f
}

func (f Filter) Match(wiki string) bool {
	return f == nil || f[wiki]
}

func (f Filter) String() string {
	if f == nil {
		return "*"
	}
	var ws []string
	for w := range f {
		ws = append(ws, w)
	}
	sort.Strings(ws)
	return strings.Join(ws, ",")
}
