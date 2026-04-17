package model

import "time"

// RawEvent is a generic envelope for raw payloads emitted by source
// adapters before they are landed in BigQuery. Payload is the untouched
// JSON (or CSV row) returned by the upstream — storing it verbatim keeps
// the bronze layer auditable.
type RawEvent struct {
	Source      string
	Kind        string
	Payload     []byte
	IngestedAt  time.Time
}
