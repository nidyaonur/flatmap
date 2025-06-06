// Code generated by the FlatBuffers compiler. DO NOT EDIT.

package books

import "strconv"

type Status byte

const (
	StatusUNKNOWN  Status = 0
	StatusACTIVE   Status = 1
	StatusPAUSED   Status = 2
	StatusDELETED  Status = 3
	StatusARCHIVED Status = 4
)

var EnumNamesStatus = map[Status]string{
	StatusUNKNOWN:  "UNKNOWN",
	StatusACTIVE:   "ACTIVE",
	StatusPAUSED:   "PAUSED",
	StatusDELETED:  "DELETED",
	StatusARCHIVED: "ARCHIVED",
}

var EnumValuesStatus = map[string]Status{
	"UNKNOWN":  StatusUNKNOWN,
	"ACTIVE":   StatusACTIVE,
	"PAUSED":   StatusPAUSED,
	"DELETED":  StatusDELETED,
	"ARCHIVED": StatusARCHIVED,
}

func (v Status) String() string {
	if s, ok := EnumNamesStatus[v]; ok {
		return s
	}
	return "Status(" + strconv.FormatInt(int64(v), 10) + ")"
}
