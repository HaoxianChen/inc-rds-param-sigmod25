package common

import (
	"fmt"
)

type ServiceError struct {
	Code uint32
	Msg  string
}

func (e *ServiceError) Error() string {
	return fmt.Sprintf("ServiceError: code=%d, msg=%s", e.Code, e.Msg)
}

var (
	// business error code: [500000, 600000)
	ErrGetColumns     = &ServiceError{500000, "get columns error"}
	ErrOpenCsv        = &ServiceError{500001, "open csv error"}
	ErrReadCsv        = &ServiceError{500002, "read csv error"}
	ErrWrongDataType  = &ServiceError{500003, "wrong data type"}
	ErrEmptyPointer   = &ServiceError{500004, "pointer is nil"}
	ErrParameter      = &ServiceError{500005, "invalid parameter"}
	ErrColumnNotExist = &ServiceError{500006, "column not exist"}
)
