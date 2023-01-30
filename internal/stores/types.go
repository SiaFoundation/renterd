package stores

import (
	"database/sql/driver"
	"errors"
	"fmt"

	"go.sia.tech/core/types"
)

type (
	fileContractID types.FileContractID
)

// GormDataType implements gorm.GormDataTypeInterface.
func (fileContractID) GormDataType() string {
	return "bytes"
}

// Scan scan value into fileContractID, implements sql.Scanner interface.
func (fcid *fileContractID) Scan(value interface{}) error {
	bytes, ok := value.([]byte)
	if !ok {
		return errors.New(fmt.Sprint("failed to unmarshal fcid value:", value))
	}
	if len(bytes) < len(fileContractID{}) {
		return errors.New(fmt.Sprint("failed to unmarshal fcid value due to insufficient bytes", value))
	}
	*fcid = *(*fileContractID)(bytes)
	return nil
}

// Value returns a fileContractID value, implements driver.Valuer interface.
func (fcid fileContractID) Value() (driver.Value, error) {
	return fcid[:], nil
}
