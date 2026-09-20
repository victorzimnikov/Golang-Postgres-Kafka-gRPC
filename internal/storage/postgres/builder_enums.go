package postgres

import "fmt"

type FieldType uint8

const (
	TypeText FieldType = iota + 1
	TypeUUID
	TypeJSONB
	TypeInteger
)

func (t FieldType) sql() (string, error) {
	switch t {
	case TypeText:
		return "text", nil
	case TypeUUID:
		return "uuid", nil
	case TypeJSONB:
		return "jsonb", nil
	case TypeInteger:
		return "integer", nil
	default:
		return "", fmt.Errorf("invalid field type: %d", t)
	}
}

type LockMode uint8

const (
	LockModeUpdate LockMode = iota + 1
	LockModeNoKeyUpdate
	LockModeShare
	LockModeKeyShare
)

func (t LockMode) sql() (string, error) {
	switch t {
	case LockModeUpdate:
		return "UPDATE", nil
	case LockModeNoKeyUpdate:
		return "NO KEY UPDATE", nil
	case LockModeShare:
		return "SHARE", nil
	case LockModeKeyShare:
		return "KEY SHARE", nil
	default:
		return "", fmt.Errorf("invalid blocking mode: %d", t)
	}
}

type OrderDirection uint8

const (
	OrderDirectionUnspecified OrderDirection = iota
	OrderAsc
	OrderDesc
)

func (d OrderDirection) sql() (string, error) {
	switch d {
	case OrderAsc:
		return "ASC", nil
	case OrderDesc:
		return "DESC", nil
	default:
		return "", fmt.Errorf("invalid order direction: %d", d)
	}
}
