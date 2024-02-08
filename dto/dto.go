package dto

import "time"

type Entity struct {
	EntID       string     `db:"ent_id"`
	Data        float64    `db:"data"`
	Value       float64    `db:"value"`
	DateAdded   *time.Time `db:"date_added"`
	DateUpdated *time.Time `db:"date_updated"`
}

func (e *Entity) ColumnsNum() int {
	return 3
}

func (e *Entity) Columns() []interface{} {
	return []interface{}{
		e.EntID,
		e.Data,
		e.Value,
	}
}
