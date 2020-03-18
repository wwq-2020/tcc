package tcc

import (
	"database/sql"
	"fmt"
	"strings"
)

// EventStorage EventStorage
type EventStorage interface {
	Create(event *Event) error
	DeleteByBiz(biz string) error
	UpdateStatusByBiz(biz string, status Status) error
	FindEvents2Recovery(statuses []Status) ([]*Event, error)
	UpdateStatusByBizs(biz []string, status Status) error
}

type eventStorage struct {
	db *sql.DB
}

// NewDefaultEventStorage 初始化默认事件存储
func NewDefaultEventStorage(db *sql.DB) EventStorage {
	return &eventStorage{
		db: db,
	}
}

func (es *eventStorage) Create(event *Event) error {
	if _, err := es.db.Exec("insert into event(status, biz, biz_data) values(?,?,?)", event.Status, event.Biz, event.BizData); err != nil {
		return err
	}
	return nil
}

func (es *eventStorage) DeleteByBiz(biz string) error {
	if _, err := es.db.Exec("delete from event where biz = ?", biz); err != nil {
		return err
	}
	return nil
}

func (es *eventStorage) UpdateStatusByBiz(biz string, status Status) error {
	if _, err := es.db.Exec("update event set status = ? where biz = ?", biz); err != nil {
		return err
	}
	return nil
}

func (es *eventStorage) FindEvents2Recovery(statuses []Status) ([]*Event, error) {
	sqlPlaceHolder := make([]string, 0, len(statuses))
	sqlArgs := make([]interface{}, 0, len(statuses))
	sqlArgs = append(sqlArgs)
	for _, status := range statuses {
		sqlPlaceHolder = append(sqlPlaceHolder, "?")
		sqlArgs = append(sqlArgs, status)
	}
	sqlBaseStr := "select id, status, biz, biz_data from event where status in (%s)"
	sqlStr := fmt.Sprintf(sqlBaseStr, strings.Join(sqlPlaceHolder, ","))

	rows, err := es.db.Query(sqlStr, sqlArgs...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var results []*Event
	for rows.Next() {
		result := &Event{}
		if err := rows.Scan(&result.ID, result.Status, &result.Biz, &result.BizData); err != nil {
			return nil, err
		}
		results = append(results, result)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return results, nil
}

func (es *eventStorage) UpdateStatusByBizs(bizs []string, status Status) error {
	sqlPlaceHolder := make([]string, 0, len(bizs))
	sqlArgs := make([]interface{}, 0, len(bizs)+1)
	sqlArgs = append(sqlArgs, status)
	for _, biz := range bizs {
		sqlPlaceHolder = append(sqlPlaceHolder, "?")
		sqlArgs = append(sqlArgs, biz)
	}
	sqlBaseStr := "update event set status = ? where biz in (%s)"
	sqlStr := fmt.Sprintf(sqlBaseStr, strings.Join(sqlPlaceHolder, ","))
	if _, err := es.db.Exec(sqlStr, sqlArgs...); err != nil {
		return err
	}
	return nil
}
