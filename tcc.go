package tcc

import (
	"encoding/json"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
)

// Factory Factory
type Factory func() interface{}

// Participant Participant
type Participant interface {
	Try(interface{}) error
	Confirm(interface{}) error
	Cancel(interface{}) error
}

// TCC TCC
type TCC interface {
}

type tcc struct {
	biz2Paticipants map[string][]Participant
	biz2Factory     map[string]Factory
	eventStorage    EventStorage
	sync.RWMutex
}

// New 初始化
func New(eventStorage EventStorage) TCC {
	return &tcc{
		biz2Paticipants: make(map[string][]Participant),
		biz2Factory:     make(map[string]Factory),
		eventStorage:    eventStorage,
	}
}

func (t *tcc) RegisterPaticipants(biz string, participants ...Participant) {
	t.biz2Paticipants[biz] = participants
}

func (t *tcc) RegisterFactory(biz string, factory Factory) {
	t.biz2Factory[biz] = factory
}

func (t *tcc) Handle(biz string, arg interface{}) error {
	bizData, err := json.Marshal(arg)
	if err != nil {
		return err
	}
	event := &Event{Status: StatusInit, Biz: biz, BizData: string(bizData)}
	if err := t.eventStorage.Create(event); err != nil {
		return err
	}
	var eg errgroup.Group
	t.Lock()
	participants := t.biz2Paticipants[biz]
	t.Unlock()
	for _, participant := range participants {
		each := participant
		eg.Go(func() error {
			return each.Try(arg)
		})
	}
	if err := eg.Wait(); err != nil {
		go t.cancel2Success(arg, participants...)
		return err
	}

	if err := t.eventStorage.UpdateStatusByBiz(biz, StatusTried); err != nil {
		go func() {
			t.setEventStatus2Success(biz, StatusTried)
			t.confirm2Success(arg, participants...)
			t.setEventStatus2Success(biz, StatusConfirmed)
		}()
		return err
	}

	for _, participant := range participants {
		each := participant
		eg.Go(func() error {
			return each.Confirm(arg)
		})
	}

	if err := eg.Wait(); err != nil {
		go func() {
			t.confirm2Success(arg, participants...)
			t.setEventStatus2Success(biz, StatusConfirmed)
		}()
		return err
	}

	if err := t.eventStorage.UpdateStatusByBiz(biz, StatusConfirmed); err != nil {
		go t.setEventStatus2Success(biz, StatusConfirmed)
		return err
	}

	return nil
}

func (t *tcc) cancel2Success(arg interface{}, participants ...Participant) {
	var eg errgroup.Group
	for _, participant := range participants {
		each := participant
		eg.Go(func() error {
			for {
				if err := each.Cancel(arg); err == nil {
					break
				}
				time.Sleep(time.Second)
			}
			return nil
		})
	}
	eg.Wait()
}

func (t *tcc) confirm2Success(arg interface{}, participants ...Participant) {
	var eg errgroup.Group
	for _, participant := range participants {
		each := participant
		eg.Go(func() error {
			for {
				if err := each.Confirm(arg); err == nil {
					break
				}
				time.Sleep(time.Second)
			}
			return nil
		})
	}
	eg.Wait()

}

func (t *tcc) setEventStatus2Success(biz string, status Status) {
	for {
		if err := t.eventStorage.UpdateStatusByBiz(biz, status); err == nil {
			break
		}
		time.Sleep(time.Second)
	}
}

func (t *tcc) Recovery() {
	for {
		events := t.findEvents2Recovery()
		if len(events) == 0 {
			return
		}
		t.handleEvents(events)
	}
}

func (t *tcc) findEvents2Recovery() []*Event {
	for {
		events, err := t.eventStorage.FindEvents2Recovery([]Status{StatusInit, StatusTried})
		if err == nil {
			return events
		}
		time.Sleep(time.Second)
	}
}

func (t *tcc) handleEvents(events []*Event) {
	for _, event := range events {
		t.handleEvent2Success(event)
	}
}

func (t *tcc) handleEvent2Success(event *Event) {
	t.Lock()
	participants := t.biz2Paticipants[event.Biz]
	factory := t.biz2Factory[event.Biz]
	t.Unlock()
	arg := factory()
	if err := json.Unmarshal([]byte(event.BizData), arg); err != nil {
		return
	}
	switch event.Status {
	case StatusInit:
		t.cancel2Success(arg, participants...)
		t.setEventStatus2Success(event.Biz, StatusCanceled)
	case StatusTried:
		t.confirm2Success(arg, participants...)
		t.setEventStatus2Success(event.Biz, StatusConfirmed)
	}
}
