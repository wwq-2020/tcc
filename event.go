package tcc

// Status Status
type Status int

const (
	// StatusInit StatusInit
	StatusInit Status = iota
	// StatusTried StatusTried
	StatusTried
	// StatusCanceled StatusCanceled
	StatusCanceled
	// StatusConfirmed StatusConfirmed
	StatusConfirmed
)

// Event Event
type Event struct {
	ID      int64
	Status  Status
	Biz     string
	BizData string
}


