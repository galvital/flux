package flux

import "fmt"

const (
	// InvalidEncoding is returned when the Serializer cannot marshal the event
	ErrInvalidEncoding = "InvalidEncoding"

	// UnboundEventType when the Serializer cannot unmarshal the serialized event
	ErrUnboundEventType = "UnboundEventType"

	// AggregateNotFound will be returned when attempting to Load an aggregateID
	// that does not exist in the Store
	ErrAggregateNotFound = "AggregateNotFound"

	// UnhandledEvent occurs when the Aggregate is unable to handle an event and returns
	// a non-nill err
	ErrUnhandledEvent = "UnhandledEvent"

	// ErrUnexpectedDbError is returned when an unexpected db error occurs.
	ErrUnexpectedDbError = "UnexpectedDbError"

	ErrOptimisticConcurrencyError = "ErrOptimisticConcurrencyError"

	ErrProducerError = "ProdocerError"

	ErrConsumerError = "ConsumerError"

	ErrEventHandlerError = "EventHandlerError"

	ErrHandlerNotRegistered = "HandlerNotRegistered"
)

// Error provides a standardized error interface for eventsource
type Error interface {
	error

	// Returns the original error if one was set.  Nil is returned if not set.
	Cause() error

	// Returns the short phrase depicting the classification of the error.
	Code() string

	// Returns the error details message.
	Message() string
}

type baseErr struct {
	cause   error
	code    string
	message string
}

func (b *baseErr) Cause() error    { return b.cause }
func (b *baseErr) Code() string    { return b.code }
func (b *baseErr) Message() string { return b.message }
func (b *baseErr) Error() string   { return fmt.Sprintf("[%v] %v - %v", b.code, b.message, b.cause) }
func (b *baseErr) String() string  { return b.Error() }

// NewError generates the common error structure
func NewError(err error, code, format string, args ...interface{}) error {
	return &baseErr{
		code:    code,
		message: fmt.Sprintf(format, args...),
		cause:   err,
	}
}
