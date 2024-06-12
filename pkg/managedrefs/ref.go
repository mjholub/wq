package managedrefs

// ManagedRef is a reference to a value that can be safely accessed from multiple goroutines.
type ManagedRef[T any] struct {
	value   T
	control chan func(*T)
}

// NewManagedRef creates a new managed reference with the given initial value.
func NewManagedRef[T any](initial T) *ManagedRef[T] {
	ref := &ManagedRef[T]{
		value:   initial,
		control: make(chan func(*T)),
	}
	go ref.run()
	return ref
}

func (ref *ManagedRef[T]) run() {
	for f := range ref.control {
		f(&ref.value)
	}
}
