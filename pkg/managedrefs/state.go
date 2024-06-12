package managedrefs

// Get returns the value of the reference.
func (ref *ManagedRef[T]) Get() T {
	result := make(chan T)
	ref.control <- func(v *T) {
		result <- *v
	}

	return <-result
}

// Set sets the value of the reference.
func (ref *ManagedRef[T]) Set(value T) {
	ref.control <- func(v *T) {
		*v = value
	}
}

// Update applies the given function to the value of the reference.
func (ref *ManagedRef[T]) Update(f func(T) T) {
	ref.control <- func(v *T) {
		*v = f(*v)
	}
}
