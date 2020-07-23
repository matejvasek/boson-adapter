package invocationStatus

type Type int

const (
	Unhandled = Type(0b0)
	Failed    = Type(0b1)
	Succeeded = Type(0b10)
	Timeout   = 0b100 | Failed
)

func (t Type) String() string {
	switch t {
	case Unhandled:
		return "Unhandled"
	case Failed:
		return "Failed"
	case Succeeded:
		return "Succeeded"
	case Timeout:
		return "Timeout"
	}
	panic("unreachable")
}

