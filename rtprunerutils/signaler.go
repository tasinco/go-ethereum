package rtprunerutils

var (
	_ Signal = (*noSignal)(nil)
	_ Signal = (*signal)(nil)
)

type Signal interface {
	Signal()
}

func NewNoSignal() Signal {
	return &noSignal{}
}

type noSignal struct {
}

func (*noSignal) Signal() {

}

type signal struct {
	ping chan struct{}
}

func NewSignal(ch chan struct{}) Signal {
	return &signal{
		ping: ch,
	}
}

func (s *signal) Signal() {
	select {
	case s.ping <- struct{}{}:
	default:
	}
}
