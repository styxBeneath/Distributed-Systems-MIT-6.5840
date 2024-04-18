package shardkv

type ShardState int

func (s *ShardState) String() string {
	tmp := ""
	switch *s {
	case Serving:
		tmp = "Serving"
	case Pulling:
		tmp = "Pulling"
	case BePulling:
		tmp = "BePulling"
	case GCing:
		tmp = "GCing"
	}

	return tmp
}

const (
	Serving ShardState = iota
	Pulling
	BePulling
	GCing
)

type Shard struct {
	Data  map[string]string
	State ShardState
}

func (s *Shard) put(key string, value string) {
	s.Data[key] = value
}

func (s *Shard) appendVal(key string, value string) {
	originVal := s.Data[key]
	s.Data[key] = originVal + value
}

func (s *Shard) hasKey(key string) bool {
	_, ok := s.Data[key]
	return ok
}

func (s *Shard) get(key string) string {
	return s.Data[key]
}

func NewShard(state ShardState) *Shard {
	return &Shard{
		Data:  make(map[string]string),
		State: state,
	}
}

func (s *Shard) deepCopy() Shard {
	newData := make(map[string]string, len(s.Data))
	for k, v := range s.Data {
		newData[k] = v
	}

	return Shard{
		Data:  newData,
		State: Serving,
	}
}
