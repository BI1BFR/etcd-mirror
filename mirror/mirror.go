package mirror

import (
	"path"
	"sync"
	"time"

	"github.com/coreos/go-etcd/etcd"
)

type Logger interface {
	Info(s string)
	Error(s string)
}

type Node struct {
	Dir   bool
	Value string
	Sub   map[string]struct{}
}

type Mirror struct {
	mtx   sync.RWMutex
	nodes map[string]*Node
	base  string
}

func New(addrs []string, path string, updated chan<- string, logger Logger) (*Mirror, error) {
	client := etcd.NewClient(addrs)
	r, err := client.Get(path, false, true)
	if err != nil {
		if logger != nil {
			logger.Error(err.Error())
		}
		return nil, err
	}

	m := &Mirror{
		nodes: make(map[string]*Node),
		base:  path,
	}

	m.init(r.Node)

	go func() {
		waitIndex := r.EtcdIndex + 1

		for {
			r, err := client.Watch(path, waitIndex, true, nil, nil)
			if err != nil {
				if logger != nil {
					logger.Error("watch failed: " + err.Error() + ", retry after 3 second...")
				}
				time.Sleep(3 * time.Second)
				continue
			}

			m.update(r.Action, r.Node)

			if updated != nil {
				updated <- r.Node.Key
			}
			if logger != nil {
				logger.Info("updated: " + r.Action + " on " + r.Node.Key)
			}

			waitIndex = r.Node.ModifiedIndex + 1
		}
	}()

	return m, nil
}

func (m *Mirror) Get(key string) *Node {
	m.mtx.RLock()
	defer m.mtx.RUnlock()

	if n, ok := m.nodes[key]; ok {
		if n.Dir {
			nn := dirNode()
			for k := range n.Sub {
				nn.Sub[k] = struct{}{}
			}
			return nn
		} else {
			return valueNode(n.Value)
		}
	}

	return nil
}

func (m *Mirror) init(n *etcd.Node) {
	if !n.Dir {
		m.nodes[n.Key] = valueNode(n.Value)
	} else {
		d := dirNode()

		for _, c := range n.Nodes {
			m.init(c)
			d.Sub[c.Key] = struct{}{}
		}
		m.nodes[n.Key] = d
	}
}

func (m *Mirror) update(action string, n *etcd.Node) {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	switch action {
	case "set", "update", "create", "compareAndSwap":
		m.set(n.Key, n.Value, n.Dir)
	case "delete", "expire", "CompareAndDelete":
		m.delete(n.Key)
	}
}

func (m *Mirror) set(key, value string, dir bool) {
	if pre, ok := m.nodes[key]; ok {
		pre.Value = value
		return
	}

	if dir {
		m.nodes[key] = dirNode()
	} else {
		m.nodes[key] = valueNode(value)
	}

	// create parent nodes
	for {
		if key == m.base || key == "/" {
			break
		}

		parent := path.Dir(key)
		if n, ok := m.nodes[parent]; ok {
			n.Sub[key] = struct{}{}
			break
		}

		n := dirNode()
		n.Sub[key] = struct{}{}
		m.nodes[parent] = n

		key = parent
	}
}

func (m *Mirror) delete(key string) {
	if pre, ok := m.nodes[key]; ok {
		if pre.Dir {
			for k := range pre.Sub {
				m.delete(k)
			}
		}

		delete(m.nodes, key)

		// delete from parent
		if key == m.base || key == "/" {
			return
		}

		parent := path.Dir(key)
		if n, ok := m.nodes[parent]; ok {
			delete(n.Sub, key)
		}
	}
}

func valueNode(v string) *Node {
	return &Node{
		Value: v,
	}
}

func dirNode() *Node {
	return &Node{
		Dir: true,
		Sub: make(map[string]struct{}),
	}
}
