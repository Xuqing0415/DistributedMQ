package namespace

import (
	"fmt"
	"sync"
	"time"
)

type Namespace struct {
	Name        string
	Topics      []string
	Quota       Quota
	ACLs        []ACL
	CreatedAt   time.Time
	ModifiedAt  time.Time
}

type Quota struct {
	MaxMessagesPerSecond int64
	MaxBytesPerSecond    int64
	MaxTopics            int
	MaxPartitions        int
}

type ACL struct {
	UserID   string
	Topic    string
	Actions  []Action
	Allow    bool
}

type Action string

const (
	ActionProduce Action = "PRODUCE"
	ActionConsume Action = "CONSUME"
	ActionAdmin   Action = "ADMIN"
)

type NamespaceManager struct {
	mu          sync.RWMutex
	namespaces  map[string]*Namespace
	topicToNS   map[string]string
	userToNS    map[string][]string
}

func NewNamespaceManager() *NamespaceManager {
	return &NamespaceManager{
		namespaces:  make(map[string]*Namespace),
		topicToNS:   make(map[string]string),
		userToNS:    make(map[string][]string),
	}
}

func (nm *NamespaceManager) CreateNamespace(name string, quota Quota) (*Namespace, error) {
	nm.mu.Lock()
	defer nm.mu.Unlock()

	if _, exists := nm.namespaces[name]; exists {
		return nil, fmt.Errorf("namespace %s already exists", name)
	}

	ns := &Namespace{
		Name:        name,
		Topics:      make([]string, 0),
		Quota:       quota,
		ACLs:        make([]ACL, 0),
		CreatedAt:   time.Now(),
		ModifiedAt:  time.Now(),
	}

	nm.namespaces[name] = ns
	return ns, nil
}

func (nm *NamespaceManager) GetNamespace(name string) (*Namespace, bool) {
	nm.mu.RLock()
	defer nm.mu.RUnlock()

	ns, ok := nm.namespaces[name]
	return ns, ok
}

func (nm *NamespaceManager) DeleteNamespace(name string) error {
	nm.mu.Lock()
	defer nm.mu.Unlock()

	ns, exists := nm.namespaces[name]
	if !exists {
		return fmt.Errorf("namespace %s not found", name)
	}

	for _, topic := range ns.Topics {
		delete(nm.topicToNS, topic)
	}

	delete(nm.namespaces, name)
	return nil
}

func (nm *NamespaceManager) AddTopic(namespace, topic string) error {
	nm.mu.Lock()
	defer nm.mu.Unlock()

	ns, exists := nm.namespaces[namespace]
	if !exists {
		return fmt.Errorf("namespace %s not found", namespace)
	}

	if len(ns.Topics) >= ns.Quota.MaxTopics && ns.Quota.MaxTopics > 0 {
		return fmt.Errorf("namespace %s exceeded max topics limit", namespace)
	}

	for _, t := range ns.Topics {
		if t == topic {
			return nil
		}
	}

	ns.Topics = append(ns.Topics, topic)
	ns.ModifiedAt = time.Now()
	nm.topicToNS[topic] = namespace

	return nil
}

func (nm *NamespaceManager) RemoveTopic(namespace, topic string) error {
	nm.mu.Lock()
	defer nm.mu.Unlock()

	ns, exists := nm.namespaces[namespace]
	if !exists {
		return fmt.Errorf("namespace %s not found", namespace)
	}

	for i, t := range ns.Topics {
		if t == topic {
			ns.Topics = append(ns.Topics[:i], ns.Topics[i+1:]...)
			ns.ModifiedAt = time.Now()
			delete(nm.topicToNS, topic)
			return nil
		}
	}

	return fmt.Errorf("topic %s not found in namespace %s", topic, namespace)
}

func (nm *NamespaceManager) GetNamespaceByTopic(topic string) (*Namespace, bool) {
	nm.mu.RLock()
	defer nm.mu.RUnlock()

	nsName, ok := nm.topicToNS[topic]
	if !ok {
		return nil, false
	}

	ns, ok := nm.namespaces[nsName]
	return ns, ok
}

func (nm *NamespaceManager) AddACL(namespace, userID, topic string, actions []Action, allow bool) error {
	nm.mu.Lock()
	defer nm.mu.Unlock()

	ns, exists := nm.namespaces[namespace]
	if !exists {
		return fmt.Errorf("namespace %s not found", namespace)
	}

	acl := ACL{
		UserID:   userID,
		Topic:    topic,
		Actions:  actions,
		Allow:    allow,
	}

	ns.ACLs = append(ns.ACLs, acl)
	ns.ModifiedAt = time.Now()

	if _, exists := nm.userToNS[userID]; !exists {
		nm.userToNS[userID] = make([]string, 0)
	}

	found := false
	for _, name := range nm.userToNS[userID] {
		if name == namespace {
			found = true
			break
		}
	}
	if !found {
		nm.userToNS[userID] = append(nm.userToNS[userID], namespace)
	}

	return nil
}

func (nm *NamespaceManager) CheckPermission(userID, topic string, action Action) (bool, error) {
	nm.mu.RLock()
	defer nm.mu.RUnlock()

	nsName, ok := nm.topicToNS[topic]
	if !ok {
		return true, nil
	}

	ns, ok := nm.namespaces[nsName]
	if !ok {
		return false, fmt.Errorf("namespace not found for topic %s", topic)
	}

	defaultAllow := true
	for _, acl := range ns.ACLs {
		if acl.UserID != userID {
			continue
		}

		if acl.Topic != "" && acl.Topic != topic {
			continue
		}

		for _, act := range acl.Actions {
			if act == action || act == ActionAdmin {
				return acl.Allow, nil
			}
		}
	}

	return defaultAllow, nil
}

func (nm *NamespaceManager) GetAllNamespaces() []*Namespace {
	nm.mu.RLock()
	defer nm.mu.RUnlock()

	result := make([]*Namespace, 0, len(nm.namespaces))
	for _, ns := range nm.namespaces {
		result = append(result, ns)
	}

	return result
}