package cache

import (
	"sync"
	"time"
)

type TypedCacheSnapshotEntry[T any] struct {
	Key          string
	TypeKey      string
	Value        T
	CreatedAt    *time.Time
	LastAccessAt *time.Time
	ExpiresAt    *time.Time
}

type TypedCache[T any] struct {
	entries map[string]map[string]*CacheEntry[T]
	mu      sync.RWMutex
	ttl     time.Duration
}

func NewTypedCache[T any](ttl time.Duration) *TypedCache[T] {
	c := &TypedCache[T]{
		entries: make(map[string]map[string]*CacheEntry[T]),
		ttl:     ttl,
	}
	gcFuncs = append(gcFuncs, c.GC)
	return c
}

func (c *TypedCache[T]) SetType(key, typeKey string, value T) {
	c.SetTypeWithExpirable(key, typeKey, value, ExpirationTime(time.Now().Add(c.ttl)))
}

func (c *TypedCache[T]) SetTypeWithTTL(key, typeKey string, value T, ttl time.Duration) {
	c.SetTypeWithExpirable(key, typeKey, value, ExpirationTime(time.Now().Add(ttl)))
}

func (c *TypedCache[T]) SetTypeWithExpirable(key, typeKey string, value T, exp Expirable) {
	c.mu.Lock()
	defer c.mu.Unlock()
	cache, exists := c.entries[key]
	if !exists {
		cache = make(map[string]*CacheEntry[T])
		c.entries[key] = cache
	}

	now := time.Now()
	cache[typeKey] = &CacheEntry[T]{
		CreatedAt:    now,
		LastAccessAt: now,
		data:      value,
		Expirable: exp,
	}
}

func (c *TypedCache[T]) GetType(key, typeKey string) (T, bool) {
	c.mu.RLock()
	cache, exists := c.entries[key]
	if !exists {
		c.mu.RUnlock()
		return *new(T), false
	}
	entry, exists := cache[typeKey]
	if !exists {
		c.mu.RUnlock()
		return *new(T), false
	}
	expired := entry.Expired()
	c.mu.RUnlock()

	if !expired {
		c.mu.Lock()
		if current := cache[typeKey]; current == entry {
			current.LastAccessAt = time.Now()
		}
		c.mu.Unlock()
		return entry.data, true
	}

	c.mu.Lock()
	if cache[typeKey] == entry {
		delete(cache, typeKey)
		if len(cache) == 0 {
			delete(c.entries, key)
		}
		c.mu.Unlock()
		return *new(T), false
	}
	c.mu.Unlock()
	return *new(T), false
}

func (c *TypedCache[T]) TouchType(key, typeKey string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	cache, exists := c.entries[key]
	if !exists {
		return false
	}
	entry, exists := cache[typeKey]
	if !exists || entry == nil || entry.Expired() {
		return false
	}
	entry.LastAccessAt = time.Now()
	return true
}

func (c *TypedCache[T]) DeleteKey(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.entries, key)
}

func (c *TypedCache[T]) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries = make(map[string]map[string]*CacheEntry[T])
}

func (c *TypedCache[T]) GC() {
	c.mu.Lock()
	defer c.mu.Unlock()
	expiredKeys := make(map[string][]string)
	for tk, entries := range c.entries {
		for key, entry := range entries {
			if !entry.Expired() {
				continue
			}
			if _, ok := expiredKeys[tk]; !ok {
				expiredKeys[tk] = make([]string, 0, len(entries))
			}
			expiredKeys[tk] = append(expiredKeys[tk], key)
		}
	}
	for tk, keys := range expiredKeys {
		for _, key := range keys {
			delete(c.entries[tk], key)
		}
		if len(c.entries[tk]) == 0 {
			delete(c.entries, tk)
		}
	}
}

func (c *TypedCache[T]) SnapshotEntries() []TypedCacheSnapshotEntry[T] {
	c.mu.RLock()
	snapshots := make([]TypedCacheSnapshotEntry[T], 0)
	expiredKeys := make(map[string][]string)
	for key, entries := range c.entries {
		for typeKey, entry := range entries {
			if entry == nil {
				continue
			}
			if entry.Expired() {
				expiredKeys[key] = append(expiredKeys[key], typeKey)
				continue
			}
			var expiresAt *time.Time
			if exp, ok := entry.Expirable.(ExpirationTime); ok {
				t := time.Time(exp)
				expiresAt = &t
			}
			createdAt := entry.CreatedAt
			lastAccessAt := entry.LastAccessAt
			snapshots = append(snapshots, TypedCacheSnapshotEntry[T]{
				Key:          key,
				TypeKey:      typeKey,
				Value:        entry.data,
				CreatedAt:    &createdAt,
				LastAccessAt: &lastAccessAt,
				ExpiresAt:    expiresAt,
			})
		}
	}
	c.mu.RUnlock()

	if len(expiredKeys) == 0 {
		return snapshots
	}

	c.mu.Lock()
	for key, typeKeys := range expiredKeys {
		entries, ok := c.entries[key]
		if !ok {
			continue
		}
		for _, typeKey := range typeKeys {
			entry, exists := entries[typeKey]
			if !exists || entry == nil || !entry.Expired() {
				continue
			}
			delete(entries, typeKey)
		}
		if len(entries) == 0 {
			delete(c.entries, key)
		}
	}
	c.mu.Unlock()
	return snapshots
}
