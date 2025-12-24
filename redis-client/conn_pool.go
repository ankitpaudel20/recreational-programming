package main

import (
	"errors"
	"fmt"
	"sync/atomic"
	"time"
)

type ResourcePool[T any] struct {
	pool          chan T
	max_size      int
	min_size      int
	init_func     func() (T, error)
	cumulative_ms atomic.Int64
}

func NewConnectionPool[T any](min_size int, max_size int, init_func func() (T, error)) (*ResourcePool[T], error) {
	if min_size > max_size {
		return &ResourcePool[T]{}, errors.New("maxsize must be less than initial size")
	}
	if min_size <= 0 {
		min_size = 1
	}

	pool := make(chan T, max_size)
	for _ = range min_size {
		instance, err := init_func()
		if err != nil {
			return &ResourcePool[T]{}, err
		}
		pool <- instance
	}
	ret := ResourcePool[T]{pool: pool, max_size: max_size, min_size: min_size, init_func: init_func}
	go ret.PoolTuner()

	return &ret, nil
}

func (p *ResourcePool[T]) PoolTuner() {
	for {
		time.Sleep(1 * time.Second)

		if p.cumulative_ms.Load() > 5 {
			fmt.Println("waited %d ms in last secs, increasing number of instances", p.cumulative_ms.Load())
			instance, err := p.init_func()
			if err != nil {
				fmt.Println("error adding a new connection %s", err.Error())
			}
			p.pool <- instance
		}
		p.cumulative_ms.Swap(0)
	}
}

func (p *ResourcePool[T]) Get() T {
	start := time.Now()
	instance := <-p.pool
	p.cumulative_ms.Add(time.Since(start).Milliseconds())
	return instance
}

func (p *ResourcePool[T]) Release(instance T) {
	p.pool <- instance
}
