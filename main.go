package main

import (
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Queue struct {
	msgs    []string
	waiters []chan string
}

var (
	queues = make(map[string]*Queue)
	mu     sync.Mutex
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: go run main.go <port>")
		os.Exit(1)
	}
	port := os.Args[1]

	http.HandleFunc("/", handler)

	addr := ":" + port
	fmt.Println("listening on", port)
	if err := http.ListenAndServe(addr, nil); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func handler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPut:
		queueName := strings.TrimPrefix(r.URL.Path, "/")

		if queueName == "" {
			http.Error(w, "", http.StatusBadRequest)
			return
		}
		v := r.URL.Query().Get("v")

		if v == "" {
			http.Error(w, "", http.StatusBadRequest)
			return
		}

		mu.Lock()

		val, ok := queues[queueName]
		if !ok {
			queues[queueName] = &Queue{
				msgs:    []string{},
				waiters: make([]chan string, 0),
			}
			val = queues[queueName]
		}

		if len(val.waiters) != 0 {
			frst := val.waiters[0]
			val.waiters = val.waiters[1:]
			mu.Unlock()
			frst <- v
		} else {
			val.msgs = append(val.msgs, v)
			mu.Unlock()
		}

		w.WriteHeader(http.StatusOK)
		return

	case http.MethodGet:
		queueName := strings.TrimPrefix(r.URL.Path, "/")

		if queueName == "" {
			http.Error(w, "", http.StatusBadRequest)
			return
		}

		mu.Lock()
		q := queues[queueName]

		if q == nil {
			q = &Queue{}
			queues[queueName] = q
		}

		if len(q.msgs) > 0 {
			msg := q.msgs[0]
			q.msgs = q.msgs[1:]
			mu.Unlock()
			w.Write([]byte(msg))
			return
		}

		timeoutStr := r.URL.Query().Get("timeout")

		if timeoutStr == "" {
			mu.Unlock()
			http.Error(w, "", http.StatusNotFound)
			return
		}

		timeout, err := strconv.Atoi(timeoutStr)

		if err != nil || timeout < 0 {
			mu.Unlock()
			http.Error(w, "", http.StatusBadRequest)
			return
		}

		ch := make(chan string, 1)

		q.waiters = append(q.waiters, ch)
		mu.Unlock()

		select {
		case msg := <-ch:
			w.Write([]byte(msg))
		case <-time.After(time.Duration(timeout) * time.Second):
			mu.Lock()
			for i, waiterCh := range q.waiters {
				if waiterCh == ch {
					q.waiters = append(q.waiters[:i], q.waiters[i+1:]...)
					break
				}
			}
			mu.Unlock()

			http.Error(w, "", http.StatusNotFound)
		}

		return

	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}
