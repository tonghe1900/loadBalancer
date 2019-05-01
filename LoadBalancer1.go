package main

import (
	"container/heap"
	"fmt"
	"time"
)
const nWorker = 5

type Request struct {
	fn func() int  // The operation to perform.
	c  chan int    // The channel to return the result.
}


func requester(work chan<- Request) {
	c := make(chan int)
	for {
		// Kill some time (fake load).

		fmt.Println("put work into channel")
		work <- Request{workFn, c} // send request
		result := <-c              // wait for answer
		furtherProcess(result)
	}
}

func workFn() int{
	time.Sleep(1* 1000*1000*1000)
	return 2
}

func furtherProcess(input int){
	fmt.Println(input)
}


type Worker struct {
	name string
	requests chan Request // work to do (buffered channel)
	pending  int          // count of pending tasks
	index     int         // index in the heap
}


func (w *Worker) work(done chan *Worker) {
	for {
		req := <-w.requests // get Request from balancer
		req.c <- req.fn()   // call fn and send result
		time.Sleep(3*1000*1000*1000)
		fmt.Println(w.name ,"execute the task")


		done <- w           // we've finished this request

	}
}


type Pool []*Worker


func (pq *Pool) Push(x interface{}) {
	n := len(*pq)
	item := x.(*Worker)
	item.index = n
	*pq = append(*pq, item)
}

func (pq *Pool) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	item.index = -1 // for safety
	*pq = old[0 : n-1]
	return item
}





//Len() int
//// Less reports whether the element with
//// index i should sort before the element with index j.
//Less(i, j int) bool
//// Swap swaps the elements with indexes i and j.
//Swap(i, j int)

func (p Pool) Len() int {
	return len(p)

}

func (pq Pool) Swap(i, j int)  {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j


}

func (p Pool) Less(i, j int) bool {
	return p[i].pending < p[j].pending
}






type Balancer struct {
	pool Pool
	done chan *Worker
}


func (b *Balancer) balance(work chan Request) {
	for {
		select {
		case req := <-work: // received a Request...
			b.dispatch(req) // ...so send it to a Worker
		case w := <-b.done: // a worker has finished ...
			b.completed(w)  // ...so update its info
		}
	}
}





// Send Request to worker
func (b *Balancer) dispatch(req Request) {
	// Grab the least loaded worker...
	w := heap.Pop(&b.pool).(*Worker)
	go w.work(b.done)
	// ...send it the task.
	w.requests <- req
	// One more in its work queue.

	w.pending++
	// Put it into its place on the heap.
	heap.Push(&b.pool, w)

}

// Job is complete; update heap
func (b *Balancer) completed(w *Worker) {
	// One fewer in the queue.
	w.pending--
	// Remove it from heap.
	heap.Remove(&b.pool, w.index)
	// Put it into its place on the heap.
	heap.Push(&b.pool, w)
}

func main(){
	//pool Pool
	//done chan *Worker
	pool := make([]*Worker, 0)

	//requests chan Request // work to do (buffered channel)
	//pending  int          // count of pending tasks
	//index     int
	worker1 := &Worker{"worker1",make(chan Request), 0, 0}
	worker2:= &Worker{"worker2",make(chan Request), 0, 1}
	worker3 := &Worker{"worker3",make(chan Request), 0, 2}
	pool = append(pool, worker1)
	pool = append(pool, worker2)
	pool = append(pool, worker3)



	done := make(chan *Worker, 10)

    balancer:=Balancer{pool: pool,done:done}
	work := make(chan Request, 100)
	go requester(work)
	go balancer.balance(work)
	time.Sleep(100*1000*1000*1000)


}
