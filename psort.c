#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <pthread.h>
#include <bits/time.h>
#include <time.h>

/* An array slice: base pointer and number of elements */
struct chunk {
	int *base;
	int count;
};

/* Simple binary heap implementation as described by
 * https://en.wikipedia.org/wiki/Binary_heap
 *
 * The heap contains `struct chunk` elements ordered by the first
 * chunk element using function `compare`.
 */
struct heap {
	int (*compare)(int, int, void *);
	struct chunk *elements;
	void *arg;
	int capacity;
	int count;
};

/* Indexes for binary tree encoded as an array */
static inline int left_child(int i) { return 2 * i + 1; }
static inline int right_child(int i) { return 2 * i + 2; }
static inline int parent(int i) { return (i - 1) / 2; }

static inline void swap(struct chunk *a, struct chunk *b)
{
	struct chunk t = *a;

	*a = *b;
	*b = t;
}

static inline int greater(struct heap *heap, struct chunk a, struct chunk b)
{
	return heap->compare(a.base[0], b.base[0], heap->arg) > 0;
}

static int heap_push(struct heap *heap, struct chunk elt)
{
	if (heap->count == heap->capacity)
		return -E2BIG;

	struct chunk *elements = heap->elements;
	int i = heap->count;
	elements[i] = elt;
	heap->count++;
	while (i != 0 && greater(heap, elements[parent(i)], elements[i])) {
		swap(&elements[i], &elements[parent(i)]);
		i = parent(i);
	}
	return 0;
}

static inline void sink_root(struct heap *heap)
{
	struct chunk *elements = heap->elements;
	int i = 0;

	while ((left_child(i)  < heap->count && greater(heap, elements[i], elements[left_child(i)])) ||
	       (right_child(i) < heap->count && greater(heap, elements[i], elements[right_child(i)]))) {
		if (right_child(i) >= heap->count || greater(heap, elements[right_child(i)], elements[left_child(i)])) {
			swap(&elements[i], &elements[left_child(i)]);
			i = left_child(i);
		} else {
			swap(&elements[i], &elements[right_child(i)]);
			i = right_child(i);
		}
	}
}

static void heap_pop(struct heap *heap)
{
	if (heap->count == 0)
		return;

	struct chunk *elements = heap->elements;
	elements[0] = elements[heap->count - 1];
	--heap->count;
	sink_root(heap);
}

static int heap_push_replace(struct heap *heap, struct chunk chunk)
{
	if (heap->count == 0)
		return -ESRCH;

	heap->elements[0] = chunk;
	sink_root(heap);
	return 0;
}

struct sort_job {
	int (*compare)(int, int, void *);
	void *arg;
	struct chunk chunk;
};

static int qsort_cmp(const void *pa, const void *pb, void *pjob)
{
	struct sort_job *job = pjob;
	const int *a = pa;
	const int *b = pb;

	return job->compare(*a, *b, job->arg);
}

static void *sort_thread_fn(void *pjob) {
	struct sort_job *job = pjob;
	struct chunk chunk = job->chunk;

	qsort_r(chunk.base, chunk.count, sizeof(*chunk.base), qsort_cmp, pjob);
	return 0;
}

/* `posrt` - parallel sort.
 * Sort array of integers `base` of `nmemb` members using compare
 * function `compare`. `arg` is passed as a 3rd argument if `compare`.
 * `compare` should return the following values:
 * - compare(a, b) < 0  if a < b
 * - compare(a, b) > 0  if a > b
 * - compare(a, b) == 0 if a == b
 * Sorting is done in parallel using `nr_jobs` thread.
 *
 * Technically, the array `base` is split in `nr_jobs` chunks,
 * each chunk is sorted in parallel using `qsort`, chunks are
 * then merged using a priority queue in a single thread.
 */
int psort(int *base, size_t nmemb, int (*compare)(int, int, void *), void *arg, size_t nr_jobs)
{
	pthread_t threads[nr_jobs];
	struct sort_job jobs[nr_jobs];
	size_t nmemb_per_job = nmemb / nr_jobs;
	int *result;

	/* Prepare chunks and initiate chunk sorting thread */
	for (int i = 0; i < nr_jobs; ++i) {
		struct sort_job *job = &jobs[i];
		int err, j;

		job->compare = compare;
		job->arg = arg;
		job->chunk.base = &base[nmemb_per_job * i];
		job->chunk.count = nmemb_per_job;
		if (i == nr_jobs - 1)
			job->chunk.count += nmemb % nr_jobs;

		err = pthread_create(&threads[i], NULL, sort_thread_fn, &jobs[i]);
		if (err) {
			for (j = 0; j < i; ++i)
				pthread_cancel(threads[j]);
			return err;
		}
	}
	for (int i = 0; i < nr_jobs; ++i)
		pthread_join(threads[i], NULL);

	/* Initialize priority queue of `nr_jobs` elements,
	 * initially put minimal element from each chunk to the queue,
	 * then pop minimal element from the queue and refill it with
	 * the next element from element's chunk.
	 */
	struct chunk heap_buf[nr_jobs];
	struct heap heap = {
		.compare = compare,
		.elements = heap_buf,
		.arg = arg,
		.capacity = nr_jobs,
		.count = 0,
	};

	result = calloc(nmemb, sizeof(int));
	if (!result)
		return -ENOMEM;

	for (int i = 0; i < nr_jobs; ++i) {
		if (jobs[i].chunk.count)
			heap_push(&heap, jobs[i].chunk);
	}

	for (int i = 0; heap.count; ++i) {
		struct chunk chunk = heap.elements[0];

		result[i] = *chunk.base;
		--chunk.count;
		++chunk.base;
		if (chunk.count)
			heap_push_replace(&heap, chunk);
		else
			heap_pop(&heap);
	}

	memcpy(base, result, sizeof(*result) * nmemb);
	free(result);
	return 0;
}
