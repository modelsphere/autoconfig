package controller

import (
	"reflect"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// statusSettled reports whether a status write can be skipped.
//
// Every reconcile used to end in a Status().Update, because lastSyncTime was stamped
// unconditionally and therefore ALWAYS differed. That write is an update event on the object
// itself, and the controllers watch their own kind with no predicate, so each round enqueued
// the next one: a loop paced only by how long a reconcile takes, rather than by the resync
// interval. It is worst where a round is expensive - APIConsumer reads every consumer's
// Secret uncached, so N consumers cost N^2 GETs per iteration.
//
// Skipping the write when only lastSyncTime would move breaks that cycle. The field keeps a
// heartbeat anyway: once it is older than the resync interval it is written even if nothing
// else changed, which is one write and one extra reconcile per interval instead of a
// continuous stream. Without that, a settled object's lastSyncTime would freeze and anyone
// using it to answer "is this still being reconciled" would read it as a dead controller.
//
// prev is the status as read from the API server, next the one about to be written. Both are
// compared with lastSyncTime zeroed, so every other field - conditions included, with their
// own observedGeneration and transition times - still forces a write.
func statusSettled(prev, next any, resyncEvery time.Duration, lastSync *metav1.Time) bool {
	if !sameIgnoringLastSync(prev, next) {
		return false
	}
	// Nothing changed. Write only if the heartbeat has gone stale.
	return lastSync != nil && time.Since(lastSync.Time) < resyncEvery
}

// sameIgnoringLastSync compares two *Status values field for field with LastSyncTime cleared.
// Reflection rather than a hand-written comparison per type: a field added to either status
// would otherwise be left out of the comparison silently, and the symptom - a status that
// stops updating for one particular field - is nothing like the cause.
func sameIgnoringLastSync(prev, next any) bool {
	a, b := zeroLastSync(prev), zeroLastSync(next)
	return reflect.DeepEqual(a, b)
}

func zeroLastSync(v any) any {
	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Pointer || rv.IsNil() {
		return v
	}
	cp := reflect.New(rv.Elem().Type())
	cp.Elem().Set(rv.Elem())
	if f := cp.Elem().FieldByName("LastSyncTime"); f.IsValid() && f.CanSet() {
		f.Set(reflect.Zero(f.Type()))
	}
	return cp.Elem().Interface()
}
