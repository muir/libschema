
# dgorder - dependency graph order

[![GoDoc](https://godoc.org/github.com/muir/libschema/dgorder?status.png)](https://pkg.go.dev/github.com/muir/libschema/dgorder)

Install:

	go get github.com/muir/libschema

---

## Dependency graph order

Dgorder provides a simple dependency graph ordering function.

The big design decision with such a library is to choose an API.  The API
for dgorder is an array of nodes that express dependencies on other nodes
based on the node index position in the array.  

While this API is never going to match the caller's data structures, it should
be easy to transform data to this format.

```go
	import "github.com/muir/libschema/dgorder"

	nodes := make([]dgorder.Node, len(myThings))
	for i, thing := range myThings {
		for _, blockedThing := range myThings.BlockedByMything {
			nodes[i].Blocking = append(nodes[i].Blocking, getIndex(blockedThing))
		}
		for _, blockingThing := range myThings.ThingsBlockingMyThing {
			j := getIndex(blockingThing)
			nodes[j].Blocking = append(nodes[j].Blocking, i)
		}
	}
	ordering, err := dgorder.Order(nodes, func(i int) string {
		return descriptionOfMyThingByIndex(i)
	})
	if err != nil {
		panic("Dependency cycle! " + err.Error())
	}
	myThingsInOrder := make([]MyThingType, len(myThings))
	for i, e := range ordering {
		myThingsInOrder[i] = myThings[e]
	}
```
