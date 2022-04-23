

all:
	go install golang.org/x/tools/...@latest
	go generate
	go test
	golangci-lint run

coverageO:
	go install github.com/eltorocorp/drygopher/drygopher@latest
	-drygopher -s 80
	go tool cover -html=coverage.out

calculate_coverage:
	echo "mode: atomic" > coverage.txt
	for d in $$(go list ./...); do \
	  go test -race -covermode=atomic -coverprofile=profile.out -coverpkg=github.com/muir/libschema/... $$d; \
	  if [ -f profile.out ]; then \
	    grep -v ^mode profile.out >> coverage.txt; \
	    rm profile.out; \
	  fi; \
	done

coverage: calculate_coverage
	go tool cover -html=coverage.txt

golanglint:
	# binary will be $(go env GOPATH)/bin/golangci-lint
	curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $$(go env GOPATH)/bin v1.45.2
	golangci-lint --version
