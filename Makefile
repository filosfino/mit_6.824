lab1:
	cd src/main && go build -buildmode=plugin ../mrapps/wc.go && go run mrsequential.go wc.so pg*.txt

master:
	cd src/main && go build -buildmode=plugin ../mrapps/wc.go && go run mrmaster.go pg-*.txt

worker:
	cd src/main && go build -buildmode=plugin ../mrapps/wc.go && go run mrworker.go wc.so

# test:
# 	git clean -fd && cd src/main && sh test-mr.sh

test:
	git clean -fd && cd src/raft && go test -run 2A -race
