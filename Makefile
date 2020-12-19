lab1:
	cd src/main && go build -buildmode=plugin ../mrapps/wc.go && go run mrsequential.go wc.so pg*.txt

master:
	cd src/main && go run mrmaster.go pg-*.txt

worker:
	cd src/main && go run mrworker.go wc.so
