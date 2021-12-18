
seq:plugin clean
	go run mrsequential.go wc.so pg*.txt

master:
	go run mrcoordinator.go pg-*.txt

worker:plugin clean
	go run mrworker.go wc.so

plugin:
	go build -buildmode=plugin ../mrapps/wc.go

clean:
	rm -f mr-*
