1. 从向主要任务
2. 主分配未开始的 map 任务.
3. 从读任务,map 它.
4. 直到最后一个 map 结束 才能开始 reduce

## 执行方法

- 编译插件: go build -buildmode=plugin ../mrapps/wc.go
- 启动 master: go run mrcoordinator.go pg-*.txt
- 启动 worker: go run mrworker.go wc.so


