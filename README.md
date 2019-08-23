> gmq基于golang和redis开发的轻量级消息队列

## 功能和特性
- 支持分布式部署
- 支持延迟消息
- 消息确认删除机制
- 长连接方式
- 支持安全传输tls
- json,gob两种序列化方式
- 提供http api接口

## 注意
- 分布式情况下不保证有序性,需要单机情况下且限制一个goroutine来消费消息,消息才能保证有序性

## 部署
## 源码运行
```bash
git clone https://github.com/wuzhc/gmq.git
cd $GOPATH/src/gmq
go get -u -v github.com/kardianos/govendor # 如果有就不需要安装了
govendor sync

cd $GOPATH/src/gmq/cmd/register
go run main.go # 需要先启动注册中心

cd $GOPATH/src/gmq/cmd/gnode
go run main.go # 启动节点,启动成功后,节点会向注册中心注册自己
```

## 工作原理
![https://github.com/wuzhc/zcnote/raw/master/images/project/gmq%E6%B5%81%E7%A8%8B%E5%9B%BE.png](https://github.com/wuzhc/zcnote/raw/master/images/project/gmq%E6%B5%81%E7%A8%8B%E5%9B%BE.png)  

## 传输协议
### 请求数据包
gmq使用了固定包头+包体的形式来解决粘包问题,一个数据包如下:
```
  xxxx   |    xx    |    xx    |    ...   |    ...   |
   包头      命令长度     数据长度     命令        数据
  4-bytes    2-bytes      2-bytes    n-bytes     n-bytes
```
### 响应数据包
```
  xx    |   xx    |   ...  |
响应类型    数据长度    数据
 2-bytes   2-bytes   n-bytes
```
更多详情参考[gmq传输协议](gmq传输协议)

## 相关文章
- gmq设计流程
- gmq传输协议
- 等待各个子goroutine安全退出
- redis+lua保证操作原子性
- 定时器设计
- redis连接池,减少重复连接开销