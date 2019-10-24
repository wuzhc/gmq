> gmq是一个轻量级的消息中间件,它具备了消息可靠性,稳定性,扩展性,高效性等特点
> https://www.jianshu.com/p/4b7865dbb19a

## 消息中间件的作用
- 解耦
- 异步
- 削峰

## 功能和特性
- 支持多节点部署
- 支持延迟消息
- 支持消息确认删除机制
- 支持安全传输tls
- 支持消息持久化
- 提供api接口
- 提供根据消息ID追踪消息功能
- 提供web页面管理

## 未来
- 支持发布订阅功能
- 支持自定义失败次数和死信队列
- 优化单个延迟消息推送速度

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

## 客户端
- [golang客户端](https://github.com/wuzhc/gmq-client-go)
- [php客户端](https://github.com/wuzhc/gmq-client-php)
- [php-swoole客户端](https://github.com/wuzhc/gmq-client-swoole)


## 未来
- 目前是简单的推拉模式,之后会增加订阅发布模式

## 相关文章
- [gmq快速入门]()
- [gmq整体设计]()
- [gmq传输协议]()
- [gmq注意问题]()
- [redis连接池]()
- [延迟消息设计]()
- [分布式ID生成]()
- [goroutine安全退出]()
- [redis+lua保证原子性操作]()
