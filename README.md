> gmq是一个轻量级的消息中间件,简单高效是它的特点

## 安装运行
```bash
git clone https://github.com/wuzhc/gmq.git $GOPATH/src/github.com/wuzhc/gmq
cd $GOPATH/src/github.com/wuzhc/gmq
make

# 启动注册中心服务,直接指定配置文件
gregister -config_file="conf.ini"
# 或者通过命令行参数指定配置
gregister -http_addr="127.0.0.1:9595"
# 启动节点服务,直接指定配置文件
gnode -config_file="conf.ini"
# 或者通过命令行参数指定配置
gnode -http_addr="127.0.0.1:9504" -tcp_addr="127.0.0.1:9503" -register_addr="http://127.0.0.1:9595" -node_id=1 -node_weight=1 
```
### 配置文件`conf.ini`,参考
- gnode配置文件 https://github.com/wuzhc/gmq/blob/gmq-dev-v3/cmd/gnode/conf.ini
- gregister配置文件 https://github.com/wuzhc/gmq/blob/gmq-dev-v3/cmd/gregister/conf.ini

### 命令行选项参数说明:  
- -http_addr 节点http地址(ip:port)
- -tcp_addr 节点tcp地址(ip:port)
- -register_addr 注册中心http地址(http://ip:port)
- -node_id 节点ID,唯一值,范围为1到1024
- -node_weight 节点权重,用于多节点场景
### 注意:  
- 先启动注册中心`gregister`,再启动节点`gnode`,因为每个节点启动时候需要把节点基本信息上报给注册中心

## 测试
启动注册中心和节点之后,便可以开始消息推送和消费了,打开终端,执行如下命令
```bash
# 推送消息
curl -d 'data={"body":"this is a job","topic":"game_1","delay":20}' 'http://127.0.0.1:9504/push'
# 消费消息
curl http://127.0.0.1:9504/pop?topic=xxx 
# 确认消息
curl http://127.0.0.1:9504/ack?msgId=xxx&topic=xxx
```

## 客户端
gmq提供了一个golang版本的客户端和一个web管理系统,链接如下:
- [gmq-client客户端](https://github.com/wuzhc/gmq-client-go)
- [gmq-web管理](https://github.com/wuzhc/gmq-web)

## docker运行
除上面之后,还可以直接运行gmq的容器
```bash
# 启动注册中心
docker run --name=gmq-register -p 9595:9595 --network=gmq-network  wuzhc/gmq-image:v1 gregister -http_addr=":9595"

# 启动节点
docker run --name=gmq-node -p 9503:9503 -p 9504:9504 --network=gmq-network  wuzhc/gmq-image:v1 gnode -node_id=1 -tcp_addr=gnode:9503 -http_addr=gnode:9504 -register_addr=http://gmq-register:9595

# 启动web管理
docker run --name=gmq-web -p 8080:8080 --network=gmq-network wuzhc/gmq-image:v1 gweb -web_addr=":8080" -register_addr="http://gmq-register:9595"

# 启动客户端
docker run --name=gmq-client --network=gmq-network wuzhc/gmq-image:v1 
docker exec gmq-client gclient -node_addr="gmq-node:9503" -cmd="push" -topic="gmq-topic-1" -push_num=1000
docker exec gmq-client gclient -node_addr="gmq-node:9503" -cmd="pop_loop" -topic="gmq-topic-1" 
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
- [gmq快速入门]()
- [gmq整体设计]()
- [gmq传输协议]()
- [gmq注意问题]()
- [redis连接池]()
- [延迟消息设计]()
- [分布式ID生成]()
- [goroutine安全退出]()
- [redis+lua保证原子性操作]()
