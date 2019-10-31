> gmq是一个轻量级的消息中间件;第一个版本的gmq是基于redis实现,因为功能和存储严重依赖于redis特性,使得之后的优化受到限制,所以在最新的版本不再使用`redis`,完全移除对redis依赖;最新版本的消息存储部分使用文件存储,并使用内存映射的技术,使得gmq更加高效和稳定;
> 对于redis版本的,大家可以参考[gmq-redis](https://github.com/wuzhc/gmq-redis)

## 架构
gmq是一个简单的推拉模型,消息推送和消费速度由客户端自己控制

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

## 相关文章
- [gmq架构设计](https://github.com/wuzhc/zcnote/blob/master/golang/gmq/gmq%E5%BF%AB%E9%80%9F%E5%85%A5%E9%97%A8.md)
- [gmq通信协议](https://github.com/wuzhc/zcnote/blob/master/golang/gmq/gmq%E9%80%9A%E4%BF%A1%E5%8D%8F%E8%AE%AE.md)
- [gmq多节点使用](https://github.com/wuzhc/zcnote/blob/master/golang/gmq/gmq%E5%A4%9A%E8%8A%82%E7%82%B9%E4%BD%BF%E7%94%A8.md)
- [gmq消息持久化](https://github.com/wuzhc/zcnote/blob/master/golang/gmq/gmq%E6%8C%81%E4%B9%85%E5%8C%96%E5%AD%98%E5%82%A8.md) 
- [gmq消息确认机制](https://github.com/wuzhc/zcnote/blob/master/golang/gmq/gmq%E6%B6%88%E6%81%AF%E7%A1%AE%E8%AE%A4%E6%9C%BA%E5%88%B6.md)
- [gmq延迟消息机制](https://github.com/wuzhc/zcnote/blob/master/golang/gmq/gmq%E5%BB%B6%E8%BF%9F%E6%B6%88%E6%81%AF%E6%9C%BA%E5%88%B6.md)
- [gmq队列处理消息过程]()
- [gmq性能分析pprof工具](https://github.com/wuzhc/zcnote/blob/master/golang/gmq/gmq%E6%80%A7%E8%83%BD%E7%9B%91%E6%8E%A7.md)

**相对于其他消息中间件,gmq足够简单,没有复杂而晦涩的概念,当然这也是gmq还没有提供更加丰富功能,gmq现在还是处于开发阶段,欢迎各位提交你们的代码~**