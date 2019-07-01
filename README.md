## 1. 概述
`gmq`是基于`redis`提供的特性,使用`go`语言开发的一个简单易用的队列;关于redis使用特性可以参考之前本人写过一篇很简陋的文章[Redis 实现队列](https://segmentfault.com/a/1190000011084493);
`gmq`的灵感和设计是基于[有赞延迟队列设计](https://tech.youzan.com/queuing_delay/),文章内容清晰而且很好理解,但是没有提供源码链接,在文章的最后也提到了一些未来架构方向,于是也就有了`gmq`
> `gmq`不是简单按照有赞延迟队列设计来实现功能,而是经过自己的思考,在原有的设计的基础上,做了一些修改,文章的最后会有一个和原设计的对比

## 2. gmq流程图如下:
![一个不规范的流程图](https://github.com/wuzhc/zcnote/raw/master/images/project/gmq%E6%B5%81%E7%A8%8B%E5%9B%BE.png)

### 2.1 延迟时间delay
- 当job.delay>0时,job会被分配到bucket中,bucket会有周期性扫描到期job,如果到期,job会被bucket移到`ready queue`,等待被消费
- 当job.delay=0时,job会直接加到`ready queue`,等待被消费

### 2.2 执行超时时间TTR
参考第一个图的流程,当job被消费者读取后,如果`job.TTR>0`,即job设置了执行超时时间,那么job会在读取后会被添加到TTRBucket(专门存放设置了超时时间的job),并且设置`job.delay = job.TTR`,如果在TTR时间内没有得到消费者ack确认然后删除job,job将在TTR时间之后添加到`ready queue`,然后再次被消费(如果消费者在TTR时间之后才请求ack,会得到失败的响应)

### 2.3 确认机制
主要和TTR的设置有关系,确认机制可以分为两种:
- 当job.TTR=0时,消费者`pop`出job时,即会自动删除`job pool`中的job元数据
- 当job.TTR>0时,即job执行超时时间,这个时间是指用户`pop`出job时开始到用户`ack`确认删除结束这段时间,如果在这段时间没有`ACK`,job会被再次加入到`ready queue`,然后再次被消费,只有用户调用了`ACK`,才会去删除`job pool`中job元数据

## 3. web监控
`gmq`提供了一个简单web监控平台,方便查看当前堆积任务数以及运行情况,默认监听端口为`9503`,界面如下:
![](https://github.com/wuzhc/zcnote/raw/master/images/project/gmq%E7%9B%91%E6%8E%A7.png)

## 4. 应用场景
- 延迟任务
    - 延迟任务,例如用户下订单一直处于未支付状态，半个小时候自动关闭订单
- 异步任务
    - 异步任务,一般用于耗时操作,例如群发邮件等批量操作
- 超时任务
    - 规定时间内`(TTR)`没有执行完毕或程序被意外中断,以致于没有反馈ack给gmq,则gmq会在TTR之后将job重新加到队列然后再次被消费,一般用于数据比较敏感,不容丢失的
- 优先级任务
    - 当多个任务同时产生时,按照任务设定等级优先被消费,例如a,b两种类型的job,当有a类型job时优先消费,没有了再消费b类型job
    
## 5. 安装
### 5.1 源码运行
配置文件位于`gmq/conf.ini`,可以根据自己项目需求修改配置
```bash
git clone https://github.com/wuzhc/gmq.git
cd gmq
go get -u -v github.com/kardianos/govendor # 如果有就不需要安装了
govendor sync
go run main.go
# go build # 可编译成可执行文件
```
### 5.2 执行文件运行
```bash
./gmq &
```

## 6. 使用
目前只实现python,go,php语言的客户端的demo
### 6.1 php
```php
// consumer.php 消费者
$client = new JsonRPC("127.0.0.1", 9503);
while (true) {
    $r = $client->Call("Service.Pop", array_slice($topic,rand(0,7)));
    if (!$r) {
        // 是否断开连接了
        print_r($client->getErrors());exit;
    }
    print_r($r);
    // sleep(2);
    // print_r($r);
    echo 'POP ' . $r['result']['id'] . PHP_EOL;
    if (!empty($r['error'])) {
        if ($r['error'] == 'empty') {
            echo '[' . date('Y-m-d H:i:s') . '] no jobs and will sleep 3 seconds' . PHP_EOL;
            echo '已处理' . $n . PHP_EOL;
            // $client->close();
            sleep(3);
            continue;
        }
    }
    $result = $r['result'];
    if ($result['TTR'] > 0) {
        echo 'ACK' . $result['id'] . PHP_EOL;
        $r = $client->Call('Service.Ack', $result['id']);
        // print_r($r);
    }
    $n++;
    echo '已处理' . $n . PHP_EOL;
}
$client->close();
```

```php
// consumer.php 生产者
$client = new JsonRPC("127.0.0.1", 9503);
for ($i = 0; $i < 100000; $i++) {
    $data = [
        'id'    => 'xxxx_id' . microtime(true) . rand(1,999999999),
        'topic' => $topic[rand(0, 7)],
        'body'  => 'this is a rpc test',
        'delay' => (string)rand(0, 1000),
        'TTR'   => '3'
    ];
    $r = $client->Call("Service.Push", $data);
    if ($client->getErrors()) {
        echo $client->getErrors() . PHP_EOL;
        sleep(3);
    } else {
        print_r($r);
        echo $i . ' ' . $r['result'] . '---' . PHP_EOL;
    }
}
$client->close();
```

## 7. 遇到问题
以下是开发遇到的问题,以及一些粗糙的解决方案

### 7.1 安全退出
如果强行中止`gmq`的运行,可能会导致一些数据丢失,例如下面一个例子:  
![gmq之timer定时器](https://github.com/wuzhc/zcnote/blob/master/images/project/gmq%E4%B9%8Btimer%E5%AE%9A%E6%97%B6%E5%99%A8.png)  
如果发生上面的情况,就会出现job不在`bucket`中,也不在`ready queue`,这就出现了job丢失的情况,而且将没有任何机会去删除`job pool`中已丢失的job,长久之后`job pool`可能会堆积很多的已丢失job的元数据;所以安全退出需要在接收到退出信号时,应该等待所有`goroutine`处理完手中的事情,然后再退出

####  7.1.1 `gmq`退出流程
![gmq安全退出.png](https://github.com/wuzhc/zcnote/raw/master/images/project/gmq%E5%AE%89%E5%85%A8%E9%80%80%E5%87%BA.png)  
首先`gmq`通过context传递关闭信号给`dispatcher`,`dispatcher`接收到信号会关闭`dispatcher.closed`,每个`bucket`会收到`close`信号,然后先退出`timer`检索,再退出`bucket`,`dispatcher`等待所有bucket退出后,然后退出

`dispatcher`退出顺序流程: `timer` -> `bucket` -> `dispatcher`

#### 7.1.2 注意
不要使用`kill -9 pid`来强制杀死进程,因为系统无法捕获SIGKILL信号,导致gmq可能执行到一半就被强制中止,应该使用`kill -15 pid`,`kill -1 pid`或`kill -2 pid`,各个数字对应信号如下:
- 9 对应SIGKILL
- 15 对应SIGTERM
- 1 对应SIGHUP
- 2 对应SIGINT
- [https://www.jianshu.com/p/5729fc095b2a](https://www.jianshu.com/p/5729fc095b2a)  
![](https://upload-images.jianshu.io/upload_images/10118224-625e74bf4a2d4204.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/516/format/webp)

### 7.2 智能定时器
- 每一个`bucket`都会维护一个`timer`,不同于有赞设计,`timer`不是每秒轮询一次,而是根据`bucket`下一个job到期时间来设置`timer`的定时时间 ,这样的目的在于如果`bucket`没有job或job到期时间要很久才会发生,就可以减少不必要的轮询;
- `timer`只有处理完一次业务后才会重置定时器;,这样的目的在于可能出现上一个时间周期还没执行完毕,下一个定时事件又发生了
- 如果到期的时间很相近,`timer`就会频繁重置定时器时间,就目前个人使用来说,还没出现什么性能上的问题

### 7.3 原子性问题
我们知道redis的命令是排队执行,在一个复杂的业务中可能会多次执行redis命令,如果在大并发的场景下,这个业务有可能中间插入了其他业务的命令,导致出现各种各样的问题;  
redis保证整个事务原子性和一致性问题一般用`multi/exec`或`lua脚本`,`gmq`在操作涉及复杂业务时使用的是`lua脚本`,因为`lua脚本`除了有`multi/exec`的功能外,还有`Pipepining`功能(主要打包命令,减少和`redis server`通信次数),下面是一个`gmq`定时器扫描bucket集合到期job的lua脚本:

```lua
-- 获取到期的50个job
local jobIds = redis.call('zrangebyscore',KEYS[1], 0, ARGV[4], 'withscores', 'limit', 0, 50)
local res = {}
for k,jobId in ipairs(jobIds) do 
	if k%2~=0 then
		local jobKey = string.format('%s:%s', ARGV[3], jobId)
		local status = redis.call('hget', jobKey, 'status')
		-- 检验job状态
		if tonumber(status) == tonumber(ARGV[1]) or tonumber(status) == tonumber(ARGV[2]) then
			-- 先移除集合中到期的job,然后到期的job返回给timer
			local isDel = redis.call('zrem', KEYS[1], jobId)
			if isDel == 1 then
				table.insert(res, jobId)
			end
		end
	end
end

local nextTime
-- 计算下一个job执行时间,用于设置timer下一个时钟周期
local nextJob = redis.call('zrange', KEYS[1], 0, 0, 'withscores')
if next(nextJob) == nil then
	nextTime = -1
else
	nextTime = tonumber(nextJob[2]) - tonumber(ARGV[4])
	if nextTime < 0 then
		nextTime = 1
	end
end

table.insert(res,1,nextTime)
return res
```

### 7.4 redis连接池
可能一般phper写业务很少会接触到连接池,其实这是由php本身所决定他应用不大,当然在php的扩展`swoole`还是很有用处的  
`gmq`的redis连接池是使用`gomodule/redigo/redis`自带连接池,它带来的好处是限制redis连接数,通过复用redis连接来减少开销,另外可以防止tcp被消耗完,这在生产者大量生成数据时会很有用
```go
// gmq/mq/redis.go
Redis = &RedisDB{
    Pool: &redis.Pool{
        MaxIdle:     30,    // 最大空闲链接
        MaxActive:   10000, // 最大链接
        IdleTimeout: 240 * time.Second, // 空闲链接超时
        Wait:        true, // 当连接池耗尽时,是否阻塞等待
        Dial: func() (redis.Conn, error) {
            c, err := redis.Dial("tcp", "127.0.0.1:6379", redis.DialPassword(""))
            if err != nil {
                return nil, err
            }
            return c, nil
		},
        TestOnBorrow: func(c redis.Conn, t time.Time) error {
            if time.Since(t) < time.Minute {
            	return nil
            }
        	_, err := c.Do("PING")
        	return err
        },
    },
}
```

### 7.5 热更新
程序需要升级,但是又不能直接关闭服务,需要接收客户端的请求;而go不像php一样动态解析代码,go是编译成一个可执行文件,如果需要升级服务需要替换整个执行文件,一般操作如下:
- 多部署几个服务,当服务需要升级时,一个个替换,把替换的服务请求打到其他服务,替换后在接收请求,类似的方式替换掉所有的服务
- fork子进程,当子进程重启服务,父进程退出,子进程缺少父进程会成为孤儿进程,托管到init进程

## 8. 注意问题
- job.id在`job pool`是唯一的,它将作为redis的缓存键;`gmq`不自动为job生成唯一id值是为了用户可以根据自己生成的job.id来追踪job情况,如果job.id是重复的,push时会报重复id的错误
- bucket数量不是越多越好,一般来说,添加到`ready queue`的速度取决与redis性能,而不是bucket数量

## 9. 使用中可能出现的问题
### 9.1 客户端出现大量的TIME_WAIT状态,并且新的连接被拒绝
```bash
netstat -anp | grep 9503 | wc -l
tcp        0      0 10.8.8.188:41482        10.8.8.185:9503         TIME_WAIT   -                   
```
这个是正常现象,由tcp四次挥手可以知道,当接收到LAST_ACK发出的FIN后会处于`TIME_WAIT`状态,主动关闭方(客户端)为了确保被动关闭方(服务端)收到ACK，会等待2MSL时间，这个时间是为了再次发送ACK,例如被动关闭方可能因为接收不到ACK而重传FIN;另外也是为了旧数据过期,不影响到下一个链接,; 如果要避免大量`TIME_WAIT`的连接导致tcp被耗尽;一般方法如下:
- 使用长连接
- 配置文件,网上很多教程,就是让系统尽快的回收`TIME_WAIT`状态的连接
- 使用连接池,当连接池耗尽时,阻塞等待,直到回收再利用

## 10. 相关链接
- [有赞延迟队列设计](https://tech.youzan.com/queuing_delay/)
- [Redis 实现队列](https://segmentfault.com/a/1190000011084493)
