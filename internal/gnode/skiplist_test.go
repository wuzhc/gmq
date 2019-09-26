package gnode

import (
	"testing"
)

// 测试结果 Benchmark_Insert-4   	  100000	     21091 ns/op	    2239 B/op	       6 allocs/op
func Benchmark_Insert(b *testing.B) {
	b.ReportAllocs()

	s := New(32)
	for i := 0; i < b.N; i++ { //use b.N for looping
		s.Insert(Elem(i), i)
	}
}

func Benchmark_TimeConsumingFunction(b *testing.B) {
	b.StopTimer() //调用该函数停止压力测试的时间计数

	//做一些初始化的工作,例如读取文件数据,数据库连接之类的,
	//这样这些时间不影响我们测试函数本身的性能

	b.StartTimer() //重新开始时间
	for i := 0; i < b.N; i++ {
		Division(4, 5)
	}
}
