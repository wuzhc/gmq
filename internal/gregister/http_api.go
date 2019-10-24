package gregister

import (
	"errors"
	"fmt"

	"github.com/wuzhc/gmq/pkg/utils"
)

type HttpApi struct {
	ctx *Context
}

// 注册节点
// curl http://127.0.0.1:9595/register?addr=127.0.0.1:9503&weight=1
func (h *HttpApi) Register(c *HttpServContext) {
	h.ctx.Gregister.mux.RLock()
	defer h.ctx.Gregister.mux.RUnlock()

	tcpAddr := c.Get("tcp_addr")
	httpAddr := c.Get("http_addr")
	weight := c.GetInt("weight")
	nodeId := c.GetInt64("node_id")

	for _, n := range h.ctx.Gregister.nodes {
		if n.Id == nodeId {
			c.JsonErr(errors.New(fmt.Sprintf("node.Id %d has exist", nodeId)))
			return
		}
		if n.TcpAddr == tcpAddr {
			c.JsonErr(errors.New(fmt.Sprintf("node.tcpAddr %s has exist", tcpAddr)))
			return
		}
		if n.HttpAddr == httpAddr {
			c.JsonErr(errors.New(fmt.Sprintf("node.httpAddr %s has exist", httpAddr)))
			return
		}
	}

	node := node{
		Id:       nodeId,
		TcpAddr:  tcpAddr,
		HttpAddr: httpAddr,
		Weight:   weight,
		JoinTime: utils.CurDatetime(),
	}
	h.ctx.Gregister.nodes = append(h.ctx.Gregister.nodes, node)

	c.JsonSuccess("success")
	return
}

// 注销节点
// curl http://127.0.0.1:9595/unregister?addr=127.0.0.1:9503
func (h *HttpApi) Unregister(c *HttpServContext) {
	h.ctx.Gregister.mux.RLock()
	defer h.ctx.Gregister.mux.RUnlock()

	index := -1
	tcp_addr := c.Get("tcp_addr")
	for k, n := range h.ctx.Gregister.nodes {
		if n.TcpAddr == tcp_addr {
			index = k
			break
		}
	}

	if index <= -1 {
		c.JsonErr(errors.New("addr is not exist"))
		return
	}
	if index == 0 {
		h.ctx.Gregister.nodes = h.ctx.Gregister.nodes[1:]
		c.JsonSuccess("success")
		return
	}

	h.ctx.Gregister.nodes = append(h.ctx.Gregister.nodes[0:index], h.ctx.Gregister.nodes[index+1:]...)
	c.JsonSuccess("success")
	return
}

// 获取节点列表
// curl http://127.0.0.1:9595/getNodes
func (h *HttpApi) GetNodes(c *HttpServContext) {
	c.JsonData(map[string][]node{
		"nodes": h.ctx.Gregister.nodes,
	})
	return
}
