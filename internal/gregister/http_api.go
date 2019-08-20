package gregister

import (
	"errors"

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

	tcp_addr := c.Get("tcp_addr")
	http_addr := c.Get("http_addr")
	weight := c.GetInt("weight")

	for _, n := range h.ctx.Gregister.nodes {
		if n.TcpAddr == tcp_addr {
			c.JsonErr(errors.New("has register"))
			return
		}
	}

	node := node{
		TcpAddr:  tcp_addr,
		HttpAddr: http_addr,
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

	h.ctx.Gregister.nodes = append(h.ctx.Gregister.nodes[0:index], h.ctx.Gregister.nodes[index:]...)
	c.JsonSuccess("success")
	return
}

func (h *HttpApi) GetNodes(c *HttpServContext) {
	c.JsonData(map[string][]node{
		"nodes": h.ctx.Gregister.nodes,
	})
	return
}
