package dcron

import (
	"sync"
	"github.com/bencen/dcron/consistenthash"
	"github.com/bencen/dcron/driver"
	"time"
)
const defaultReplicas = 50
const defaultDuration  = 10
type NodePool struct {
	serverName string
	NodeId string

	mu    sync.Mutex
	nodes *consistenthash.Map

	Driver driver.Driver
	opts  PoolOptions
}
type PoolOptions struct {
	Replicas int
	HashFn consistenthash.Hash
}


func newNodePool(serverName,driverName string, dataSourceOption driver.DriverConnOpt) *NodePool{

	nodePool := new(NodePool)
	nodePool.Driver = driver.GetDriver(driverName)
	nodePool.Driver.Open(dataSourceOption)


	nodePool.serverName = serverName

	option := PoolOptions{
		Replicas:defaultReplicas,
	}
	nodePool.opts = option

	nodePool.initPool()

	go nodePool.tickerUpdatePool()

	return nodePool
}


func (this *NodePool)initPool(){
	this.Driver.SetTimeout(defaultDuration*time.Second)
	this.NodeId = this.Driver.RegisterNode(this.serverName)

	this.Driver.SetHeartBeat(this.NodeId)

	this.updatePool()
}

func (this *NodePool)updatePool(){
	this.mu.Lock()
	defer this.mu.Unlock()
	nodes := this.Driver.GetNodeList(this.serverName)
	this.nodes = consistenthash.New(this.opts.Replicas, this.opts.HashFn)
	for _, node := range nodes {
		this.nodes.Add(node)
	}
}
func(this *NodePool)tickerUpdatePool(){
	tickers := time.NewTicker(time.Second * defaultDuration)
	for range tickers.C {
		this.updatePool()
	}
}

//使用一致性hash算法根据任务名获取一个执行节点
func (this *NodePool) PickNodeByJobName(jobName string) string {
	this.mu.Lock()
	defer this.mu.Unlock()
	if this.nodes.IsEmpty() {
		return ""
	}
	return this.nodes.Get(jobName)
}

