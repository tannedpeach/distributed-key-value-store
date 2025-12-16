package paxos

import (
	"coms4113/hw5/pkg/base"
)

const (
	Propose = "propose"
	Accept  = "accept"
	Decide  = "decide"
)

type Proposer struct {
	N             int
	Phase         string
	N_a_max       int
	V             interface{}
	SuccessCount  int
	ResponseCount int
	// To indicate if response from peer is received, should be initialized as []bool of len(server.peers)
	Responses []bool
	// Use this field to check if a message is latest.
	SessionId int

	// in case node will propose again - restore initial value
	InitialValue interface{}
}

type ServerAttribute struct {
	peers []base.Address
	me    int

	// Paxos parameter
	n_p int
	n_a int
	v_a interface{}

	// final result
	agreedValue interface{}

	// Propose parameter
	proposer Proposer

	// retry
	timeout *TimeoutTimer
}

type Server struct {
	base.CoreNode
	ServerAttribute
}

func NewServer(peers []base.Address, me int, proposedValue interface{}) *Server {
	response := make([]bool, len(peers))
	return &Server{
		CoreNode: base.CoreNode{},
		ServerAttribute: ServerAttribute{
			peers: peers,
			me:    me,
			proposer: Proposer{
				InitialValue: proposedValue,
				Responses:    response,
			},
			timeout: &TimeoutTimer{},
		},
	}
}

func (server *Server) MessageHandler(message base.Message) []base.Node {
	switch msg := message.(type) {
	case *ProposeRequest:
		return server.handleProposeRequest(msg)
	case *ProposeResponse:
		return server.handleProposeResponse(msg)
	case *AcceptRequest:
		return server.handleAcceptRequest(msg)
	case *AcceptResponse:
		return server.handleAcceptResponse(msg)
	case *DecideRequest:
		return server.handleDecideRequest(msg)
	default:
		return []base.Node{server}
	}
}

func (server *Server) handleProposeRequest(msg *ProposeRequest) []base.Node {
	newServer := server.copy()

	// Check if this is a newer proposal
	if msg.N > newServer.n_p {
		newServer.n_p = msg.N
		response := &ProposeResponse{
			CoreMessage: base.MakeCoreMessage(server.Address(), msg.From()),
			Ok:          true,
			N_p:         msg.N,
			N_a:         newServer.n_a,
			V_a:         newServer.v_a,
			SessionId:   msg.SessionId,
		}
		newServer.SetSingleResponse(response)
	} else {
		// Reject older proposal
		response := &ProposeResponse{
			CoreMessage: base.MakeCoreMessage(server.Address(), msg.From()),
			Ok:          false,
			N_p:         newServer.n_p,
			SessionId:   msg.SessionId,
		}
		newServer.SetSingleResponse(response)
	}

	return []base.Node{newServer}
}

func (server *Server) handleProposeResponse(msg *ProposeResponse) []base.Node {
	// Only handle responses for current session
	if msg.SessionId != server.proposer.SessionId {
		return []base.Node{server}
	}

	// Only handle if we're in Propose phase
	if server.proposer.Phase != Propose {
		return []base.Node{server}
	}

	newNodes := make([]base.Node, 0, 2)

	if msg.Ok {
		// Find peer index
		peerIndex := -1
		for i, peer := range server.peers {
			if peer == msg.From() {
				peerIndex = i
				break
			}
		}
		if peerIndex == -1 {
			return []base.Node{server}
		}

		// Check if we already processed this response
		if server.proposer.Responses[peerIndex] {
			return []base.Node{server}
		}

		// Update N_a_max and V if needed
		newServer := server.copy()
		newServer.proposer.Responses[peerIndex] = true
		newServer.proposer.ResponseCount++

		if msg.N_a > newServer.proposer.N_a_max {
			newServer.proposer.N_a_max = msg.N_a
			newServer.proposer.V = msg.V_a
		}

		newServer.proposer.SuccessCount++

		majority := len(server.peers)/2 + 1

		if newServer.proposer.SuccessCount < majority {
			// Not enough responses yet - only wait
			newNodes = append(newNodes, newServer)
		} else if newServer.proposer.SuccessCount == majority {
			// Exactly majority - two options: proceed or wait
			// Option 1: Proceed to Accept phase
			acceptServer := newServer.copy()
			acceptServer.proposer.Phase = Accept
			acceptServer.proposer.SuccessCount = 0
			acceptServer.proposer.ResponseCount = 0
			for i := range acceptServer.proposer.Responses {
				acceptServer.proposer.Responses[i] = false
			}

			// Send AcceptRequest to all peers
			acceptMessages := make([]base.Message, 0, len(server.peers))
			for _, peer := range server.peers {
				acceptMsg := &AcceptRequest{
					CoreMessage: base.MakeCoreMessage(server.Address(), peer),
					N:           acceptServer.proposer.N,
					V:           acceptServer.proposer.V,
					SessionId:   acceptServer.proposer.SessionId,
				}
				acceptMessages = append(acceptMessages, acceptMsg)
			}
			acceptServer.SetResponse(acceptMessages)
			newNodes = append(newNodes, acceptServer)

			// Option 2: Continue waiting
			newNodes = append(newNodes, newServer)
		} else {
			// More than majority - can still wait for more responses
			newNodes = append(newNodes, newServer)
		}
	} else {
		// Rejected - just track the response
		peerIndex := -1
		for i, peer := range server.peers {
			if peer == msg.From() {
				peerIndex = i
				break
			}
		}
		if peerIndex != -1 && !server.proposer.Responses[peerIndex] {
			newServer := server.copy()
			newServer.proposer.Responses[peerIndex] = true
			newServer.proposer.ResponseCount++
			newNodes = append(newNodes, newServer)
		} else {
			newNodes = append(newNodes, server)
		}
	}

	if len(newNodes) == 0 {
		return []base.Node{server}
	}
	return newNodes
}

func (server *Server) handleAcceptRequest(msg *AcceptRequest) []base.Node {
	newServer := server.copy()

	// Check if this is a newer or equal proposal
	if msg.N >= newServer.n_p {
		newServer.n_p = msg.N
		newServer.n_a = msg.N
		newServer.v_a = msg.V
		response := &AcceptResponse{
			CoreMessage: base.MakeCoreMessage(server.Address(), msg.From()),
			Ok:          true,
			N_p:         msg.N,
			SessionId:   msg.SessionId,
		}
		newServer.SetSingleResponse(response)
	} else {
		// Reject older proposal
		response := &AcceptResponse{
			CoreMessage: base.MakeCoreMessage(server.Address(), msg.From()),
			Ok:          false,
			N_p:         newServer.n_p,
			SessionId:   msg.SessionId,
		}
		newServer.SetSingleResponse(response)
	}

	return []base.Node{newServer}
}

func (server *Server) handleAcceptResponse(msg *AcceptResponse) []base.Node {
	// Only handle responses for current session
	if msg.SessionId != server.proposer.SessionId {
		return []base.Node{server}
	}

	// Only handle if we're in Accept phase
	if server.proposer.Phase != Accept {
		return []base.Node{server}
	}

	newNodes := make([]base.Node, 0, 2)

	if msg.Ok {
		// Find peer index
		peerIndex := -1
		for i, peer := range server.peers {
			if peer == msg.From() {
				peerIndex = i
				break
			}
		}
		if peerIndex == -1 {
			return []base.Node{server}
		}

		// Check if we already processed this response
		if server.proposer.Responses[peerIndex] {
			return []base.Node{server}
		}

		newServer := server.copy()
		newServer.proposer.Responses[peerIndex] = true
		newServer.proposer.ResponseCount++
		newServer.proposer.SuccessCount++

		majority := len(server.peers)/2 + 1

		if newServer.proposer.SuccessCount < majority {
			// Not enough responses yet - only wait
			newNodes = append(newNodes, newServer)
		} else if newServer.proposer.SuccessCount == majority {
			// Exactly majority - two options: proceed or wait
			// Option 1: Proceed to Decide phase
			decideServer := newServer.copy()
			decideServer.proposer.Phase = Decide
			decideServer.proposer.SuccessCount = 0
			decideServer.proposer.ResponseCount = 0
			for i := range decideServer.proposer.Responses {
				decideServer.proposer.Responses[i] = false
			}

			// Send DecideRequest to all peers
			decideMessages := make([]base.Message, 0, len(server.peers))
			for _, peer := range server.peers {
				decideMsg := &DecideRequest{
					CoreMessage: base.MakeCoreMessage(server.Address(), peer),
					V:           decideServer.proposer.V,
					SessionId:   decideServer.proposer.SessionId,
				}
				decideMessages = append(decideMessages, decideMsg)
			}
			decideServer.SetResponse(decideMessages)
			newNodes = append(newNodes, decideServer)

			// Option 2: Continue waiting
			newNodes = append(newNodes, newServer)
		} else {
			// More than majority - can still wait for more responses
			newNodes = append(newNodes, newServer)
		}
	} else {
		// Rejected - just track the response
		peerIndex := -1
		for i, peer := range server.peers {
			if peer == msg.From() {
				peerIndex = i
				break
			}
		}
		if peerIndex != -1 && !server.proposer.Responses[peerIndex] {
			newServer := server.copy()
			newServer.proposer.Responses[peerIndex] = true
			newServer.proposer.ResponseCount++
			newNodes = append(newNodes, newServer)
		} else {
			newNodes = append(newNodes, server)
		}
	}

	if len(newNodes) == 0 {
		return []base.Node{server}
	}
	return newNodes
}

func (server *Server) handleDecideRequest(msg *DecideRequest) []base.Node {
	newServer := server.copy()
	newServer.agreedValue = msg.V
	return []base.Node{newServer}
}

// To start a new round of Paxos.
func (server *Server) StartPropose() {
	// Don't start if already decided
	if server.agreedValue != nil {
		return
	}

	// Increment proposal number
	server.proposer.N++
	server.proposer.Phase = Propose

	// Reset proposer fields
	server.proposer.SuccessCount = 0
	server.proposer.ResponseCount = 0
	for i := range server.proposer.Responses {
		server.proposer.Responses[i] = false
	}

	// Increment session ID
	server.proposer.SessionId++

	// Reset N_a_max and V
	server.proposer.N_a_max = 0
	server.proposer.V = server.proposer.InitialValue

	// Send ProposeRequest to all peers (including itself)
	messages := make([]base.Message, 0, len(server.peers))
	for _, peer := range server.peers {
		proposeMsg := &ProposeRequest{
			CoreMessage: base.MakeCoreMessage(server.Address(), peer),
			N:           server.proposer.N,
			SessionId:   server.proposer.SessionId,
		}
		messages = append(messages, proposeMsg)
	}
	server.SetResponse(messages)
}

// Returns a deep copy of server node
func (server *Server) copy() *Server {
	response := make([]bool, len(server.peers))
	for i, flag := range server.proposer.Responses {
		response[i] = flag
	}

	var copyServer Server
	copyServer.me = server.me
	// shallow copy is enough, assuming it won't change
	copyServer.peers = server.peers
	copyServer.n_a = server.n_a
	copyServer.n_p = server.n_p
	copyServer.v_a = server.v_a
	copyServer.agreedValue = server.agreedValue
	copyServer.proposer = Proposer{
		N:             server.proposer.N,
		Phase:         server.proposer.Phase,
		N_a_max:       server.proposer.N_a_max,
		V:             server.proposer.V,
		SuccessCount:  server.proposer.SuccessCount,
		ResponseCount: server.proposer.ResponseCount,
		Responses:     response,
		InitialValue:  server.proposer.InitialValue,
		SessionId:     server.proposer.SessionId,
	}

	// doesn't matter, timeout timer is state-less
	copyServer.timeout = server.timeout

	return &copyServer
}

func (server *Server) NextTimer() base.Timer {
	return server.timeout
}

// A TimeoutTimer tick simulates the situation where a proposal procedure times out.
// It will close the current Paxos round and start a new one if no consensus reached so far,
// i.e. the server after timer tick will reset and restart from the first phase if Paxos not decided.
// The timer will not be activated if an agreed value is set.
func (server *Server) TriggerTimer() []base.Node {
	if server.timeout == nil {
		return nil
	}

	subNode := server.copy()
	subNode.StartPropose()

	return []base.Node{subNode}
}

func (server *Server) Attribute() interface{} {
	return server.ServerAttribute
}

func (server *Server) Copy() base.Node {
	return server.copy()
}

func (server *Server) Hash() uint64 {
	return base.Hash("paxos", server.ServerAttribute)
}

func (server *Server) Equals(other base.Node) bool {
	otherServer, ok := other.(*Server)

	if !ok || server.me != otherServer.me ||
		server.n_p != otherServer.n_p || server.n_a != otherServer.n_a || server.v_a != otherServer.v_a ||
		(server.timeout == nil) != (otherServer.timeout == nil) {
		return false
	}

	if server.proposer.N != otherServer.proposer.N || server.proposer.V != otherServer.proposer.V ||
		server.proposer.N_a_max != otherServer.proposer.N_a_max || server.proposer.Phase != otherServer.proposer.Phase ||
		server.proposer.InitialValue != otherServer.proposer.InitialValue ||
		server.proposer.SuccessCount != otherServer.proposer.SuccessCount ||
		server.proposer.ResponseCount != otherServer.proposer.ResponseCount {
		return false
	}

	for i, response := range server.proposer.Responses {
		if response != otherServer.proposer.Responses[i] {
			return false
		}
	}

	return true
}

func (server *Server) Address() base.Address {
	return server.peers[server.me]
}
