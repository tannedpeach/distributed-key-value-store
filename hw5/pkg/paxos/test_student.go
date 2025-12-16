package paxos

import (
	"coms4113/hw5/pkg/base"
	"fmt"
)

// Helper function to wrap predicates with logging
func logPredicate(name string, pred func(s *base.State) bool) func(s *base.State) bool {
	return func(s *base.State) bool {
		result := pred(s)
		if result {
			fmt.Printf("... [PREDICATE MATCHED] %s\n", name)
		}
		return result
	}
}

// Fill in the function to lead the program to a state where A2 rejects the Accept Request of P1
func ToA2RejectP1() []func(s *base.State) bool {
	// P1 should start proposing
	p1ProposePhase := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		return s1.proposer.Phase == Propose
	}

	// P3 should propose with higher N than P1
	p3ProposePhase := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		s3 := s.Nodes()["s3"].(*Server)
		return s3.proposer.Phase == Propose && s3.proposer.N > s1.proposer.N
	}

	// P1 should enter Accept phase
	p1AcceptPhase := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		return s1.proposer.Phase == Accept
	}

	return []func(s *base.State) bool{p1ProposePhase, p3ProposePhase, p1AcceptPhase}
}

// Fill in the function to lead the program to a state where a consensus is reached in Server 3.
func ToConsensusCase5() []func(s *base.State) bool {
	// P3 should start proposing with value v3
	p3ProposePhase := func(s *base.State) bool {
		s3 := s.Nodes()["s3"].(*Server)
		return s3.proposer.Phase == Propose && s3.proposer.V == "v3"
	}

	// P3 should enter Accept phase with v3
	p3AcceptPhase := func(s *base.State) bool {
		s3 := s.Nodes()["s3"].(*Server)
		return s3.proposer.Phase == Accept && s3.proposer.V == "v3"
	}

	// P3 should enter Decide phase
	p3DecidePhase := func(s *base.State) bool {
		s3 := s.Nodes()["s3"].(*Server)
		return s3.proposer.Phase == Decide && s3.proposer.V == "v3"
	}

	return []func(s *base.State) bool{p3ProposePhase, p3AcceptPhase, p3DecidePhase}
}

// Fill in the function to lead the program to a state where all the Accept Requests of P1 are rejected
func NotTerminate1() []func(s *base.State) bool {
	// P1 should start proposing
	p1ProposePhase := logPredicate("NotTerminate1: P1 proposes", func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		return s1.proposer.Phase == Propose
	})

	// P1 should enter Accept phase
	p1AcceptPhase := logPredicate("NotTerminate1: P1 enters Accept", func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		return s1.proposer.Phase == Accept
	})

	// P3 should propose with higher N than P1
	p3ProposeHigherN := logPredicate("NotTerminate1: P3 proposes with higher N", func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		s3 := s.Nodes()["s3"].(*Server)
		return s3.proposer.Phase == Propose && s3.proposer.N > s1.proposer.N
	})

	// P3's ProposeRequests should be in network (sent to all acceptors)
	p3ProposeRequestsSent := logPredicate("NotTerminate1: P3 ProposeRequests sent", func(s *base.State) bool {
		s3 := s.Nodes()["s3"].(*Server)
		p3N := s3.proposer.N
		if p3N == 0 {
			return false
		}
		// Check that ProposeRequests from P3 are in network
		count := 0
		for _, msg := range s.Network {
			if req, ok := msg.(*ProposeRequest); ok && req.From() == "s3" && req.N == p3N {
				count++
			}
		}
		return count >= 2 // At least 2 sent (to s1 and s2)
	})

	// All acceptors should have processed P3's proposal (n_p >= P3's N) so they will reject P1
	allAcceptorsProcessedP3 := logPredicate("NotTerminate1: All acceptors processed P3", func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		s2 := s.Nodes()["s2"].(*Server)
		s3 := s.Nodes()["s3"].(*Server)
		p3N := s3.proposer.N
		if p3N == 0 {
			return false
		}
		// All acceptors must have n_p >= P3's N (so they will reject P1's Accept with N = p1N)
		return s1.n_p >= p3N && s2.n_p >= p3N && s3.n_p >= p3N
	})

	// P1's AcceptRequests should be in network (sent to all acceptors)
	p1AcceptRequestsSent := logPredicate("NotTerminate1: P1 AcceptRequests sent", func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		if s1.proposer.Phase != Accept {
			return false
		}
		p1N := s1.proposer.N
		if p1N == 0 {
			return false
		}
		count := 0
		for _, msg := range s.Network {
			if req, ok := msg.(*AcceptRequest); ok && req.From() == "s1" && req.N == p1N {
				count++
			}
		}
		return count >= 2 // At least 2 sent
	})

	// At least one reject response received by P1
	oneRejectReceived := logPredicate("NotTerminate1: One reject received", func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		if s1.proposer.Phase != Accept {
			return false
		}
		return s1.proposer.ResponseCount >= 1 && s1.proposer.SuccessCount == 0
	})

	// At least two reject responses received by P1
	twoRejectsReceived := logPredicate("NotTerminate1: Two rejects received", func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		if s1.proposer.Phase != Accept {
			return false
		}
		return s1.proposer.ResponseCount >= 2 && s1.proposer.SuccessCount == 0
	})

	return []func(s *base.State) bool{p1ProposePhase, p1AcceptPhase, p3ProposeHigherN, p3ProposeRequestsSent, allAcceptorsProcessedP3, p1AcceptRequestsSent, oneRejectReceived, twoRejectsReceived}
}

// Fill in the function to lead the program to a state where all the Accept Requests of P3 are rejected
func NotTerminate2() []func(s *base.State) bool {
	// P1 should propose again with higher N than P3 (after timeout from previous rejection)
	p1ProposeHigherN := logPredicate("NotTerminate2: P1 proposes again with higher N", func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		s3 := s.Nodes()["s3"].(*Server)
		return s1.proposer.Phase == Propose && s1.proposer.N > s3.proposer.N
	})

	// P1's ProposeRequests should be in network
	p1ProposeRequestsSent := logPredicate("NotTerminate2: P1 ProposeRequests sent", func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		p1N := s1.proposer.N
		if p1N == 0 {
			return false
		}
		count := 0
		for _, msg := range s.Network {
			if req, ok := msg.(*ProposeRequest); ok && req.From() == "s1" && req.N == p1N {
				count++
			}
		}
		return count >= 2 // At least 2 sent
	})

	// All acceptors should have processed P1's new proposal (n_p >= P1's N) so they will reject P3
	allAcceptorsProcessedP1 := logPredicate("NotTerminate2: All acceptors processed P1", func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		s2 := s.Nodes()["s2"].(*Server)
		s3 := s.Nodes()["s3"].(*Server)
		p1N := s1.proposer.N
		p3N := s3.proposer.N
		if p1N <= p3N {
			return false
		}
		// All acceptors must have n_p >= P1's N (so they will reject P3's Accept with N = p3N)
		return s1.n_p >= p1N && s2.n_p >= p1N && s3.n_p >= p1N
	})

	// P3 should enter Accept phase
	p3AcceptPhase := logPredicate("NotTerminate2: P3 enters Accept", func(s *base.State) bool {
		s3 := s.Nodes()["s3"].(*Server)
		return s3.proposer.Phase == Accept
	})

	// P3's AcceptRequests should be in network
	p3AcceptRequestsSent := logPredicate("NotTerminate2: P3 AcceptRequests sent", func(s *base.State) bool {
		s3 := s.Nodes()["s3"].(*Server)
		if s3.proposer.Phase != Accept {
			return false
		}
		p3N := s3.proposer.N
		if p3N == 0 {
			return false
		}
		count := 0
		for _, msg := range s.Network {
			if req, ok := msg.(*AcceptRequest); ok && req.From() == "s3" && req.N == p3N {
				count++
			}
		}
		return count >= 2 // At least 2 sent
	})

	// At least one reject response received by P3
	oneRejectReceivedP3 := logPredicate("NotTerminate2: One reject received by P3", func(s *base.State) bool {
		s3 := s.Nodes()["s3"].(*Server)
		if s3.proposer.Phase != Accept {
			return false
		}
		return s3.proposer.ResponseCount >= 1 && s3.proposer.SuccessCount == 0
	})

	// At least two reject responses received by P3
	twoRejectsReceivedP3 := logPredicate("NotTerminate2: Two rejects received by P3", func(s *base.State) bool {
		s3 := s.Nodes()["s3"].(*Server)
		if s3.proposer.Phase != Accept {
			return false
		}
		return s3.proposer.ResponseCount >= 2 && s3.proposer.SuccessCount == 0
	})

	return []func(s *base.State) bool{p1ProposeHigherN, p1ProposeRequestsSent, allAcceptorsProcessedP1, p3AcceptPhase, p3AcceptRequestsSent, oneRejectReceivedP3, twoRejectsReceivedP3}
}

// Fill in the function to lead the program to a state where all the Accept Requests of P1 are rejected again.
func NotTerminate3() []func(s *base.State) bool {
	// P3 should propose again with higher N than P1
	p3ProposeHigherN := logPredicate("NotTerminate3: P3 proposes again with higher N", func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		s3 := s.Nodes()["s3"].(*Server)
		return s3.proposer.Phase == Propose && s3.proposer.N > s1.proposer.N
	})

	// P3's ProposeRequests should be in network
	p3ProposeRequestsSentAgain := logPredicate("NotTerminate3: P3 ProposeRequests sent again", func(s *base.State) bool {
		s3 := s.Nodes()["s3"].(*Server)
		p3N := s3.proposer.N
		if p3N == 0 {
			return false
		}
		count := 0
		for _, msg := range s.Network {
			if req, ok := msg.(*ProposeRequest); ok && req.From() == "s3" && req.N == p3N {
				count++
			}
		}
		return count >= 2 // At least 2 sent
	})

	// All acceptors should have processed P3's new proposal (n_p >= P3's N) so they will reject P1
	allAcceptorsProcessedP3Again := logPredicate("NotTerminate3: All acceptors processed P3 again", func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		s2 := s.Nodes()["s2"].(*Server)
		s3 := s.Nodes()["s3"].(*Server)
		p1N := s1.proposer.N
		p3N := s3.proposer.N
		if p3N <= p1N {
			return false
		}
		// All acceptors must have n_p >= P3's N (so they will reject P1's Accept with N = p1N)
		return s1.n_p >= p3N && s2.n_p >= p3N && s3.n_p >= p3N
	})

	// P1 should enter Accept phase again
	p1AcceptPhaseAgain := logPredicate("NotTerminate3: P1 enters Accept again", func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		return s1.proposer.Phase == Accept
	})

	// P1's AcceptRequests should be in network again
	p1AcceptRequestsSentAgain := logPredicate("NotTerminate3: P1 AcceptRequests sent again", func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		if s1.proposer.Phase != Accept {
			return false
		}
		p1N := s1.proposer.N
		if p1N == 0 {
			return false
		}
		count := 0
		for _, msg := range s.Network {
			if req, ok := msg.(*AcceptRequest); ok && req.From() == "s1" && req.N == p1N {
				count++
			}
		}
		return count >= 2 // At least 2 sent
	})

	// At least one reject response received by P1 again
	oneRejectReceivedAgain := logPredicate("NotTerminate3: One reject received by P1 again", func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		if s1.proposer.Phase != Accept {
			return false
		}
		return s1.proposer.ResponseCount >= 1 && s1.proposer.SuccessCount == 0
	})

	// At least two reject responses received by P1 again
	twoRejectsReceivedAgain := logPredicate("NotTerminate3: Two rejects received by P1 again", func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		if s1.proposer.Phase != Accept {
			return false
		}
		return s1.proposer.ResponseCount >= 2 && s1.proposer.SuccessCount == 0
	})

	return []func(s *base.State) bool{p3ProposeHigherN, p3ProposeRequestsSentAgain, allAcceptorsProcessedP3Again, p1AcceptPhaseAgain, p1AcceptRequestsSentAgain, oneRejectReceivedAgain, twoRejectsReceivedAgain}
}

// Fill in the function to lead the program to make P1 propose first, then P3 proposes, but P1 get rejects in
// Accept phase
func concurrentProposer1() []func(s *base.State) bool {
	// P1 should start proposing
	p1ProposePhase := logPredicate("ConcurrentProposer1: P1 proposes", func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		return s1.proposer.Phase == Propose
	})

	// P1 should enter Accept phase
	p1AcceptPhase := logPredicate("ConcurrentProposer1: P1 enters Accept", func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		return s1.proposer.Phase == Accept
	})

	// P3 should propose with higher N than P1
	p3ProposeWithHigherN := logPredicate("ConcurrentProposer1: P3 proposes with higher N", func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		s3 := s.Nodes()["s3"].(*Server)
		return s3.proposer.Phase == Propose && s3.proposer.N > s1.proposer.N
	})

	// P3's ProposeRequests should be in network
	p3ProposeRequestsSent := logPredicate("ConcurrentProposer1: P3 ProposeRequests sent", func(s *base.State) bool {
		s3 := s.Nodes()["s3"].(*Server)
		p3N := s3.proposer.N
		if p3N == 0 {
			return false
		}
		count := 0
		for _, msg := range s.Network {
			if req, ok := msg.(*ProposeRequest); ok && req.From() == "s3" && req.N == p3N {
				count++
			}
		}
		return count >= 2 // At least 2 sent
	})

	// All acceptors should have processed P3's proposal (n_p >= P3's N) so they will reject P1
	allAcceptorsProcessedP3 := logPredicate("ConcurrentProposer1: All acceptors processed P3", func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		s2 := s.Nodes()["s2"].(*Server)
		s3 := s.Nodes()["s3"].(*Server)
		p3N := s3.proposer.N
		if p3N == 0 {
			return false
		}
		// All acceptors must have n_p >= P3's N (so they will reject P1's Accept requests)
		return s1.n_p >= p3N && s2.n_p >= p3N && s3.n_p >= p3N
	})

	// P1's AcceptRequests should be in network
	p1AcceptRequestsSent := logPredicate("ConcurrentProposer1: P1 AcceptRequests sent", func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		if s1.proposer.Phase != Accept {
			return false
		}
		p1N := s1.proposer.N
		if p1N == 0 {
			return false
		}
		count := 0
		for _, msg := range s.Network {
			if req, ok := msg.(*AcceptRequest); ok && req.From() == "s1" && req.N == p1N {
				count++
			}
		}
		return count >= 2 // At least 2 sent
	})

	// At least one reject response received by P1
	oneRejectReceived := logPredicate("ConcurrentProposer1: One reject received", func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		if s1.proposer.Phase != Accept {
			return false
		}
		return s1.proposer.ResponseCount >= 1 && s1.proposer.SuccessCount == 0
	})

	// At least two reject responses received by P1
	twoRejectsReceived := logPredicate("ConcurrentProposer1: Two rejects received", func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		if s1.proposer.Phase != Accept {
			return false
		}
		return s1.proposer.ResponseCount >= 2 && s1.proposer.SuccessCount == 0
	})

	return []func(s *base.State) bool{p1ProposePhase, p1AcceptPhase, p3ProposeWithHigherN, p3ProposeRequestsSent, allAcceptorsProcessedP3, p1AcceptRequestsSent, oneRejectReceived, twoRejectsReceived}
}

// Fill in the function to lead the program continue  P3's proposal  and reaches consensus at the value of "v3".
func concurrentProposer2() []func(s *base.State) bool {
	// P3 should enter Accept phase with v3
	p3AcceptPhase := func(s *base.State) bool {
		s3 := s.Nodes()["s3"].(*Server)
		return s3.proposer.Phase == Accept && s3.proposer.V == "v3"
	}

	// P3 should enter Decide phase
	p3DecidePhase := func(s *base.State) bool {
		s3 := s.Nodes()["s3"].(*Server)
		return s3.proposer.Phase == Decide && s3.proposer.V == "v3"
	}

	return []func(s *base.State) bool{p3AcceptPhase, p3DecidePhase}
}
