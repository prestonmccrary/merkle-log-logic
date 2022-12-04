import unittest
from merkle import MerkleLog

class MerkleLogTests(unittest.TestCase):
    def test_identical_startup(self):
        
        uuids = [1, 2]
        id1, id2 = uuids  
        
        log1 = MerkleLog(id1, uuids)
        log2 = MerkleLog(id2, uuids)
        
        self.assertEqual(log1, log2)
        
    def test_adding_node(self):
        uuids = [1, 2]
        id1, id2 = uuids  
        
        log1 = MerkleLog(id1, uuids)
        log2 = MerkleLog(id2, uuids)
        
        logs = [log1, log2]
        
        for log in logs:
            genesis_node = log._get_genesis_node_hash()
            
            node1_hash = log.add_node(10)
                
          
            self.assertEqual(log.dependencies, {genesis_node: [], node1_hash: (genesis_node, )})

            self.assertEqual(log.dependents, {genesis_node: [node1_hash,]})
            self.assertEqual(log.roots, [node1_hash])
            
            node2_hash = log.add_node(20)
    
            self.assertEqual(log.dependencies, {genesis_node: [], node1_hash: (genesis_node,), node2_hash: (node1_hash,)})
            self.assertEqual(log.dependents, {genesis_node: [node1_hash], node1_hash: [node2_hash]})
            self.assertEqual(log.roots, [node2_hash])
    
    def test_prepare_delta_basic(self):
            uuids = [1, 2]
            id1, id2 = uuids  
            
            log1 = MerkleLog(id1, uuids)
            log2 = MerkleLog(id2, uuids)
            
            logs = [log1, log2]
            
            node1_hash = log1.add_node(10)
            node2_hash = log1.add_node(20)
            node3_hash = log2.add_node(11)
            
            nodes_to_send, roots =  log1.prepare_swap(2)
            expected_node_delta = [node1_hash, node2_hash]
            
            self.assertEquals(set(nodes_to_send.keys()), set(expected_node_delta))
            
            
    
    def test_swap_basic(self):
            
            uuids = [1, 2]
            id1, id2 = uuids  
            
            log1 = MerkleLog(id1, uuids)
            log2 = MerkleLog(id2, uuids)
            
            logs = [log1, log2]
            
            node1_hash = log1.add_node(10)
            node2_hash = log1.add_node(20)
            node3_hash = log2.add_node(11)
            
            nodes_to_send, roots =  log1.prepare_swap(2)
            expected_node_delta = [node1_hash, node2_hash]
            self.assertEquals(set(nodes_to_send.keys()), set(expected_node_delta))
            
            nodes_to_send2, roots2, on_deliver = log2.respond_to_swap(1, nodes_to_send, roots)
            self.assertEquals(roots2, set([node2_hash, node3_hash]))
            
            
            self.assertEqual(log1.other_replica_roots[2], set([log1._get_genesis_node_hash()]))
            self.assertEqual(log2.other_replica_roots[1], set([log1._get_genesis_node_hash()]))
            
            log1.swap_final(2, nodes_to_send2, roots2)
            on_deliver()
            
            self.assertEqual(log1.other_replica_roots[2], set([node2_hash, node3_hash]))
            self.assertEqual(log1.other_replica_roots[2], log2.other_replica_roots[1])
            
            
    def test_swap_concurrent_ops(self):
            
            uuids = [1, 2]
            id1, id2 = uuids  
            
            log1 = MerkleLog(id1, uuids)
            log2 = MerkleLog(id2, uuids)
            
            logs = [log1, log2]
            
            genesis_node = log1._get_genesis_node_hash()
            ## Replica 1: G <- (10) <- (20)
            log1_first_node = log1.add_node(10)
            log1_second_node = log1.add_node(20)
            ## Replica 2: G <- (11)
            log2_first_node = log2.add_node(11)
            
            ## Replica 1 initiates swap, and prepares payload
            nodes_to_send, roots =  log1.prepare_swap(2)
            expected_node_delta = [log1_first_node, log1_second_node]
            ## Payloud should inclue (10) and (20)
            self.assertEquals(set(nodes_to_send.keys()), set(expected_node_delta))
            
            
            ## Replica 1 adds a log node after initiating swap, SHOULD NOT be reflected in common subgraph
            ## Replica 1 :  G <- (10) <- (20) <- (30)
            log1_third_node = log1.add_node(30)
            
            ## Replica 2 adds a log node after Replica 1 initiates a swap while it is in network, SHOULD be reflected in common subgraph
            ## Replica 2 :  G <- (11) <- (12)
            log2_second_node = log2.add_node(12)
                        
                        
            ## Replica 2 responds to swap, and prepares payload        
            nodes_to_send2, roots2, on_deliver = log2.respond_to_swap(1, nodes_to_send, roots)
            ## Payload should include (11) and (12)
            expected_node_delta = [log2_first_node, log2_second_node]
            self.assertEquals(set(nodes_to_send2.keys()), set(expected_node_delta))


            ## Replica 1 and 2 SHOULD NOT have updated their other replica roots yet
            self.assertEqual(log1.other_replica_roots[2], set([genesis_node]))
            self.assertEqual(log2.other_replica_roots[1], set([genesis_node]))
            
            log1.swap_final(2, nodes_to_send2, roots2)
            on_deliver()
        
            self.assertEqual(log1.other_replica_roots[2], set([log1_second_node, log2_second_node]))
            self.assertEqual(log1.other_replica_roots[2], log2.other_replica_roots[1])
            
            # ## Replica 1 roots should be (30) and (12)
            self.assertEqual(set(log1.roots), set([log1_third_node, log2_second_node]) )
            self.assertEqual(set(log2.roots), set([log1_second_node, log2_second_node]) )
            
            log1_fourth_node = log1.add_node(40)
            self.assertEqual(set(log1.roots), set([log1_fourth_node]))
        
            log1_fifth_node = log1.add_node(50)
            self.assertEqual(set(log1.roots), set([log1_fifth_node]))
            
            
            log2_third_node = log2.add_node(13)
            self.assertEqual(set(log2.roots), set([log2_third_node]))
            
            ## Checking replica 1
            self.assertEqual(log1.dependencies,{
                genesis_node: [],
                log1_first_node: (genesis_node,),
                log1_second_node: (log1_first_node,),
                log1_third_node: (log1_second_node,),
                log1_fourth_node: (log1_third_node, log2_second_node),
                log2_first_node: (genesis_node,),
                log2_second_node: (log2_first_node,),
                log1_fifth_node: (log1_fourth_node,)
            })
            
            self.assertEqual(log1.dependents, {
                genesis_node: sorted([log1_first_node, log2_first_node]),
                log1_first_node: [log1_second_node],
                log1_second_node: [log1_third_node],
                log1_third_node: [log1_fourth_node],
                log2_first_node: [log2_second_node],
                log2_second_node: [log1_fourth_node],
                log1_fourth_node: [log1_fifth_node]
            })
            
            ## Checking replica 2
            self.assertEqual(log2.dependencies,{
                genesis_node: [],
                log1_first_node: (genesis_node,),
                log1_second_node: (log1_first_node,),
                log2_first_node: (genesis_node,),
                log2_second_node: (log2_first_node,),
                log2_third_node: (log1_second_node, log2_second_node ),
            })

            self.assertEqual(log2.dependents, {
                genesis_node: sorted([log2_first_node, log1_first_node ]),
                log1_first_node: [log1_second_node],
                log1_second_node: [log2_third_node],
                log2_first_node: [log2_second_node],
                log2_second_node: [log2_third_node],
            })
        
    def test_stability_two(self):
        
            uuids = [1, 2]
            id1, id2 = uuids  
            
            log1 = MerkleLog(id1, uuids)
            log2 = MerkleLog(id2, uuids)
            
            logs = [log1, log2]
            
        
            genesis_node = log1._get_genesis_node_hash()
            node1_hash = log1.add_node(10)
            node2_hash = log1.add_node(20)
            node3_hash = log2.add_node(11)
        
            
            self.assertEqual(log1.check_stable(genesis_node), True)
            self.assertEqual(log2.check_stable(genesis_node), True)
            self.assertEqual(log1.check_stable(node1_hash), False)
            self.assertEqual(log1.check_stable(node2_hash), False)
            self.assertEqual(log2.check_stable(node3_hash), False)
            
            nodes_to_send, roots_to_send = log1.prepare_swap(2)
            
            nodes_to_send2, roots_to_send2, on_deliver = log2.respond_to_swap(1, nodes_to_send, roots_to_send)
            
            self.assertEqual(log1.check_stable(genesis_node), True)
            self.assertEqual(log2.check_stable(genesis_node), True)
            self.assertEqual(log1.check_stable(node1_hash), False)
            self.assertEqual(log1.check_stable(node2_hash), False)
            self.assertEqual(log2.check_stable(node3_hash), False)
            
            log1.swap_final(2, nodes_to_send2, roots_to_send2)
        
            ## Log1 should update stability
            self.assertEqual(log1.check_stable(genesis_node), True)
            self.assertEqual(log1.check_stable(node1_hash), True)
            self.assertEqual(log1.check_stable(node2_hash), True)
            self.assertEqual(log1.check_stable(node3_hash), True)
            
            ## Log2 should not update stability until it receives ACK
            self.assertEqual(log2.check_stable(genesis_node), True)
            self.assertEqual(log2.check_stable(node1_hash), False)
            self.assertEqual(log2.check_stable(node2_hash), False)
            self.assertEqual(log2.check_stable(node3_hash), False)
            
            ## On ACK, should change
            on_deliver()
            self.assertEqual(log2.check_stable(genesis_node), True)
            self.assertEqual(log2.check_stable(node1_hash), True)
            self.assertEqual(log2.check_stable(node2_hash), True)
            self.assertEqual(log2.check_stable(node3_hash), True)
            
            ## New nodes appended shouldn't be stable
            
            node4_hash = log1.add_node(30)
            node5_hash = log2.add_node(12)
            
            self.assertEqual(log1.check_stable(node4_hash), False)
            self.assertEqual(log2.check_stable(node5_hash), False)


    def swap(self, log1, log2):
        nodes_to_send, roots_to_send = log1.prepare_swap(log2.my_uuid)
        nodes_to_send2, roots_to_send2, on_deliver = log2.respond_to_swap(log1.my_uuid, nodes_to_send, roots_to_send)
        log1.swap_final(log2.my_uuid, nodes_to_send2, roots_to_send2)
        on_deliver()

        
    
    def test_stability_three_simple(self):
        uuids = [1, 2, 3]
        id1, id2, id3 = uuids  
        
        log1 = MerkleLog(id1, uuids)
        log2 = MerkleLog(id2, uuids)
        log3 = MerkleLog(id3, uuids)
        
        log1_node = log1.add_node(10)
        
        self.swap(log1, log2)
        
        
        
        self.swap(log1, log3)
        
        self.assertTrue(log1.check_stable(log1_node))
        self.assertFalse(log2.check_stable(log1_node))
        self.assertFalse(log3.check_stable(log1_node))
                
        self.swap(log2, log3)
        
        self.assertTrue(log1.check_stable(log1_node))
        self.assertTrue(log2.check_stable(log1_node))
        self.assertTrue(log3.check_stable(log1_node))
            
    def test_stability_three(self):
    
        uuids = [1, 2, 3]
        id1, id2, id3 = uuids  
        
        log1 = MerkleLog(id1, uuids)
        log2 = MerkleLog(id2, uuids)
        log3 = MerkleLog(id3, uuids)
        
        log1_node = log1.add_node(10)
        log2_node = log2.add_node(20)
        log3_node = log3.add_node(30)
        
        self.swap(log1, log2)
        
        self.assertFalse(log1.check_stable(log1_node))
        self.assertFalse(log1.check_stable(log2_node))
        self.assertFalse(log2.check_stable(log1_node))
        self.assertFalse(log2.check_stable(log2_node))

        self.swap(log2, log3)
        
        self.assertTrue(log2.check_stable(log1_node))
        self.assertTrue(log2.check_stable(log2_node))
        self.assertFalse(log2.check_stable(log3_node))
        
        
        self.assertFalse(log3.check_stable(log1_node))
        self.assertFalse(log3.check_stable(log2_node))
        self.assertFalse(log3.check_stable(log3_node))
        
        self.swap(log1, log3)
        
        self.assertTrue(log1.check_stable(log1_node))
        self.assertTrue(log1.check_stable(log2_node))
        self.assertFalse(log1.check_stable(log3_node))
        
        self.assertTrue(log3.check_stable(log1_node))
        self.assertTrue(log3.check_stable(log2_node))
        self.assertTrue(log3.check_stable(log3_node))
        
        # reswapping shouldn't change anything
        self.swap(log1, log3)
        
        self.assertTrue(log1.check_stable(log1_node))
        self.assertTrue(log1.check_stable(log2_node))
        self.assertFalse(log1.check_stable(log3_node))
        
        self.assertTrue(log3.check_stable(log1_node))
        self.assertTrue(log3.check_stable(log2_node))
        self.assertTrue(log3.check_stable(log3_node))
        
        # swapping should let log
        self.swap(log1, log2)
        
        self.assertTrue(log1.check_stable(log1_node))
        self.assertTrue(log1.check_stable(log2_node))
        self.assertTrue(log1.check_stable(log3_node))
        
        self.assertTrue(log2.check_stable(log1_node))
        self.assertTrue(log2.check_stable(log2_node))
        self.assertTrue(log2.check_stable(log3_node))
       
       # check roots
        
        self.assertEquals(set(log1.roots), set([log1_node, log2_node, log3_node]))
        self.assertEquals(set(log2.roots), set([log1_node, log2_node, log3_node]))
        self.assertEquals(set(log3.roots), set([log1_node, log2_node, log3_node]))

        self.assertEquals(log1.dependencies, log2.dependencies)
        self.assertEquals(log2.dependencies, log3.dependencies)
        self.assertEquals(log1.dependents, log2.dependents)
        self.assertEquals(log2.dependents, log3.dependents)
        
        # check new append 
        
        log1_node2 = log1.add_node(11)
        
        self.assertEquals(set(log1.roots), set([log1_node2]))
        self.assertFalse(log1.check_stable(log1_node2))
        
        

        self.swap(log1, log2)

        self.assertEquals(set(log1.roots), set([log1_node2]))
        self.assertFalse(log1.check_stable(log1_node2))
        self.assertEquals(set(log2.roots), set([log1_node2]))
        self.assertFalse(log2.check_stable(log1_node2))
        
        self.swap(log2, log3)

        self.assertEquals(set(log1.roots), set([log1_node2]))
        self.assertFalse(log1.check_stable(log1_node2))
        self.assertEquals(set(log2.roots), set([log1_node2]))
        #log 2 should now mark it as stable
        self.assertTrue(log2.check_stable(log1_node2))
        self.assertEquals(set(log3.roots), set([log1_node2]))
        self.assertFalse(log3.check_stable(log1_node2))  
        
        self.swap(log3, log1)
        
        #all should have it stable

        self.assertEquals(set(log1.roots), set([log1_node2]))
        self.assertTrue(log1.check_stable(log1_node2))
        self.assertEquals(set(log2.roots), set([log1_node2]))
        self.assertTrue(log2.check_stable(log1_node2))
        self.assertEquals(set(log3.roots), set([log1_node2]))
        self.assertTrue(log3.check_stable(log1_node2))  
    
    def test_stability_three_concurrent(self):

        uuids = [1, 2, 3]
        id1, id2, id3 = uuids  
        
        log1 = MerkleLog(id1, uuids)
        log2 = MerkleLog(id2, uuids)
        log3 = MerkleLog(id3, uuids)
        
        node10 = log1.add_node(10)
        node20 = log2.add_node(20)
        node30 = log3.add_node(30)
        
        nodes_to_send, roots_to_send = log1.prepare_swap(log2.my_uuid)
        
        #(11) shouln't be in subgraph
        node11 = log1.add_node(11)
        #(21) should be in subgraph
        node21 = log2.add_node(21)
        
        nodes_to_send2, roots_to_send2, on_deliver = log2.respond_to_swap(log1.my_uuid, nodes_to_send, roots_to_send)
        
        #(22) shouldn't be in subgraph
        node22 = log2.add_node(22)
        
        log1.swap_final(log2.my_uuid, nodes_to_send2, roots_to_send2)
        #(12) shouldn't be in subgraph
        node12 = log1.add_node(12)
        on_deliver()
        
        #log1 and 2 share (10, 20, 21)
        
        
        #log2 should be
        #   (10) (11) 
        # G           (22)
        #   (20) (21) 
        
        #log1 should be
        #   (10) (11) 
        # G           (12)
        #   (20) (21) 
        
        self.assertEqual(set(log1.roots), set([node12]))
        self.assertEqual(set(log2.roots), set([node22]))
        self.assertEqual( log1.other_replica_roots[log2.my_uuid], log2.other_replica_roots[log1.my_uuid] )
        
        
        nodes_to_send, roots_to_send = log3.prepare_swap(log1.my_uuid)
        
        #(31) shouln't be in subgraph
        node31 = log3.add_node(31)
        #(13) should be in subgraph
        node13 = log1.add_node(13)
        
        nodes_to_send2, roots_to_send2, on_deliver = log1.respond_to_swap(log3.my_uuid, nodes_to_send, roots_to_send)
        
        #(14) shouldn't be in subgraph
        node14 = log1.add_node(14)
        
        log3.swap_final(log1.my_uuid, nodes_to_send2, roots_to_send2)
        #(32) shouldn't be in subgraph
        node32= log3.add_node(32)
        #(15) shouldn't be in subgraph
        node15 = log1.add_node(15)
        on_deliver()
        
        #log1 and 3 share (10,11, 12, 13, 20, 21, 30)
        
        
        #log1 should be
        #   (10) (11) 
        # G           (12) (13)
        #   (20) (21)            V   
        #                         (14) -> (15)
        #   (30)                ^
        
        #log3 should be
        #   (10) (11) 
        # G           (12) (13)
        #   (20) (21)            V   
        #                          (32)
        #   (30) -> (31)          ^     
        
        self.assertEqual(set(log1.roots), set([node15]) )
        self.assertEqual(set(log3.roots), set([node32]) )
        self.assertEqual( log1.other_replica_roots[log3.my_uuid], log3.other_replica_roots[log1.my_uuid] )

        
        #Stable should be 
        
        #Log 1 has the following subgraphs defined
        # Replica 2 : {10, 21} roots
        # Replica 3: {30, 21, 13}roots
        self.assertTrue(log1.check_stable(node10))
        self.assertFalse(log1.check_stable(node11))
        self.assertTrue(log1.check_stable(node20))
        self.assertTrue(log1.check_stable(node21))
        self.assertFalse(log1.check_stable(node12))
        self.assertFalse(log1.check_stable(node14))
        self.assertFalse(log1.check_stable(node15))
        self.assertFalse(log3.check_stable(node30))

        # Log 2 should think everything is unstable
        # Replica 1 : {10, 21} roots
        # Replica 3 : {g} roots
        self.assertFalse(log2.check_stable(node10))
        self.assertFalse(log2.check_stable(node11))
        self.assertFalse(log2.check_stable(node20))
        self.assertFalse(log2.check_stable(node21))
        self.assertFalse(log2.check_stable(node22))
        
        # Log 3 should think that everything is unstable as it hasn't talked to Log 2
        # Replica 2 : {g} roots
        # Replica 3: {30, 21, 13}roots
        self.assertFalse(log3.check_stable(node10))
        self.assertFalse(log3.check_stable(node11))
        self.assertFalse(log3.check_stable(node12))
        self.assertFalse(log3.check_stable(node13))
        self.assertFalse(log3.check_stable(node20))
        self.assertFalse(log3.check_stable(node21))
        self.assertFalse(log3.check_stable(node30))
        self.assertFalse(log3.check_stable(node31))


        
        
        self.swap(log2, log3)
        self.assertEqual( log2.other_replica_roots[log3.my_uuid], log3.other_replica_roots[log2.my_uuid] )

        #Stable should be 
        
        #Log 1 has the following subgraphs defined (NOTHING CHANGED)
        # Replica 2 : {10, 21} roots
        # Replica 3: {30, 21, 13}roots
        self.assertTrue(log1.check_stable(node10))
        self.assertFalse(log1.check_stable(node11))
        self.assertFalse(log1.check_stable(node12))
        self.assertFalse(log1.check_stable(node13))
        self.assertFalse(log1.check_stable(node14))
        self.assertFalse(log1.check_stable(node15))
        self.assertTrue(log1.check_stable(node20))
        self.assertTrue(log1.check_stable(node21))
        self.assertFalse(log1.check_stable(node30))
        self.assertFalse(log2.check_stable(node31))
        self.assertFalse(log2.check_stable(node32))

        # Log 2
        # Replica 1 : {10, 21} roots
        # Replica 3 : {32, 22} roots
        self.assertTrue(log2.check_stable(node10))
        self.assertFalse(log2.check_stable(node11))
        self.assertFalse(log2.check_stable(node12))
        self.assertFalse(log2.check_stable(node13))
        self.assertTrue(log2.check_stable(node20))
        self.assertTrue(log2.check_stable(node21))
        self.assertFalse(log2.check_stable(node22))
        self.assertFalse(log2.check_stable(node30))
        self.assertFalse(log2.check_stable(node31))
        self.assertFalse(log2.check_stable(node32))

        # Log 3 should think that everything is unstable as it hasn't talked to Log 2
        # Replica 2 : {32, 22} roots
        # Replica 1: {30, 21, 13}roots
        self.assertTrue(log3.check_stable(node10))
        self.assertTrue(log3.check_stable(node11))
        self.assertTrue(log3.check_stable(node12))
        self.assertTrue(log3.check_stable(node13))
        self.assertTrue(log3.check_stable(node20))
        self.assertTrue(log3.check_stable(node21))
        self.assertFalse(log3.check_stable(node22))
        self.assertTrue(log3.check_stable(node30))
        self.assertFalse(log3.check_stable(node31))
        self.assertFalse(log3.check_stable(node32))


        self.swap(log3, log1)
        self.assertEqual( log1.other_replica_roots[log3.my_uuid], log3.other_replica_roots[log1.my_uuid] )

        # #Stable should be 
        
        #Log 1 has the following subgraphs defined (NOTHING CHANGED)
        # Replica 2 : {10, 21} roots
        # Replica 3: {32, 22, 15} roots
        self.assertTrue(log1.check_stable(node10))
        self.assertFalse(log1.check_stable(node11))
        self.assertFalse(log1.check_stable(node12))
        self.assertFalse(log1.check_stable(node13))
        self.assertFalse(log1.check_stable(node14))
        self.assertFalse(log1.check_stable(node15))
        self.assertTrue(log1.check_stable(node20))
        self.assertTrue(log1.check_stable(node21))
        self.assertFalse(log1.check_stable(node22))
        self.assertFalse(log1.check_stable(node30))
        self.assertFalse(log1.check_stable(node31))
        self.assertFalse(log1.check_stable(node32))

        # Log 2
        # Replica 1 : {10, 21} roots
        # Replica 3 : {32, 22} roots
        self.assertTrue(log2.check_stable(node10))
        self.assertFalse(log2.check_stable(node11))
        self.assertFalse(log2.check_stable(node12))
        self.assertFalse(log2.check_stable(node13))
        self.assertTrue(log2.check_stable(node20))
        self.assertTrue(log2.check_stable(node21))
        self.assertFalse(log2.check_stable(node22))
        self.assertFalse(log2.check_stable(node30))
        self.assertFalse(log2.check_stable(node31))
        self.assertFalse(log2.check_stable(node32))

        # Log 3 should think that everything is unstable as it hasn't talked to Log 2
        # Replica 2 : {32, 22} roots
        # Replica 1: {32, 22, 15} roots
        self.assertTrue(log3.check_stable(node10))
        self.assertTrue(log3.check_stable(node11))
        self.assertTrue(log3.check_stable(node12))
        self.assertTrue(log3.check_stable(node13))
        self.assertFalse(log3.check_stable(node14))
        self.assertFalse(log3.check_stable(node15))
        self.assertTrue(log3.check_stable(node20))
        self.assertTrue(log3.check_stable(node21))
        self.assertTrue(log3.check_stable(node22))
        self.assertTrue(log3.check_stable(node30))
        self.assertTrue(log3.check_stable(node31))
        self.assertTrue(log3.check_stable(node32))
        
        
        self.swap(log1, log2)

        #Log 1 has the following subgraphs defined (NOTHING CHANGED)
        # Replica 2 : {32, 22, 15} roots
        # Replica 3: {32, 22, 15} roots
        self.assertTrue(log1.check_stable(node10))
        self.assertTrue(log1.check_stable(node11))
        self.assertTrue(log1.check_stable(node12))
        self.assertTrue(log1.check_stable(node13))
        self.assertTrue(log1.check_stable(node14))
        self.assertTrue(log1.check_stable(node15))
        self.assertTrue(log1.check_stable(node20))
        self.assertTrue(log1.check_stable(node21))
        self.assertTrue(log1.check_stable(node22))
        self.assertTrue(log1.check_stable(node30))
        self.assertTrue(log1.check_stable(node31))
        self.assertTrue(log1.check_stable(node32))
        
        # Log 2
        # Replica 1 : {32, 22, 15} roots
        # Replica 3 : {32, 22} roots
        self.assertTrue(log2.check_stable(node10))
        self.assertTrue(log2.check_stable(node11))
        self.assertTrue(log2.check_stable(node12))
        self.assertTrue(log2.check_stable(node13))
        self.assertFalse(log2.check_stable(node14))
        self.assertFalse(log2.check_stable(node15))
        self.assertTrue(log2.check_stable(node20))
        self.assertTrue(log2.check_stable(node21))
        self.assertTrue(log2.check_stable(node22))
        self.assertTrue(log2.check_stable(node30))
        self.assertTrue(log2.check_stable(node31))
        self.assertTrue(log2.check_stable(node32))
        
        # Log 3 should think that everything is unstable as it hasn't talked to Log 2
        # Replica 2 : {32, 22} roots
        # Replica 1: {32, 22, 15} roots
        self.assertTrue(log3.check_stable(node10))
        self.assertTrue(log3.check_stable(node11))
        self.assertTrue(log3.check_stable(node12))
        self.assertTrue(log3.check_stable(node13))
        self.assertFalse(log3.check_stable(node14))
        self.assertFalse(log3.check_stable(node15))
        self.assertTrue(log3.check_stable(node20))
        self.assertTrue(log3.check_stable(node21))
        self.assertTrue(log3.check_stable(node22))
        self.assertTrue(log3.check_stable(node30))
        self.assertTrue(log3.check_stable(node31))
        self.assertTrue(log3.check_stable(node32))
        
        
        self.swap(log2, log3)
        self.assertEqual( log2.other_replica_roots[log3.my_uuid], log3.other_replica_roots[log2.my_uuid] )

    
        #Log 1 has the following subgraphs defined (NOTHING CHANGED)
        # Replica 2 : {32, 22, 15} roots
        # Replica 3: {32, 22, 15} roots
        self.assertTrue(log1.check_stable(node10))
        self.assertTrue(log1.check_stable(node11))
        self.assertTrue(log1.check_stable(node12))
        self.assertTrue(log1.check_stable(node13))
        self.assertTrue(log1.check_stable(node14))
        self.assertTrue(log1.check_stable(node15))
        self.assertTrue(log1.check_stable(node20))
        self.assertTrue(log1.check_stable(node21))
        self.assertTrue(log1.check_stable(node22))
        self.assertTrue(log1.check_stable(node30))
        self.assertTrue(log1.check_stable(node31))
        self.assertTrue(log1.check_stable(node32))
        
        # Log 2
        # Replica 1 : {32, 22, 15} roots
        # Replica 3 : {32, 22, 15} roots
        self.assertTrue(log2.check_stable(node10))
        self.assertTrue(log2.check_stable(node11))
        self.assertTrue(log2.check_stable(node12))
        self.assertTrue(log2.check_stable(node13))
        self.assertTrue(log2.check_stable(node14))
        self.assertTrue(log2.check_stable(node15))
        self.assertTrue(log2.check_stable(node20))
        self.assertTrue(log2.check_stable(node21))
        self.assertTrue(log2.check_stable(node22))
        self.assertTrue(log2.check_stable(node30))
        self.assertTrue(log2.check_stable(node31))
        self.assertTrue(log2.check_stable(node32))
        
        # Log 3 should think that everything is unstable as it hasn't talked to Log 2
        # Replica 2 : {32, 22, 15} roots
        # Replica 1: {32, 22, 15} roots
        self.assertTrue(log3.check_stable(node10))
        self.assertTrue(log3.check_stable(node11))
        self.assertTrue(log3.check_stable(node12))
        self.assertTrue(log3.check_stable(node13))
        self.assertTrue(log3.check_stable(node14))
        self.assertTrue(log3.check_stable(node15))
        self.assertTrue(log3.check_stable(node20))
        self.assertTrue(log3.check_stable(node21))
        self.assertTrue(log3.check_stable(node22))
        self.assertTrue(log3.check_stable(node30))
        self.assertTrue(log3.check_stable(node31))
        self.assertTrue(log3.check_stable(node32))
            

if __name__ == '__main__':
    unittest.main()