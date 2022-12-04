from collections import deque
import bisect 


def h(x):
    return x.__hash__()


class MerkleLog:
    
    class _MerkleLogNode: 
        def __init__(self, dependencies, value):
            self.dependencies = tuple(dependencies)
            self.value = value
            self.stable = False
        
        def __hash__(self) -> int:
            return hash((self.dependencies, self.value))
            
        def is_stable(self):
            return self.stable
        def mark_stable(self):
            self.stable = True
            
        def get_copy(self):
            return self.__init__(self.dependencies, self.value)
            
        def __repr__(self) -> str:
            return str(self.value)
    def __init__(self, my_uuid, other_replicas, enable_compaction = False): 
        self.other_replicas = [r for r in other_replicas if r!=my_uuid]
        self.my_uuid = my_uuid
        
        genesis_node = self._construct_genesis_node()
        genesis_node.mark_stable()
        
        self.other_replica_roots = { uuid : set([h(genesis_node)]) for uuid in self.other_replicas if uuid != self.my_uuid}
        
        self.nodes = {h(genesis_node) : genesis_node}
        self.dependencies = {h(genesis_node): []}
        self.dependents = {}
        self.roots = [h(genesis_node)]
        
        self.compacted = set([h(genesis_node)])
        self.auto_compaction = enable_compaction
        
    def _exists(self, hash):
        return hash in self.compacted or hash in self.nodes
                
    def _construct_genesis_node(self):
        return self._MerkleLogNode([], 0)
    
    def _get_genesis_node_hash(self):
        return h(self._construct_genesis_node())
    
    def _new_node(self, value):
        prev_roots = self.roots
        new_node = self._MerkleLogNode(prev_roots, value)
        new_node_hash = h(new_node)
        
        self._add_node_graph(new_node)
        self._add_node_reverse_graph(new_node)
        
        self.roots = [new_node_hash]
        return new_node_hash
        
    
    def _add_node_graph(self,node):
        node_hash = h(node)
        self.nodes[node_hash] = node

        self.dependencies[node_hash] = node.dependencies
        
    def _add_node_reverse_graph(self, node):
        node_hash = h(node)
        for dependencies in node.dependencies:
            if dependencies not in self.dependents:
                self.dependents[dependencies] = []
            bisect.insort(self.dependents[dependencies], node_hash) 
        
    def add_node(self, value):
        return self._new_node(value)
                
                       
    def _verify_delta(self, nodes):
        return all([ h(node) == hash for hash, node in nodes.items()])
    
    def _add_verified_nodes(self, nodes):
        for hash, node in nodes.items():
            if not self._exists(hash):
                copy_node = self._MerkleLogNode(node.dependencies, node.value)
                self.nodes[hash] = copy_node 
                self._add_node_graph(copy_node)
                self._add_node_reverse_graph(copy_node)
        
    def _bfs_from_roots_until(self, filter_fn):
        return self._bfs_from_nodes_until(self.roots, filter_fn)
        
    def _bfs_from_nodes_until(self, nodes, filter_fn):
        queue = deque(nodes)
        seen = set()
        while queue:
            n = queue.pop()
            if filter_fn(n) and n not in seen:
                seen.add(n)
                queue.extend(self.dependencies[n])    
        return seen
    
    def prepare_swap(self, other_uuid):
        other_roots = self.other_replica_roots[other_uuid]
        filter_fn = lambda x : x not in other_roots
        hashes_to_send = self._bfs_from_roots_until(filter_fn)
        return  { h:self.nodes[h] for h in hashes_to_send}, set(self.roots)
    
    def is_root(self, hash):
        return hash not in self.dependents
    
    def _determine_new_roots(self, received_nodes, received_roots):
        root_same = received_roots.intersection(set(self.roots))
        ## new roots that haven't been seen before MUST be new roots of common subgraph
        new_remote_roots = set(filter(lambda root: not self._exists(root), received_roots))
        if new_remote_roots:        
            self._add_verified_nodes(received_nodes)
        ## old roots that don't have dependents in new subgraph will stay as roots
        kept_local_roots = set(filter(self.is_root, self.roots))
        return root_same.union(new_remote_roots).union(kept_local_roots)
    
    def respond_to_swap(self,other_uuid, received_nodes, received_roots):
        if not self._verify_delta(received_nodes):
            raise Exception("Bad delta received")
        
        new_roots = self._determine_new_roots(received_nodes, received_roots)
    
        filter_fn = lambda x : x not in self.other_replica_roots[other_uuid] and x not in received_roots
        hashes_to_send = self._bfs_from_roots_until(filter_fn)

        self.roots = tuple(new_roots)
        
        def on_deliver():
            self.other_replica_roots[other_uuid] = new_roots
            self.update_stability()
    
        return { h:self.nodes[h] for h in hashes_to_send}, new_roots, on_deliver 
        
    def swap_final(self, other_uuid, received_nodes, received_roots):
        if not self._verify_delta(received_nodes):
            raise Exception("Bad delta received")
            
        self.other_replica_roots[other_uuid] = received_roots
        new_roots = self._determine_new_roots(received_nodes, received_roots)
        self.roots = tuple(new_roots)
        self.update_stability()

    def update_stability(self):
       
        unstable_seen_everywhere = self._bfs_from_roots_until(lambda x : not self.check_stable(x))
       
        for replica in self.other_replicas:
        
            other_replica_roots = self.other_replica_roots[replica]
            seen_non_stable = self._bfs_from_nodes_until(other_replica_roots, lambda x :not self.check_stable(x))
            unstable_seen_everywhere = unstable_seen_everywhere.intersection(seen_non_stable)

        for hash in unstable_seen_everywhere:
            self.nodes[hash].mark_stable()
        
        
        if self.auto_compaction:
            cog = self.next_cog()
            if cog:
                self.compact_log(cog)
        
    def check_stable(self, hash):
        return hash in self.compacted or ( hash in self.nodes and self.nodes[hash].is_stable() )

    def is_compacted(self, hash):
        return hash in self.compacted
    
    def solely_dependent(self, node, hashes):
        return all([ (d in hashes) for d in self.dependencies[node] ])
    
    def solely_dependent_on_compact(self, node):
        return self.solely_dependent(node, self.compacted)
    
    def get_compact_frontier(self):
        compacted_frontier = set()
        for hash in self.compacted:
            for dependent in self.dependents[hash]:
                if self.solely_dependent_on_compact(dependent):
                    compacted_frontier.add(dependent)
        return compacted_frontier
    
    def sole_dependents(self, node_hash):
        return [] if node_hash not in self.dependents else [d for d in self.dependents[node_hash] if self.solely_dependent(d, [node_hash])]
    
    def next_cog(self):
        
        queue = deque(self.get_compact_frontier()) 
        next_cog = set()
        
        while queue:
            n = queue.pop()
            if not self.check_stable(n):
                return set()
            next_cog.add(n)
            for sole_dependent in self.sole_dependents(n):
                queue.append(sole_dependent)
            
        return next_cog
        
    def compact_log(self, next_cog):
        print("To compact", [ self.nodes[hash].value for hash in next_cog])
        
        for n in next_cog:
            
            for d in self.dependencies[n]:
                self.dependents[d].remove(n)
                
                if d in self.dependents and not self.dependents[d]:
                    if d in self.compacted:
                        self.compacted.remove(d)
                        del self.dependents[d]
            
            del self.nodes[n]
            
            if n in self.dependents and self.dependents[n]:
                self.compacted.add(n)
                
        
    def __eq__(self, __o: object) -> bool:
        if not isinstance(__o, MerkleLog):
            return False 
        else:
            return self.dependencies == __o.dependencies and self.dependents == __o.dependents and self.roots == __o.roots
        