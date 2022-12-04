import networkx as nx
import matplotlib.pyplot as plt


def visualize_dag(nodes_in_graph, edges, labels, genesis_node, replica_num, ax):
    
    DG = nx.DiGraph(edges)
    
    options = {"edgecolors": "tab:gray", "node_size": 400, "alpha": 0.9, "ax": ax}
    # nx.draw_networkx_nodes(G, pos, nodelist=[0, 1, 2, 3], node_color="tab:red", **options)
    # nx.draw_networkx_nodes(G, pos, nodelist=[4, 5, 6, 7], node_color="tab:blue", **options)
    
    
    for layer, nodes in enumerate(nx.topological_generations(DG)):
        # `multipartite_layout` expects the layer as a node attribute, so add the
        # numeric layer value as a node attribute
        for node in nodes:
            DG.nodes[node]["layer"] = layer
            
    # Compute the multipartite_layout using the "layer" node attribute
    pos = nx.multipartite_layout(DG, subset_key="layer")
    
    if len(nodes_in_graph) > 1:
        nx.draw_networkx_edges(DG, pos, width=0.3, alpha=0.5, ax=ax)

    def get_ith_replica_nodes(i):
        return [node for node in nodes_in_graph if labels[node] - (labels[node]%10) == i*10]
        

    nx.draw_networkx_nodes(DG, pos, nodelist=[genesis_node], node_color="tab:gray", **options)
    
    nx.draw_networkx_nodes(DG, pos, nodelist=get_ith_replica_nodes(1), node_color="tab:red", **options)
    nx.draw_networkx_nodes(DG, pos, nodelist=get_ith_replica_nodes(2), node_color="tab:green", **options)
    nx.draw_networkx_nodes(DG, pos, nodelist=get_ith_replica_nodes(3), node_color="tab:blue", **options)

    nx.draw_networkx_labels(DG, pos, labels, font_size=8, font_color="whitesmoke", ax=ax)
    

    plt.sca(ax)
    ax.margins(0.20)
    plt.title(str(replica_num))

def visualize_merkel(log, ax):
    genesis_node = log._get_genesis_node_hash()
    nodes = set([genesis_node])
    labels = {genesis_node: "G"}
    edges = []
    for hash, node in log.nodes.items():
        nodes.add(hash)
        labels[hash] = node.value
        for dep in node.dependencies:
            edges.append((dep, hash))
    
    if not edges:
        edges = [(genesis_node, 100)]
    visualize_dag(nodes, edges, labels, genesis_node, log.my_uuid, ax)

def visualize_multiple(logs):
    fig, axs = plt.subplots(len(logs))
    
    for i, log in enumerate(logs):
        visualize_merkel(log, axs[i])
        
    plt.show()
        