def dfs(graph, start_node, info, visited=None, result=None):
    if visited is None:
        visited = set()
    if result is None:
        result = {}

    visited.add(start_node)
    # Process the current node based on `info`. This is where you filter/subset data.
    result[start_node] = process_node(start_node, info)

    for neighbor in graph[start_node]:
        if neighbor not in visited:
            dfs(graph, neighbor, info, visited, result)

    return result

def process_node(node, info):
    print(node)
    return f"Data for {node} based on {info}"

# Example usage
graph = {
    "public.data_src": ["public.datsrcln"],
    "public.datsrcln": ["public.nut_data"],
    "public.nut_data": ["public.deriv_cd", "public.food_des", "public.nutr_def", "public.src_cd"],
    "public.deriv_cd": [],  # No further dependencies
    "public.food_des": ["public.fd_group"],
    "public.fd_group": [],  # Assuming no further dependencies
    "public.nutr_def": [],  # Assuming no further dependencies
    "public.src_cd": [],    # Assuming no further dependencies
    # Add other nodes as needed, ensuring all are accounted for
}

start_node = "public.data_src"
info = {'rows': [2, 3, 4]}  # Example specific information
result = dfs(graph, start_node, info)
