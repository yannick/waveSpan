// One-hop expansion over the FOLLOWS adjacency.
MATCH (n:User {id: 'user-0'})-[:FOLLOWS]->(m) RETURN m.name
