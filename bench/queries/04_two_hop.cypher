// Two-hop traversal (friends of friends).
MATCH (n:User {id: 'user-0'})-[:FOLLOWS]->(m)-[:FOLLOWS]->(p) RETURN p.name LIMIT 50
