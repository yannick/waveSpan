// Expansion with a destination filter.
MATCH (n:User)-[:FOLLOWS]->(m:User) WHERE m.city = 'NYC' RETURN n.name, m.name LIMIT 100
