// All users (label scan). Tests the label index + projection path.
MATCH (n:User) RETURN n.name LIMIT 100
