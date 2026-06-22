// Users over an age threshold (property index range seek).
MATCH (n:User) WHERE n.age >= 40 RETURN n.name, n.age LIMIT 100
