// Social graph fixture for the Cypher executor oracle (M8). Statements are separated by ';'.
// 8 User nodes with name/age/city, plus FOLLOWS edges.
//
// Oracle (asserted by the fixture test):
//   MATCH (n:User) RETURN n.name
//     -> Alice, Bob, Carol, Dave, Eve, Frank, Grace, Heidi
//   MATCH (n:User) WHERE n.age >= 35 RETURN n.name
//     -> Bob, Dave, Eve, Grace
//   MATCH (n:User) WHERE n.city = 'NYC' RETURN n.name
//     -> Alice, Bob, Eve, Heidi
//   MATCH (a:User {id:'alice'})-[:FOLLOWS]->(m) RETURN m.name
//     -> Bob, Carol

CREATE (:User {id: 'alice', name: 'Alice', age: 30, city: 'NYC'});
CREATE (:User {id: 'bob', name: 'Bob', age: 40, city: 'NYC'});
CREATE (:User {id: 'carol', name: 'Carol', age: 25, city: 'SF'});
CREATE (:User {id: 'dave', name: 'Dave', age: 35, city: 'LA'});
CREATE (:User {id: 'eve', name: 'Eve', age: 45, city: 'NYC'});
CREATE (:User {id: 'frank', name: 'Frank', age: 28, city: 'SF'});
CREATE (:User {id: 'grace', name: 'Grace', age: 50, city: 'LA'});
CREATE (:User {id: 'heidi', name: 'Heidi', age: 33, city: 'NYC'});

MATCH (a:User {id: 'alice'}), (b:User {id: 'bob'}) CREATE (a)-[:FOLLOWS]->(b);
MATCH (a:User {id: 'alice'}), (b:User {id: 'carol'}) CREATE (a)-[:FOLLOWS]->(b);
MATCH (a:User {id: 'bob'}), (b:User {id: 'carol'}) CREATE (a)-[:FOLLOWS]->(b);
MATCH (a:User {id: 'dave'}), (b:User {id: 'eve'}) CREATE (a)-[:FOLLOWS]->(b);
MATCH (a:User {id: 'eve'}), (b:User {id: 'grace'}) CREATE (a)-[:FOLLOWS]->(b);
MATCH (a:User {id: 'heidi'}), (b:User {id: 'alice'}) CREATE (a)-[:FOLLOWS]->(b);
MATCH (a:User {id: 'frank'}), (b:User {id: 'heidi'}) CREATE (a)-[:FOLLOWS]->(b);
MATCH (a:User {id: 'grace'}), (b:User {id: 'alice'}) CREATE (a)-[:FOLLOWS]->(b)
