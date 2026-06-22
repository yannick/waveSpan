// Approximate vector search over the docs index (requires `docs` index + ingested vectors).
CALL vector.searchApprox('docs', [1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0], 10, {efSearch: 64}) YIELD node, score RETURN node, score
