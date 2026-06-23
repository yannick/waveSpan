// Package sampledata holds small, open-licensed demo graphs that the node UI can load for intuitive
// exploration (design/26). The data is plain Go fixtures; the observability loader assigns versions
// and commits them through the graph store's atomic Batch API.
package sampledata

import wavespanv1 "github.com/cwire/wavespan/proto/wavespan/v1"

// Node is a fixture node: a stable id, one or more labels, and property values.
type Node struct {
	ID     string
	Labels []string
	Props  map[string]*wavespanv1.Value
}

// Edge is a fixture relationship between two node ids.
type Edge struct {
	Start string
	Type  string
	End   string
	Props map[string]*wavespanv1.Value
}

// Dataset bundles a named fixture with its provenance so the UI can show attribution.
type Dataset struct {
	Name        string
	License     string
	Attribution string
	Nodes       []Node
	Edges       []Edge
}

func str(s string) *wavespanv1.Value {
	return &wavespanv1.Value{Value: &wavespanv1.Value_StringValue{StringValue: s}}
}

func i64(n int64) *wavespanv1.Value {
	return &wavespanv1.Value{Value: &wavespanv1.Value_IntValue{IntValue: n}}
}

func list(items ...string) *wavespanv1.Value {
	vs := make([]*wavespanv1.Value, len(items))
	for i, it := range items {
		vs[i] = str(it)
	}
	return &wavespanv1.Value{Value: &wavespanv1.Value_ListValue{ListValue: &wavespanv1.ValueList{Values: vs}}}
}

func movie(id, title string, released int64, tagline string) Node {
	return Node{ID: id, Labels: []string{"Movie"}, Props: map[string]*wavespanv1.Value{
		"title": str(title), "released": i64(released), "tagline": str(tagline),
	}}
}

func person(id, name string, born int64) Node {
	return Node{ID: id, Labels: []string{"Person"}, Props: map[string]*wavespanv1.Value{
		"name": str(name), "born": i64(born),
	}}
}

func acted(p, m string, roles ...string) Edge {
	props := map[string]*wavespanv1.Value{}
	if len(roles) > 0 {
		props["roles"] = list(roles...)
	}
	return Edge{Start: p, Type: "ACTED_IN", End: m, Props: props}
}

func directed(p, m string) Edge { return Edge{Start: p, Type: "DIRECTED", End: m} }
func produced(p, m string) Edge { return Edge{Start: p, Type: "PRODUCED", End: m} }
func wrote(p, m string) Edge    { return Edge{Start: p, Type: "WROTE", End: m} }

func reviewed(p, m string, rating int64, summary string) Edge {
	return Edge{Start: p, Type: "REVIEWED", End: m, Props: map[string]*wavespanv1.Value{
		"rating": i64(rating), "summary": str(summary),
	}}
}

func follows(a, b string) Edge { return Edge{Start: a, Type: "FOLLOWS", End: b} }

// Movies returns a compact, fully-connected slice of the well-known "Movies" property graph: people
// and films linked by ACTED_IN / DIRECTED / PRODUCED / WROTE / REVIEWED / FOLLOWS. It is small enough
// to take in at a glance (~40 nodes) yet rich enough to show multiple labels and relationship types.
//
// Provenance: this mirrors a subset of Neo4j's public "Movies" example graph (the `:play movies`
// dataset). The underlying facts (titles, release years, cast, crew) are public information; this
// curated subset is vendored with WaveSpan under the repository licence.
func Movies() Dataset {
	nodes := []Node{
		// --- Films ---
		movie("TheMatrix", "The Matrix", 1999, "Welcome to the Real World"),
		movie("TheMatrixReloaded", "The Matrix Reloaded", 2003, "Free your mind"),
		movie("TheDevilsAdvocate", "The Devil's Advocate", 1997, "Evil has its winning ways"),
		movie("AFewGoodMen", "A Few Good Men", 1992, "In the heart of the nation's capital, a crime"),
		movie("TopGun", "Top Gun", 1986, "I feel the need, the need for speed."),
		movie("JerryMaguire", "Jerry Maguire", 2000, "The rest of his life begins now."),
		movie("StandByMe", "Stand By Me", 1986, "The last real taste of innocence."),
		movie("AsGoodAsItGets", "As Good as It Gets", 1997, "A comedy from the heart that goes for the throat."),
		movie("YouveGotMail", "You've Got Mail", 1998, "At odds in life... in love on-line."),
		movie("SleeplessInSeattle", "Sleepless in Seattle", 1993, "What if someone you never met was the one?"),
		movie("Apollo13", "Apollo 13", 1995, "Houston, we have a problem."),
		movie("TheGreenMile", "The Green Mile", 1999, "Walk a mile you'll never forget."),
		movie("CloudAtlas", "Cloud Atlas", 2012, "Everything is connected"),

		// --- People (cast & crew) ---
		person("KeanuReeves", "Keanu Reeves", 1964),
		person("CarrieAnneMoss", "Carrie-Anne Moss", 1967),
		person("LaurenceFishburne", "Laurence Fishburne", 1961),
		person("HugoWeaving", "Hugo Weaving", 1960),
		person("LanaWachowski", "Lana Wachowski", 1965),
		person("LillyWachowski", "Lilly Wachowski", 1967),
		person("JoelSilver", "Joel Silver", 1952),
		person("AlPacino", "Al Pacino", 1940),
		person("CharlizeTheron", "Charlize Theron", 1975),
		person("TomCruise", "Tom Cruise", 1962),
		person("JackNicholson", "Jack Nicholson", 1937),
		person("DemiMoore", "Demi Moore", 1962),
		person("KevinBacon", "Kevin Bacon", 1958),
		person("KieferSutherland", "Kiefer Sutherland", 1966),
		person("AaronSorkin", "Aaron Sorkin", 1961),
		person("RobReiner", "Rob Reiner", 1947),
		person("CubaGoodingJr", "Cuba Gooding Jr.", 1968),
		person("TomHanks", "Tom Hanks", 1956),
		person("MegRyan", "Meg Ryan", 1961),
		person("HelenHunt", "Helen Hunt", 1963),
		person("HalleBerry", "Halle Berry", 1966),
		person("JamesCromwell", "James Cromwell", 1940),
		person("DavidMorse", "David Morse", 1953),
		person("BillPaxton", "Bill Paxton", 1955),

		// --- Reviewers ---
		person("JessicaThompson", "Jessica Thompson", 1974),
		person("JamesThompson", "James Thompson", 1980),
		person("AngelaScope", "Angela Scope", 1979),
	}

	edges := []Edge{
		// The Matrix trilogy
		acted("KeanuReeves", "TheMatrix", "Neo"),
		acted("CarrieAnneMoss", "TheMatrix", "Trinity"),
		acted("LaurenceFishburne", "TheMatrix", "Morpheus"),
		acted("HugoWeaving", "TheMatrix", "Agent Smith"),
		directed("LanaWachowski", "TheMatrix"),
		directed("LillyWachowski", "TheMatrix"),
		produced("JoelSilver", "TheMatrix"),
		acted("KeanuReeves", "TheMatrixReloaded", "Neo"),
		acted("CarrieAnneMoss", "TheMatrixReloaded", "Trinity"),
		acted("LaurenceFishburne", "TheMatrixReloaded", "Morpheus"),
		acted("HugoWeaving", "TheMatrixReloaded", "Agent Smith"),
		directed("LanaWachowski", "TheMatrixReloaded"),
		directed("LillyWachowski", "TheMatrixReloaded"),
		produced("JoelSilver", "TheMatrixReloaded"),

		// The Devil's Advocate — Keanu bridges to the Matrix cluster
		acted("KeanuReeves", "TheDevilsAdvocate", "Kevin Lomax"),
		acted("AlPacino", "TheDevilsAdvocate", "John Milton"),
		acted("CharlizeTheron", "TheDevilsAdvocate", "Mary Ann Lomax"),
		produced("JoelSilver", "TheDevilsAdvocate"),

		// A Few Good Men
		acted("TomCruise", "AFewGoodMen", "Lt. Daniel Kaffee"),
		acted("JackNicholson", "AFewGoodMen", "Col. Nathan R. Jessup"),
		acted("DemiMoore", "AFewGoodMen", "Lt. Cdr. JoAnne Galloway"),
		acted("KevinBacon", "AFewGoodMen", "Capt. Jack Ross"),
		acted("KieferSutherland", "AFewGoodMen", "Lt. Jonathan Kendrick"),
		wrote("AaronSorkin", "AFewGoodMen"),
		directed("RobReiner", "AFewGoodMen"),

		// Top Gun — Tom Cruise links A Few Good Men; Meg Ryan links the Hanks cluster
		acted("TomCruise", "TopGun", "Maverick"),
		acted("MegRyan", "TopGun", "Carole"),

		// Jerry Maguire
		acted("TomCruise", "JerryMaguire", "Jerry Maguire"),
		acted("CubaGoodingJr", "JerryMaguire", "Rod Tidwell"),

		// Stand By Me — Kiefer + Rob Reiner bridge to A Few Good Men
		acted("KieferSutherland", "StandByMe", "Ace Merrill"),
		directed("RobReiner", "StandByMe"),

		// As Good as It Gets — Jack + Cuba bridge in
		acted("JackNicholson", "AsGoodAsItGets", "Melvin Udall"),
		acted("HelenHunt", "AsGoodAsItGets", "Carol Connelly"),
		acted("CubaGoodingJr", "AsGoodAsItGets", "Frank Sachs"),

		// Tom Hanks / Meg Ryan cluster
		acted("TomHanks", "YouveGotMail", "Joe Fox"),
		acted("MegRyan", "YouveGotMail", "Kathleen Kelly"),
		acted("TomHanks", "SleeplessInSeattle", "Sam Baldwin"),
		acted("MegRyan", "SleeplessInSeattle", "Annie Reed"),

		// Apollo 13 — Kevin Bacon bridges A Few Good Men to Tom Hanks
		acted("TomHanks", "Apollo13", "Jim Lovell"),
		acted("KevinBacon", "Apollo13", "Jack Swigert"),
		acted("BillPaxton", "Apollo13", "Fred Haise"),

		// The Green Mile
		acted("TomHanks", "TheGreenMile", "Paul Edgecomb"),
		acted("JamesCromwell", "TheGreenMile", "Warden Hal Moores"),
		acted("DavidMorse", "TheGreenMile", "Brutus Howell"),

		// Cloud Atlas — Hugo Weaving (Matrix) + Tom Hanks + the Wachowskis bridge both clusters
		acted("TomHanks", "CloudAtlas", "Zachry"),
		acted("HugoWeaving", "CloudAtlas", "Bill Smoke"),
		acted("HalleBerry", "CloudAtlas", "Luisa Rey"),
		directed("LanaWachowski", "CloudAtlas"),
		directed("LillyWachowski", "CloudAtlas"),

		// Reviewers — exercise REVIEWED + FOLLOWS
		reviewed("JessicaThompson", "CloudAtlas", 95, "An amazing journey across time."),
		reviewed("JessicaThompson", "TheGreenMile", 90, "Unforgettable and deeply moving."),
		reviewed("JamesThompson", "AFewGoodMen", 92, "You can't handle the truth — gripping."),
		reviewed("AngelaScope", "TheMatrix", 98, "Redefined what an action film could be."),
		follows("JamesThompson", "JessicaThompson"),
		follows("AngelaScope", "JessicaThompson"),
	}

	return Dataset{
		Name:        "Movies",
		License:     "Public facts; curated subset of Neo4j's example Movies graph, vendored under the WaveSpan repository licence.",
		Attribution: "Subset of the Neo4j `:play movies` example dataset.",
		Nodes:       nodes,
		Edges:       edges,
	}
}
