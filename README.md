# neo-utils-go
Neo4j Utils in Go.

Provides a wrapper for neoism.Database to output the database url in the String() method.

Provides and EnsureIndexes function that will take a map of label/property pairs,
and an IndexManager (normally this will be a neoism.Database) and checks whether those
indexes exist. If not, they are created.
