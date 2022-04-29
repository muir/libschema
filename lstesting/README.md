# libschema/lstesting - support functions for libschema tests

With libscheama, you can write tests to verify that your migrations apply cleanly.

This is particlarly useful when developing the migrations.

lstesting provides a support function to help:

```go
func TestMyMigrations(t *testing.T) {
	// Pick a random string for the name of your schema (PostgreSQL) or
	// database (MySQL).
	// use "CASCADE" for PostgreSQL, use "" for MySQL
	options, cleanup := lstesting.FakeSchema(t, "CASCADE")

	db, err := sql.Open("postgres", dsn)
	require.NoError(t, err)
	defer db.Close()

	defer cleanup(db)

	s := libschema.New(context.Background(), options)
	dbase, err := lspostgres.New(libschema.LogFromLog(t), "test", s, db)
	require.NoError(t, err)

	// Add all your migrations to dbase

	err = s.Migrate(context.Background())
	assert.NoError(t, err)
}
```

