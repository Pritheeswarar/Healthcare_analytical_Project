# tSQLt Test Harness

1. Install tSQLt into a disposable development database (`EXEC tSQLt.Install;`).
2. Run the commands in `tests/bootstrap.sql` to create baseline test classes (`test_transform`, `test_kpis`) and any shared fixtures.
3. Author Arrange-Act-Assert style test procedures under the appropriate class using `tSQLt.FakeTable` for staging tables and `tSQLt.AssertEqualsTable` for result comparisons.
4. Execute the suite with `EXEC tSQLt.RunAll;` before opening a PR and capture failures in the PR description.
5. Reset the database between runs if tests mutate state; never commit generated data or tSQLt binaries to git.
