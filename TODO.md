# TODO List

- delete cached items every now and then

# WORKING List

# DONE List

- clean up facts-sql files. better named entry point.
- implement merge query in symrel
- queries could be combined better
- query params should be where clauses if they are grounded after walk when the query runs.

This query:

```
membero($.person, ["celeste"]),
familytree.parentAgg($.person, $.parents),
familytree.stepParentAgg($.person, $.step_parents),
```

Results in these SQL queries:

```
  "SELECT parent AS in_s_3, 'celeste' AS q_person_0 FROM family WHERE kid = 'celeste'",
  "SELECT parent AS stepparentof_parent_5, 'celeste' AS q_person_0 FROM family WHERE kid = 'celeste'",
  "SELECT 'jen' AS stepParentAgg_in_s_4, 'celeste' AS q_person_0 FROM family WHERE parent = 'jen' AND kid = 'celeste'",
```

# NOTES
