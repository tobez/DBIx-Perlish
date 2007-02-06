use warnings;
use strict;
use Test::More tests => 32;
use DBIx::Perlish;
use t::test_utils;

# lone [boolean] tests
test_select_sql {
	tbl->boolvar
} "bool test",
"select * from tbl t01 where t01.boolvar",
[];
test_select_sql {
	!tbl->boolvar
} "not bool test",
"select * from tbl t01 where not t01.boolvar",
[];

# not expr
test_select_sql {
	!tbl->id == 5
} "bool test",
"select * from tbl t01 where not t01.id = 5",
[];

# simple RE (postgres)
test_select_sql {
	tbl->id =~ /^abc/
} "like test",
"select * from tbl t01 where t01.id like 'abc%'",
[];
test_select_sql {
	tbl->id !~ /^abc/
} "not like test",
"select * from tbl t01 where t01.id not like 'abc%'",
[];
test_select_sql {
	tbl->id =~ /^abc/i
} "ilike test",
"select * from tbl t01 where t01.id ilike 'abc%'",
[];
test_select_sql {
	tbl->id !~ /^abc/i
} "not ilike test",
"select * from tbl t01 where t01.id not ilike 'abc%'",
[];

# return
test_select_sql {
	return tbl->name
} "return one",
"select t01.name from tbl t01",
[];
test_select_sql {
	return (tbl->name, tbl->val);
} "return two",
"select t01.name, t01.val from tbl t01",
[];
test_select_sql {
	return (nm => tbl->name);
} "return one, aliased",
"select t01.name as nm from tbl t01",
[];
test_select_sql {
	return (tbl->name, value => tbl->val);
} "return two, second aliased",
"select t01.name, t01.val as value from tbl t01",
[];

# subselects
test_select_sql {
	tbl->id  <-  db_fetch { return t2->some_id };
} "simple IN subselect",
"select * from tbl t01 where t01.id in (select s01_t01.some_id from t2 s01_t01)",
[];
test_select_sql {
	!tbl->id  <-  db_fetch { return t2->some_id };
} "simple NOT IN subselect",
"select * from tbl t01 where t01.id not in (select s01_t01.some_id from t2 s01_t01)",
[];

test_select_sql {
	my $t : tbl;
	db_fetch { $t->id == t2->some_id };
} "simple EXISTS subselect",
"select * from tbl t01 where exists (select * from t2 s01_t01 where t01.id = s01_t01.some_id)",
[];
test_select_sql {
	my $t : tbl;
	!db_fetch { $t->id == t2->some_id };
} "simple NOT EXISTS subselect",
"select * from tbl t01 where not exists (select * from t2 s01_t01 where t01.id = s01_t01.some_id)",
[];

# distinct
test_select_sql {
	return distinct => tbl->id
} "simple SELECT DISTINCT",
"select distinct t01.id from tbl t01",
[];

