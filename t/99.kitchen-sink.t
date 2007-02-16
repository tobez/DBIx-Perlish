use warnings;
use strict;
use Test::More tests => 94;
use DBIx::Perlish qw/:all/;
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

# distinct via a label
test_select_sql {
	DISTINCT: return tbl->id
} "simple SELECT DISTINCT via a label",
"select distinct t01.id from tbl t01",
[];

# limit via a label
test_select_sql {
	my $t : tbl;
	limit: 5;
} "simple limit label",
"select * from tbl t01 limit 5",
[];

# offset via a label
test_select_sql {
	my $t : tbl;
	offset: 42;
} "simple offset label",
"select * from tbl t01 offset 42",
[];

# limit & offset via a label
my $lim = 10;  my $ofs = 20;
test_select_sql {
	my $t : tbl;
	limit: $lim;
	offset: $ofs;
} "simple limit/offset label with closure",
"select * from tbl t01 limit 10 offset 20",
[];

# order by
test_select_sql {
	my $t : tbl;
	order_by: $t->name;
} "simple order by",
"select * from tbl t01 order by t01.name",
[];

# order by desc
test_select_sql {
	my $t : tbl;
	order_by: descending => $t->name;
} "simple order by",
"select * from tbl t01 order by t01.name desc",
[];

# order by several
test_select_sql {
	my $t : tbl;
	order_by: asc => $t->name, desc => $t->age;
} "simple order by",
"select * from tbl t01 order by t01.name, t01.age desc",
[];

# group by
test_select_sql {
	my $t : tbl;
	group_by: $t->type;
} "simple order by",
"select * from tbl t01 group by t01.type",
[];

#  return t.*
test_select_sql {
	my $t1 : table1;
	my $t2 : table2;
	$t1->id == $t2->table1_id;
	return $t1, $t2->name;
} "select t.*",
"select t01.*, t02.name from table1 t01, table2 t02 where t01.id = t02.table1_id",
[];

my $vart = 'table1';
my $self = { table => 'table1', id => 42 };
my %self = ( table => 'table1', id => 42 );
test_select_sql {
	table: my $t1 = $vart;
	my $t2 : table2;
	$t1->id == $t2->table1_id;
	return $t1, $t2->name;
} "vartable label 1",
"select t01.*, t02.name from table1 t01, table2 t02 where t01.id = t02.table1_id",
[];

test_select_sql {
	table: my $t1 = $self{table};
	my $t2 : table2;
	$t1->id == $t2->table1_id;
	return $t1, $t2->name;
} "vartable label 2",
"select t01.*, t02.name from table1 t01, table2 t02 where t01.id = t02.table1_id",
[];

test_select_sql {
	table: my $t1 = $self->{table};
	my $t2 : table2;
	$t1->id == $t2->table1_id;
	return $t1, $t2->name;
} "vartable label 3",
"select t01.*, t02.name from table1 t01, table2 t02 where t01.id = t02.table1_id",
[];

test_select_sql {
	my $t : table1;
	$t->id == $self->{id};
} "hashref",
"select * from table1 t01 where t01.id = ?",
[42];

test_select_sql {
	my $t : table1;
	$t->id == $self{id};
} "hashelement",
"select * from table1 t01 where t01.id = ?",
[42];

test_select_sql {
	my $t : table = $self->{table};
} "vartable attribute",
"select * from table1 t01",
[];

# conditional post-if
my $type = "ICBM";
test_select_sql {
	my $t : products;
	$t->type eq $type if $type;
} "conditional post-if, true",
"select * from products t01 where t01.type = ?",
["ICBM"];
$type = "";
test_select_sql {
	my $t : products;
	$t->type eq $type if $type;
} "conditional post-if, false",
"select * from products t01",
[];

# special handling of sysdate
test_select_sql {
	tab->foo == sysdate();
} "sysdate() is not special",
"select * from tab t01 where t01.foo = sysdate()",
[];
$main::flavor = "Oracle";
test_select_sql {
	tab->foo == sysdate();
} "sysdate() is special",
"select * from tab t01 where t01.foo = sysdate",
[];
$main::flavor = "";

# verbatim
test_select_sql {
	tab->id == sql "some_seq.nextval";
} "verbatim in select",
"select * from tab t01 where t01.id = some_seq.nextval",
[];
test_update_sql {
	tab->state eq "new";

	tab->id = sql "some_seq.nextval";
} "verbatim in update",
"update tab set id = some_seq.nextval where state = ?",
['new'];

# expression return
test_select_sql {
	return tab->n1 - tab->n2
} "expression return 1",
"select (t01.n1 - t01.n2) from tab t01",
[];
test_select_sql {
	return diff => abs(tab->n1 - tab->n2)
} "expression return 2",
"select abs((t01.n1 - t01.n2)) as diff from tab t01",
[];

test_select_sql {
	{ return t1->name } union { return t2->name }
} "simple union",
"select t01.name from t1 t01 union select t01.name from t2 t01",
[];

test_select_sql {
	{ return t1->name } intersect { return t2->name }
} "simple intersect",
"select t01.name from t1 t01 intersect select t01.name from t2 t01",
[];

# string concatenation
test_select_sql {
	return "foo-" . tab->name . "-moo";
} "concatenation in return",
"select ((? || t01.name) || ?) from tab t01",
["foo-","-moo"];

test_select_sql {
	tab->name . "x" eq "abcx";
	return tab->name;
} "concatenation in filter",
"select t01.name from tab t01 where (t01.name || ?) = ?",
["x","abcx"];
