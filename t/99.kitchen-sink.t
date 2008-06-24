use warnings;
use strict;
use Test::More tests => 319;
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
test_select_sql {
	tbl->id =~ /^abc_/
} "like underscore",
"select * from tbl t01 where t01.id like 'abc!_%' escape '!'",
[];
test_select_sql {
	tbl->id =~ /^abc%/
} "like percent",
"select * from tbl t01 where t01.id like 'abc!%%' escape '!'",
[];
test_select_sql {
	tbl->id =~ /^abc!/
} "like exclamation",
"select * from tbl t01 where t01.id like 'abc!!%' escape '!'",
[];
test_select_sql {
	tbl->id =~ /^abc!_%/
} "like exclamation underscore percent",
"select * from tbl t01 where t01.id like 'abc!!!_!%%' escape '!'",
[];
test_select_sql {
	tbl->id =~ /^abc!!__%%/
} "like exclamation underscore percent doubled",
"select * from tbl t01 where t01.id like 'abc!!!!!_!_!%!%%' escape '!'",
[];

# lowercasing case-independent like
$main::flavor = "oracle";
test_select_sql {
	tbl->id =~ /AbC/i
} "ilike emulation",
"select * from tbl t01 where lower(t01.id) like '%abc%'",
[];
$main::flavor = "";

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
test_select_sql {
	my $t : tbl;
	return value => "Value";
} "return constant alias",
"select ? as value from tbl t01",
["Value"];
test_select_sql {
	my $t : tbl;
	return $t, value => "Value";
} "return *, then constant alias",
"select t01.*, ? as value from tbl t01",
["Value"];
test_select_sql {
	my $t : tbl;
	return $t->val, value => "Value";
} "return column, then constant alias",
"select t01.val, ? as value from tbl t01",
["Value"];

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

my $someid = 42;
$main::flavor = "oracle";
test_select_sql {
	tbl->id  <- tablefunc($someid);
} "Ora: tablefunc IN subselect",
"select * from tbl t01 where t01.id in (select * from table(tablefunc(?)))",
[42];
test_select_sql {
	!tbl->id  <- tablefunc($someid);
} "Ora: tablefunc NOT IN subselect",
"select * from tbl t01 where t01.id not in (select * from table(tablefunc(?)))",
[42];
$main::flavor = "postgresql";
test_select_sql {
	tbl->id  <- tablefunc($someid);
} "Pg: tablefunc IN subselect",
"select * from tbl t01 where t01.id in (select tablefunc(?))",
[42];
test_select_sql {
	!tbl->id  <- tablefunc($someid);
} "Pg: tablefunc NOT IN subselect",
"select * from tbl t01 where t01.id not in (select tablefunc(?))",
[42];
$main::flavor = "";

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

# limit & offset via a label (with int vars)
my $lim = 10;  my $ofs = 20;
test_select_sql {
	my $t : tbl;
	limit: $lim;
	offset: $ofs;
} "simple limit/offset label with int vars",
"select * from tbl t01 limit 10 offset 20",
[];

# limit & offset via a label (with string vars)
my $s_lim = '10';  my $s_ofs = '20';
test_select_sql {
	my $t : tbl;
	limit: $s_lim;
	offset: $s_ofs;
} "simple limit/offset label with string vars",
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
} "simple order by, descending",
"select * from tbl t01 order by t01.name desc",
[];

# order by several
test_select_sql {
	my $t : tbl;
	order_by: asc => $t->name, desc => $t->age;
} "order by several",
"select * from tbl t01 order by t01.name, t01.age desc",
[];

# sort
test_select_sql {
	my $t : tbl;
	sort $t->name;
} "simple sort",
"select * from tbl t01 order by t01.name",
[];

# sort desc
test_select_sql {
	my $t : tbl;
	sort descending => $t->name;
} "simple sort, descending",
"select * from tbl t01 order by t01.name desc",
[];

# sort several
test_select_sql {
	my $t : tbl;
	sort asc => $t->name, desc => $t->age;
} "sort several",
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
my $self = { table => 'table1', id => 42,
	h1 => {
		v  => 42,
		h2 => { v => 42, h3 => { v => 42 } },
	},
};
my %self = ( table => 'table1', id => 42,
	h1 => {
		v  => 42,
		h2 => { v => 42, h3 => { v => 42 } },
	},
);
our $GLOBAL = 42;
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
	my $t : table1;
	$t->id == $GLOBAL;
} "global scalar",
"select * from table1 t01 where t01.id = ?",
[42];

test_select_sql {
	my $t : table1;
	$t->id == $self->{h1}{v};
} "hashref l2",
"select * from table1 t01 where t01.id = ?",
[42];

test_select_sql {
	my $t : table1;
	$t->id == $self{h1}{v};
} "hashelement l2",
"select * from table1 t01 where t01.id = ?",
[42];

test_select_sql {
	my $t : table1;
	$t->id == $self->{h1}{h2}{v};
} "hashref l3",
"select * from table1 t01 where t01.id = ?",
[42];

test_select_sql {
	my $t : table1;
	$t->id == $self{h1}{h2}{v};
} "hashelement l3",
"select * from table1 t01 where t01.id = ?",
[42];

test_select_sql {
	my $t : table1;
	$t->id == $self->{h1}{h2}{h3}{v};
} "hashref l4",
"select * from table1 t01 where t01.id = ?",
[42];

test_select_sql {
	my $t : table1;
	$t->id == $self{h1}{h2}{h3}{v};
} "hashelement l4",
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
$main::flavor = "oracle";
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
test_select_sql {
	tab->id == `some_seq.nextval`;
} "verbatim `` in select",
"select * from tab t01 where t01.id = some_seq.nextval",
[];
test_update_sql {
	tab->state eq "new";

	tab->id = `some_seq.nextval`;
} "verbatim `` in update",
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

test_select_sql {
	{ return t1->name } except { return t2->name }
} "simple except",
"select t01.name from t1 t01 except select t01.name from t2 t01",
[];

# string concatenation
test_select_sql {
	return "foo-" . tab->name . "-moo";
} "concatenation in return",
"select (? || t01.name || ?) from tab t01",
["foo-","-moo"];

test_select_sql {
	tab->name . "x" eq "abcx";
	return tab->name;
} "concatenation in filter",
"select t01.name from tab t01 where (t01.name || ?) = ?",
["x","abcx"];

test_select_sql {
	my $t : tab;
	return "foo-$t->name-moo";
} "concatenation with interpolation",
"select (? || t01.name || ?) from tab t01",
["foo-", "-moo"];

test_select_sql {
	my $t : tab;
	return "foo-" . $t->firstname . " $t->lastname-moo";
} "concat, interp+normal",
"select (? || t01.firstname || ? || t01.lastname || ?) from tab t01",
["foo-", " ", "-moo"];

test_select_sql {
	my $t : tab;
	return "foo-$t->firstname $t->lastname-moo";
} "concat, interp x 2",
"select (? || t01.firstname || ? || t01.lastname || ?) from tab t01",
["foo-", " ", "-moo"];

test_select_sql {
	my $t : tab;
	return "abc$t->{name}xyz";
} "concat, interp, hash syntax",
"select (? || t01.name || ?) from tab t01",
["abc", "xyz"];

# defined
test_select_sql {
	defined(tab->field);
} "defined",
"select * from tab t01 where t01.field is not null",
[];

test_select_sql {
	defined tab->field;
} "defined",
"select * from tab t01 where t01.field is not null",
[];

test_select_sql {
	!defined(tab->field);
} "!defined",
"select * from tab t01 where t01.field is null",
[];

test_select_sql {
	not defined(tab->field);
} "not defined",
"select * from tab t01 where t01.field is null",
[];

# <- @array
my @ary = (1,2,3);
test_select_sql {
	tab->id  <-  @ary;
} "in array",
"select * from tab t01 where t01.id in (?,?,?)",
[@ary];

# <- @$array
my $ary = [1,2,3];
test_select_sql {
	tab->id  <-  @$ary;
} "in array",
"select * from tab t01 where t01.id in (?,?,?)",
[@$ary];

test_select_sql {
	!tab->id  <-  @ary;
} "in arrayref",
"select * from tab t01 where t01.id not in (?,?,?)",
[@ary];

test_select_sql {
	!tab->id  <-  [1,2,3];
} "in list",
"select * from tab t01 where t01.id not in (1,2,3)",
[];

test_select_sql {
	!tab->id  <-  [1,$self{id},3];
} "in list vals",
"select * from tab t01 where t01.id not in (1,?,3)",
[42];

test_select_sql {
	!tab->id  <-  [1,$self->{id},3];
} "in list vals",
"select * from tab t01 where t01.id not in (1,?,3)",
[42];

# autogrouping
test_select_sql {
	my $t : tab;
	return $t->name, $t->type, count($t->age);
} "autogrouping",
"select t01.name, t01.type, count(t01.age) from tab t01 group by t01.name, t01.type",
[];

test_select_sql {
	my $t : tab;
	return $t->name, $t->type, cnt($t->age);
} "no autogrouping - not an aggregate",
"select t01.name, t01.type, cnt(t01.age) from tab t01",
[];

test_select_sql {
	my $t : tab;
	return $t, count($t->age);
} "no autogrouping - table reference",
"select t01.*, count(t01.age) from tab t01",
[];

test_select_sql {
	my $t : tab;
	return count($t->age);
} "no autogrouping - aggregate only",
"select count(t01.age) from tab t01",
[];

test_select_sql {
	my $t : tab;
	return $t->name, count($t->age);
	group_by: $t->name, $t->type;
} "no autogrouping - explicit group by",
"select t01.name, count(t01.age) from tab t01 group by t01.name, t01.type",
[];

# undef comparisons
test_select_sql {
	tab->age == undef;
} "undef cmp, literal, right, equal",
"select * from tab t01 where t01.age is null",
[];

test_select_sql {
	tab->age != undef;
} "undef cmp, literal, right, not equal",
"select * from tab t01 where t01.age is not null",
[];

test_select_sql {
	undef == tab->age;
} "undef cmp, literal, left, equal",
"select * from tab t01 where t01.age is null",
[];

test_select_sql {
	undef != tab->age;
} "undef cmp, literal, left, not equal",
"select * from tab t01 where t01.age is not null",
[];

my $undef = undef;
test_select_sql {
	tab->age == $undef;
} "undef cmp, scalar, right, equal",
"select * from tab t01 where t01.age is null",
[];

test_select_sql {
	tab->age != $undef;
} "undef cmp, scalar, right, not equal",
"select * from tab t01 where t01.age is not null",
[];

test_select_sql {
	$undef == tab->age;
} "undef cmp, scalar, left, equal",
"select * from tab t01 where t01.age is null",
[];

test_select_sql {
	$undef != tab->age;
} "undef cmp, scalar, left, not equal",
"select * from tab t01 where t01.age is not null",
[];

# RE with vars (postgres)
my $re = "abc";
test_select_sql {
	tbl->id =~ /^$re/
} "like test, scalar",
"select * from tbl t01 where t01.id like 'abc%'",
[];

$re = { re => "abc" };
test_select_sql {
	tbl->id =~ /^$re->{re}/
} "like test, hashref",
"select * from tbl t01 where t01.id like 'abc%'",
[];

# varcols
my $col = "col1";
test_select_sql {
	tab->$col == 42;
} "varcol1",
"select * from tab t01 where t01.col1 = 42",
[];
test_select_sql {
	my $t : tab;
	$t->$col == 42;
} "varcol2",
"select * from tab t01 where t01.col1 = 42",
[];

test_select_sql {
	my $t : tab;
} "select with exec is no mistake",
"select * from tab t01",
[];

# update selfmod
test_update_sql {
	tab->col++;
	exec;
} "postinc",
"update tab set col = col + 1",
[];
test_update_sql {
	++tab->col;
	exec;
} "preinc",
"update tab set col = col + 1",
[];
test_update_sql {
	tab->col--;
	exec;
} "postdec",
"update tab set col = col - 1",
[];
test_update_sql {
	--tab->col;
	exec;
} "predec",
"update tab set col = col - 1",
[];

test_update_sql {
	tab->col += 2;
	exec;
} "+= 2",
"update tab set col = col + 2",
[];
test_update_sql {
	tab->col -= 2;
	exec;
} "-= 2",
"update tab set col = col - 2",
[];
test_update_sql {
	tab->col *= 2;
	exec;
} "*= 2",
"update tab set col = col * 2",
[];
test_update_sql {
	tab->col /= 2;
	exec;
} "/= 2",
"update tab set col = col / 2",
[];
test_update_sql {
	tab->col .= "2";
	exec;
} ".= 2",
"update tab set col = col || ?",
["2"];

test_update_sql {
	tab->col += $self->{id} + 2;
	exec;
} "+= complex",
"update tab set col = col + (? + 2)",
[42];
test_update_sql {
	tab->col -= $self->{id} + 2;
	exec;
} "-= complex",
"update tab set col = col - (? + 2)",
[42];
test_update_sql {
	tab->col *= $self->{id} + 2;
	exec;
} "*= complex",
"update tab set col = col * (? + 2)",
[42];
test_update_sql {
	tab->col /= $self->{id} + 2;
	exec;
} "/= complex",
"update tab set col = col / (? + 2)",
[42];

my $h = { col1 => 42, col2 => 666 };
my %h = ( col1 => 42, col2 => 666 );
test_update_sql {
	my $t : tab;
 	$t = {%h};
	exec;
} "hash assignment 1",
"update tab set col1 = ?, col2 = ?",
[42,666];
test_update_sql {
	my $t : tab;
 	$t = {%$h};
	exec;
} "hashref assignment 1",
"update tab set col1 = ?, col2 = ?",
[42,666];
test_update_sql {
	my $t : tab;
 	$t = {%h, foobar => 2};
	exec;
} "hash assignment 2",
"update tab set col1 = ?, col2 = ?, foobar = 2",
[42,666];
test_update_sql {
	my $t : tab;
 	$t = {%$h, foobar => 2};
	exec;
} "hashref assignment 2",
"update tab set col1 = ?, col2 = ?, foobar = 2",
[42,666];

package DBI::db;
package good_dbh;
use vars '@ISA';
@ISA="DBI::db";
package main;

my $bad_dbh = bless {}, 'something';
eval { DBIx::Perlish::init($bad_dbh) };
like($@||"", qr/Invalid database handle supplied/, "init with bad dbh");
eval { DBIx::Perlish->new(dbh => $bad_dbh) };
like($@||"", qr/Invalid database handle supplied/, "new with bad dbh");

my $good_dbh = bless {}, 'good_dbh';
eval { DBIx::Perlish::init($good_dbh) };
is($@||"", "", "init with inherited dbh");
eval { DBIx::Perlish->new(dbh => $good_dbh) };
is($@||"", "", "new with inherited dbh");

# union with the same var check
test_select_sql {
	{ t1->name == $vart } union { t2->name == $vart }
} "union with the same var",
"select * from t1 t01 where t01.name = ? union select * from t2 t01 where t01.name = ?",
["table1","table1"];

# multi-union
test_select_sql {
	{ return t1->name } union { return t2->name } union { return t3->name }
} "multi-union",
"select t01.name from t1 t01 union select t01.name from t2 t01 union select t01.name from t3 t01",
[];

# conditional returns
test_select_sql {
	my $a : tab;
	return $a->smth, $a->with_id if $self->{id};
	return $a->smth              unless $self->{id};
} "conditional return, if true",
"select t01.smth, t01.with_id from tab t01",
[];
test_select_sql {
	my $a : tab;
	return $a->smth, $a->with_id if $self->{noid};
	return $a->smth              unless $self->{noid};
} "conditional return, unless false",
"select t01.smth from tab t01",
[];

# logical ops
test_select_sql {
	my $a : tab;
	$a->x == 1 && $a->y == 2;
} "explicit simple AND",
"select * from tab t01 where (t01.x = 1) and (t01.y = 2)",
[];
test_select_sql {
	my $a : tab;
	$a->x == 1 || $a->y == 2;
} "explicit simple OR",
"select * from tab t01 where ((t01.x = 1) or (t01.y = 2))",
[];
test_select_sql {
	my $a : tab;
	$a->x == 1 && $a->y == 2
	or
	$a->x == 3 && $a->y == 4
} "explicit ANDs inside OR",
"select * from tab t01 where (((t01.x = 1) and (t01.y = 2)) or ((t01.x = 3) and (t01.y = 4)))",
[];
test_select_sql {
	my $a : tab;
	$a->x == 1 || $a->y == 2
	and
	$a->x == 3 || $a->y == 4
} "explicit ORs inside AND",
"select * from tab t01 where ((t01.x = 1) or (t01.y = 2)) and ((t01.x = 3) or (t01.y = 4))",
[];
test_select_sql {
	my $a : tab;
	$a->x == 1 || $a->y == 2;
	$a->z == 3;
} "explicit simple OR, implicit AND",
"select * from tab t01 where ((t01.x = 1) or (t01.y = 2)) and t01.z = 3",
[];
