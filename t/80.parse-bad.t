use warnings;
use strict;
use Test::More tests => 41;
use DBIx::Perlish;
use t::test_utils;

our $testour;
my $testmy;

test_bad_select {} "empty select", qr/no tables specified in select/;
test_bad_update {} "empty update", qr/no tables specified in update/;
test_bad_delete {} "empty delete", qr/no tables specified in delete/;

test_bad_update { tbl->id == 42 } "nothing to update", qr/nothing to update/;

test_bad_select {
	table: 1;
} "table label1", qr/label .*? must be followed by an assignment/;
test_bad_select {
	table: $testmy = 1;
} "table label2", qr/label .*? must be followed by a lexical variable declaration/;

test_bad_select {
	limit: "hello";
} "limit label1", qr/label .*? must be followed by an integer/;
test_bad_select {
	my $t : tab;
	limit: $t;
} "limit label2", qr/cannot use table variable after/;

test_bad_select {
	offset: "hello";
} "offset label1", qr/label .*? must be followed by an integer/;
test_bad_select {
	my $t : tab;
	offset: $t;
} "offset label2", qr/cannot use table variable after/;

test_bad_select {
	label: "blah";
} "bad label1", qr/label .*? is not understood/;

test_bad_select {
	last unless $testmy;
} "bad range1", qr/range operator expected/;

test_bad_update {
	last;
} "last in update", qr/there should be no "last" statements in update's query sub/;
test_bad_delete {
	last;
} "last in update", qr/there should be no "last" statements in delete's query sub/;
test_bad_update {
	last unless 1..2;
} "last unless in update", qr/there should be no "last" statements in update's query sub/;
test_bad_delete {
	last unless 1..2;
} "last unless in update", qr/there should be no "last" statements in delete's query sub/;

# this should be implemented
test_bad_select { t->id % 5 } "no modulo", qr/unsupported binop modulo/;

test_bad_update {
	$testmy = { x => 1, y => 2};
} "bad my table in update", qr/cannot get a table to update/;
test_bad_update {
	$testour = { x => 1, y => 2};
} "bad our table in update", qr/cannot get a table to update/;

test_bad_select {
	t->id = 1;
} "assignment in select", qr/assignments are not understood in select's query sub/;
test_bad_delete {
	t->id = 1;
} "assignment in delete", qr/assignments are not understood in delete's query sub/;

test_bad_select {
	my $t : t1;
	$t->id  <-  db_fetch { my $tt : t2 };
} "subselect returns too much 1", qr/subselect query sub must return exactly one value/;
test_bad_select {
	my $t : t1;
	$t->id  <-  db_fetch {
		my $t2 : t2; my $t3 : t3;
		$t2->id == $t3->id;
		return $t2;
	};
} "subselect returns too much 2", qr/subselect query sub must return exactly one value/;

test_bad_select {
	table: my $t = $testmy * $testour;
} "bad simple term 1", qr/cannot reconstruct simple term from operation/;
test_bad_select {
	last unless $testmy * $testour..3;
} "bad simple term 2", qr/cannot reconstruct simple term from operation/;
test_bad_select {
	last unless 3..($testmy * $testour);
} "bad simple term 3", qr/cannot reconstruct simple term from operation/;

test_bad_select { join 1,2,3,4; } "bad join 1", qr/not a valid join/;
test_bad_select { join 1,2; } "bad join 2", qr/not a valid join/;
test_bad_select { join 1,2,3; } "bad join 3", qr/not a valid join/;
test_bad_select { join $testmy - 2; } "bad join 4", qr/not a valid join.*x is expected/;
test_bad_select { join $testmy - 2, 1; } "bad join 5", qr/not a valid join.*> is expected/;
test_bad_select { join $testmy + 2, 1; } "bad join 6", qr/not a valid join/;
test_bad_select { join 2 * $testmy; } "bad join 7", qr/first argument join/;
test_bad_select { my $t : tab; join $t * 2; } "bad join 8", qr/second argument join/;
test_bad_select { my $t1 : t1; my $t2 : t2; join $t1 * $t2, xx(); } "bad join 9", qr/not a db_fetch/;
test_bad_select { my $t1 : t1; my $t2 : t2;
	join $t1 * $t2 => db_fetch { my $t3 : t3 }
} "bad join 10", qr/anything but conditional/;
test_bad_select { my $t1 : t1; my $t2 : t2;
	join $t1 * $t2 => db_fetch { return $t1->x }
} "bad join 11", qr/anything but conditional/;
test_bad_select { my $t1 : t1; my $t2 : t2;
	join $t1 * $t2 => db_fetch { group_by: $t1->name }
} "bad join 12", qr/anything but conditional/;
test_bad_select { my $t1 : t1; my $t2 : t2;
	join $t1 * $t2 => db_fetch { order: $t1->name }
} "bad join 13", qr/anything but conditional/;
test_bad_select { my $t1 : t1; my $t2 : t2;
	join $t1 + $t2 => db_fetch { }
} "bad join 14", qr/at least one conditional/;
test_bad_select { my $t1 : t1; my $t2 : t2;
	join $t1 * $t2 <= db_fetch {}, 42;
} "bad join 15", qr/not a valid join/;
