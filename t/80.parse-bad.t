use warnings;
use strict;
use Test::More tests => 25;
use DBIx::Perlish;
use t::test_utils;

our $testour;
my $testmy;

test_bad_select {} "empty select", qr/no tables specified in select/;
test_bad_update {} "empty update", qr/no tables specified in update/;
test_bad_delete {} "empty delete", qr/no tables specified in delete/;

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
