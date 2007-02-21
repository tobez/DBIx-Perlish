# $Id$
use warnings;
use strict;
use Test::More tests => 2;
use DBIx::Perlish qw/:all/;
use t::test_utils;

test_select_sql {
	my $x : x;
	my $y : y;
	join $x * $y;
} "unconditional join",
"select * from x t01 cross join y t02",
[];

test_select_sql {
	my $x : x;
	my $y : y;
	join $x < $y => db_fetch { $x-> id > $y-> id };
} "conditional join",
"select * from x t01 left outer join y t02 on t01.id > t02.id",
[];

