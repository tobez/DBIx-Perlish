# $Id$
use warnings;
use strict;
use Test::More tests => 35;
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
	join $x * $y => db_fetch {};
} "unconditional join 2",
"select * from x t01 cross join y t02",
[];

test_select_sql {
	my $x : x;
	my $y : y;
	join $x < $y => db_fetch { $x-> id > $y-> id };
} "conditional join",
"select * from x t01 left outer join y t02 on t01.id > t02.id",
[];

test_select_sql {
	my $x : x;
	my $y : y;
	my $z : z;
	$y->id == $z->y_id;
	join $x < $y => db_fetch { $x-> id > $y-> id };
	my $w : w;
	$x->id == $w->x_id;
} "funny join 1",
"select * from x t01 left outer join y t02 on t01.id > t02.id, z t03, w t04 where t02.id = t03.y_id and t01.id = t04.x_id",
[];

test_select_sql {
	my $w : w;
	my $z : z;
	my $x : x;
	my $y : y;
	$y->id == $z->y_id;
	join $x < $y => db_fetch { $x-> id > $y-> id };
	$x->id == $w->x_id;
} "funny join 2",
"select * from w t01, z t02, x t03 left outer join y t04 on t03.id > t04.id where t04.id = t02.y_id and t03.id = t01.x_id",
[];

test_select_sql {
	my $w : w;
	my $x : x;
	my $y : y;
	my $z : z;
	$y->id == $z->y_id;
	join $x < $y => db_fetch { $x-> id > $y-> id };
	$x->id == $w->x_id;
} "funny join 3",
"select * from w t01, x t02 left outer join y t03 on t02.id > t03.id, z t04 where t03.id = t04.y_id and t02.id = t01.x_id",
[];

test_select_sql {
	my $x : x;
	my $y : y;
	join $x * $y <= db_fetch {};
} "inverse join",
"select * from x t01 cross join y t02",
[];

test_select_sql {
	my $w : w;
	my $x : x;
	my $y : y;
	my $z : z;
	$y->id == $z->y_id;
	join $x + $y <= db_fetch { $x-> id > $y-> id };
	$x->id == $w->x_id;
} "inverse join 2",
"select * from w t01, x t02 full outer join y t03 on t02.id > t03.id, z t04 where t03.id = t04.y_id and t02.id = t01.x_id",
[];

test_select_sql {
	my $x : x;
	my $y : y;
	join $x < $y => db_fetch { $x->blah == "hello" };
} "join with bound values",
"select * from x t01 left outer join y t02 on t01.blah = ?",
["hello"];

test_select_sql {
	my $x : x;
	my $y : y;
	$y->yy == "y";
	join $x < $y => db_fetch { $x->blah == "hello" };
} "join with bound values",
"select * from x t01 left outer join y t02 on t01.blah = ? where t02.yy = ?",
["hello", "y"];

test_select_sql {
	my $x : x;
	my $y : y;
	$y->yy == "y";
	join $x < $y => db_fetch { $x->blah == "hello" };
	$x->xx == "x";
} "join with bound values 2",
"select * from x t01 left outer join y t02 on t01.blah = ? where t02.yy = ? and t01.xx = ?",
["hello", "y", "x"];

test_select_sql {
	my $w : w;
	$w->w1 == "w1";
	$w->id  <-  db_fetch {
		my $x : x;
		my $y : y;
		$y->yy == "y";
		join $x < $y => db_fetch { $x->blah == "hello" };
		$x->xx == "x";
		return $x->toret;
	};
	$w->w2 == "w2";
} "complex join with bound values",
"select * from w t01 where t01.w1 = ? and t01.id in (select s01_t01.toret from x s01_t01 left outer join y s01_t02 on s01_t01.blah = ? where s01_t02.yy = ? and s01_t01.xx = ?) and t01.w2 = ?",
["w1", "hello", "y", "x", "w2"];
