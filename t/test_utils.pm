package t::test_utils;
package main;

sub format_value { $_[0] }

sub test_sql
{
	my ($sub, $tname, $exp_sql, $exp_v, $dbop) = @_;
	my ($sql, $v) = DBIx::Perlish::gen_sql($sub, $dbop, flavor => 'postgresql');
	is($sql, $exp_sql, "$tname: SQL");
	is(+@$v, @$exp_v, "$tname: number of bound values");
	for (my $i = 0; $i < @$v; $i++) {
		my $bv = format_value($exp_v->[$i]);
		is($v->[$i], $exp_v->[$i], "$tname: bound value is '$bv'");
	}
}

sub test_select_sql (&$$$) {
	test_sql(@_,"select");
}

1;
