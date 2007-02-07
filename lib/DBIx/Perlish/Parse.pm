package DBIx::Perlish::Parse;
# $Id$
use 5.008;
use warnings;
use strict;

our $DEVEL;

use B;
use Data::Dumper;
use Carp;

sub bailout
{
	my ($S, @rest) = @_;
	if ($DEVEL) {
		confess @rest;
	} else {
		my $args = join '', @rest;
		$args = "Something's wrong" unless $args;
		my $file = $S->{file};
		my $line = $S->{line};
		$args .= " at $file line $line.\n"
			unless substr($args, length($args) -1) eq "\n";
		CORE::die($args);
	}
}

# "is" checks

sub is
{
	my ($optype, $op, $name) = @_;
	return 0 unless ref($op) eq $optype;
	return 1 unless $name;
	return $op->name eq $name;
}

sub gen_is
{
	my ($optype) = @_;
	my $pkg = "B::" . uc($optype);
	eval qq[ sub is_$optype { is("$pkg", \@_) } ];
}

gen_is("op");
gen_is("cop");
gen_is("unop");
gen_is("listop");
gen_is("svop");
gen_is("null");
gen_is("binop");
gen_is("logop");
gen_is("padop");

sub is_const
{
	my ($S, $op) = @_;
	return () unless is_svop($op, "const");
	my $sv = $op->sv;
	if (!$$sv) {
		$sv = $S->{padlist}->[1]->ARRAYelt($op->targ);
	}
	if (wantarray) {
		return (${$sv->object_2svref}, $sv);
	} else {
		return ${$sv->object_2svref};
	}
}

# "want" helpers

sub gen_want
{
	my ($optype, $return) = @_;
	if (!$return) {
		$return = '$op';
	} elsif ($return =~ /^\w+$/) {
		$return = '$op->' . $return;
	}
	eval <<EOF;
	sub want_$optype {
		my (\$S, \$op, \$n) = \@_;
		unless (is_$optype(\$op, \$n)) {
			bailout \$S, "want $optype" unless \$n;
			bailout \$S, "want $optype \$n";
		}
		$return;
	}
EOF
}

gen_want("op");
gen_want("unop", "first");
gen_want("listop", 'get_all_children($op)');
gen_want("svop", "sv");
gen_want("null");

sub want_const
{
	my ($S, $op) = @_;
	my $sv = want_svop($S, $op, "const");
	if (!$$sv) {
		$sv = $S->{padlist}->[1]->ARRAYelt($op->targ);
	}
	${$sv->object_2svref};
}

sub want_method
{
	my ($S, $op) = @_;
	my $sv = want_svop($S, $op, "method_named");
	if (!$$sv) {
		$sv = $S->{padlist}->[1]->ARRAYelt($op->targ);
	}
	${$sv->object_2svref};
}

# getters

sub get_all_children
{
	my ($op) = @_;
	my $c = $op->children;
	my @op;
	return @op unless $c;
	push @op, $op->first;
	while (--$c) {
		push @op, $op[-1]->sibling;
	}
	@op;
}

sub get_table
{
	my ($S, $op) = @_;
	want_const($S, $op);
}

sub padname
{
	my ($S, $op) = @_;

	my $padname = $S->{padlist}->[0]->ARRAYelt($op->targ);
	if ($padname && ref($padname) ne "B::SPECIAL") {
		return "my " . $padname->PVX;
	} else {
		return "my #" . $op->targ;
	}
}

sub get_var
{
	my ($S, $op) = @_;
	if (is_op($op, "padsv")) {
		return padname($S, $op);
	} elsif (is_unop($op, "null")) {
		$op = $op->first;
		want_svop($S, $op, "gvsv");
		return "*" . $op->gv->NAME;
	} else {
	# XXX
		print "$op\n";
		print "type: ", $op->type, "\n";
		print "name: ", $op->name, "\n";
		print "desc: ", $op->desc, "\n";
		print "targ: ", $op->targ, "\n";
		bailout $S, "cannot get var";
	}
}

sub find_aliased_tab
{
	my ($S, $op) = @_;
	my $var = padname($S, $op);
	my $ss = $S;
	while ($ss) {
		my $tab;
		if ($ss->{operation} eq "select") {
			$tab = $ss->{var_alias}{$var};
		} else {
			$tab = $ss->{vars}{$var};
		}
		return $tab if $tab;
		$ss = $ss->{gen_args}->{prev_S};
	}
	return "";
}

sub get_tab_field
{
	my ($S, $unop, $expect_lvalue) = @_;
	my $op = want_unop($S, $unop, "entersub");
	want_op($S, $op, "pushmark");
	$op = $op->sibling;
	my $tab = is_const($S, $op);
	if ($tab) {
		$tab = new_tab($S, $tab);
	} elsif (is_op($op, "padsv")) {
		$tab = find_aliased_tab($S, $op);
	}
	unless ($tab) {
		bailout $S, "cannot get a table";
	}
	$op = $op->sibling;
	my $field = want_method($S, $op);
	$op = $op->sibling;
	if ($expect_lvalue) {
		want_unop($S, $op, "rv2cv");
		$op = $op->sibling;
	}
	want_null($S, $op);
	($tab, $field);
}

# helpers

sub maybe_one_table_only
{
	my ($S) = @_;
	return if $S->{operation} eq "select";
	if ($S->{tabs} && keys %{$S->{tabs}} or $S->{vars} && keys %{$S->{vars}}) {
		bailout "a $S->{operation}'s query sub can only refer to a single table";
	}
}

sub new_tab
{
	my ($S, $tab) = @_;
	unless ($S->{tabs}{$tab}) {
		maybe_one_table_only($S);
		$S->{tabs}{$tab} = 1;
		$S->{tab_alias}{$tab} = $S->{alias};
		$S->{alias}++;
	}
	$S->{tab_alias}{$tab};
}

sub new_var
{
	my ($S, $var, $tab) = @_;
	maybe_one_table_only($S);
	bailout $S, "cannot reuse $var for table $tab, it's already used by $S->{vars}{$var}"
		if $S->{vars}{$var};
	$S->{vars}{$var} = $tab;
	$S->{var_alias}{$var} = $S->{alias};
	$S->{alias}++;
}

# parsers

sub try_parse_attr_assignment
{
	my ($S, $op) = @_;
	return unless is_unop($op, "entersub");
	$op = want_unop($S, $op);
	return unless is_op($op, "pushmark");
	$op = $op->sibling;
	my $c = is_const($S, $op);
	return unless $c && $c eq "attributes";
	$op = $op->sibling;
	return unless is_const($S, $op);
	$op = $op->sibling;
	return unless is_unop($op, "srefgen");
	my $op1 = want_unop($S, $op);
	$op1 = want_unop($S, $op1) if is_unop($op1, "null");
	return unless is_op($op1, "padsv");
	my $varn = padname($S, $op1);
	$op = $op->sibling;
	my $attr = is_const($S, $op);
	return unless $attr;
	$op = $op->sibling;
	return unless is_svop($op, "method_named");
	return unless want_method($S, $op, "import");
	new_var($S, $varn, $attr);
	return $attr;
}

sub parse_list
{
	my ($S, $op) = @_;
	my @op = get_all_children($op);
	for $op (@op) {
		parse_op($S, $op);
	}
}

sub parse_return
{
	my ($S, $op) = @_;
	my @op = get_all_children($op);
	bailout "there should be no \"return\" statements in $S->{operation}'s query sub"
		unless $S->{operation} eq "select";
	bailout "there should be at most one return statement" if $S->{returns};
	$S->{returns} = [];
	my $last_alias;
	for $op (@op) {
		my %rv = parse_return_value($S, $op);
		if (exists $rv{field}) {
			if (defined $last_alias) {
				push @{$S->{returns}}, "$rv{field} as $last_alias";
				undef $last_alias;
			} else {
				push @{$S->{returns}}, $rv{field};
			}
		} elsif (exists $rv{alias}) {
			bailout "bad alias name \"$rv{alias}\""
				unless $rv{alias} =~ /^\w+$/;
			bailout "cannot alias an alias"
				if defined $last_alias;
			if (lc $rv{alias} eq "distinct") {
				bailout "\"$rv{alias}\" is not a valid alias name" if @{$S->{returns}};
				$S->{distinct}++;
				next;
			}
			$last_alias = $rv{alias};
		}
	}
}

sub parse_return_value
{
	my ($S, $op) = @_;

	if (is_unop($op, "entersub")) {
		my ($t, $f) = get_tab_field($S, $op);
		return field => "$t.$f";
	} elsif (my $const = is_const($S, $op)) {
		return alias => $const;
	} elsif (is_op($op, "pushmark")) {
		return ();
	} else {
		bailout "error parsing return values";
	}
}

sub parse_term
{
	my ($S, $op, %p) = @_;

	if (is_unop($op, "entersub")) {
		my $funcall = try_funcall($S, $op);
		return $funcall if $funcall;
		my ($t, $f) = get_tab_field($S, $op);
		if ($S->{operation} eq "delete" || $S->{operation} eq "update") {
			return $f;
		} else {
			return "$t.$f";
		}
	} elsif (is_unop($op, "lc")) {
		my $term = parse_term($S, $op->first);
		return "lower($term)";
	} elsif (is_unop($op, "uc")) {
		my $term = parse_term($S, $op->first);
		return "upper($term)";
	} elsif (is_unop($op, "null")) {
		return parse_term($S, $op->first, %p);
	} elsif (is_unop($op, "not")) {
		my $subop = $op-> first;
		if (ref($subop) eq "B::PMOP" && $subop->name eq "match") {
			return parse_regex( $S, $subop, 1);
		} else {
			my $term = parse_term($S, $subop);
			if ($p{not_after}) {
				return "$term not";
			} else {
				return "not $term";
			}
		}
	} elsif (is_binop($op)) {
		my $expr = parse_expr($S, $op);
		return "($expr)";
	} elsif (is_logop($op, "or")) {
		my $or = parse_or($S, $op);
		bailout $S, "looks like a limiting range inside an expression\n"
			unless $or;
		return "($or)";
	} elsif (my ($const,$sv) = is_const($S, $op)) {
		if (ref $sv eq "B::IV" || ref $sv eq "B::NV") {
			# This is surely a number, so we can
			# safely inline it in the SQL.
			return $const;
		} else {
			# This will probably be represented by a string,
			# we'll let DBI to handle the quoting of a bound
			# value.
			push @{$S->{values}}, $const;
			return "?";
		}
	} elsif (is_op($op, "padsv")) {
		if (find_aliased_tab($S, $op)) {
			bailout $S, "cannot use table variable as a term";
		}
		my $vv = $S->{padlist}->[1]->ARRAYelt($op->targ)->object_2svref;
		push @{$S->{values}}, $$vv;
		return "?";
	} else {
		bailout $S, "cannot reconstruct term from operation \"",
				$op->name, '"';
	}
}

sub parse_simple_term
{
	my ($S, $op) = @_;
	if (my $const = is_const($S, $op)) {
		return $const;
	} elsif (is_op($op, "padsv")) {
		if (find_aliased_tab($S, $op)) {
			bailout $S, "cannot use table variable as a simple term";
		}
		my $vv = $S->{padlist}->[1]->ARRAYelt($op->targ)->object_2svref;
		return $$vv;
	} else {
		bailout $S, "cannot reconstruct simple term from operation \"",
				$op->name, '"';
	}
}

sub get_gv
{
	my ($S, $op) = @_;

	my ($gv_on_pad, $gv_idx);
	if (is_svop($op, "gv")) {
		$gv_idx = $op->targ;
	} elsif (is_padop($op, "gv")) {
		$gv_idx = $op->padix;
		$gv_on_pad = 1;
	} else {
		return;
	}
	return unless is_null($op->sibling);

	my $gv = $gv_on_pad ? "" : $op->sv;
	if (!$gv || !$$gv) {
		$gv = $S->{padlist}->[1]->ARRAYelt($gv_idx);
	}
	return unless ref $gv eq "B::GV";
	$gv;
}

sub try_parse_subselect
{
	my ($S, $sop) = @_;
	my $sub = $sop->last->first;
	return unless is_unop($sub, "entersub");
	$sub = $sub->first if is_unop($sub->first, "null");
	return unless is_op($sub->first, "pushmark");

	my $rg = $sub->first->sibling;
	return if is_null($rg);
	my $dbfetch = $rg->sibling;
	return if is_null($dbfetch);
	return unless is_null($dbfetch->sibling);

	return unless is_unop($rg, "refgen");
	$rg = $rg->first if is_unop($rg->first, "null");
	return unless is_op($rg->first, "pushmark");
	my $codeop = $rg->first->sibling;
	return unless is_svop($codeop, "anoncode");

	$dbfetch = $dbfetch->first if is_unop($dbfetch->first, "null");
	$dbfetch = $dbfetch->first;
	my $gv = get_gv($S, $dbfetch);
	return unless $gv;
	return unless $gv->NAME eq "db_fetch";

	my $sql = handle_subselect($S, $codeop);

	my $left = parse_term($S, $sop->first, not_after => 1);
	return "$left in ($sql)";
}

sub handle_subselect
{
	my ($S, $codeop, %p) = @_;

	my $cv = $codeop->sv;
	if (!$$cv) {
		$cv = $S->{padlist}->[1]->ARRAYelt($codeop->targ);
	}
	my $subref = $cv->object_2svref;

	my %gen_args = %{$S->{gen_args}};
	$gen_args{prev_S} = $S;
	if ($gen_args{prefix}) {
		$gen_args{prefix} = "$gen_args{prefix}_$S->{subselect}";
	} else {
		$gen_args{prefix} = $S->{subselect};
	}
	$S->{subselect}++;
	my ($sql, $vals, $nret) = DBIx::Perlish::gen_sql($subref, "select", %gen_args);
	if ($nret != 1 && !$p{returns_dont_care}) {
		bailout $S, "subselect query sub must return exactly one value\n";
	}

	push @{$S->{values}}, @$vals;
	return $sql;
}

sub parse_assign
{
	my ($S, $op) = @_;

	bailout $S, "assignments are no understood in $S->{operation}'s query sub"
		unless $S->{operation} eq "update";
	if (is_unop($op->first, "srefgen")) {
		parse_multi_assign($S, $op);
	} else {
		parse_simple_assign($S, $op);
	}
}

sub parse_simple_assign
{
	my ($S, $op) = @_;

	my ($tab, $f) = get_tab_field($S, $op->last, "lvalue");
	my $saved_values = $S->{values};
	$S->{values} = [];
	my $set = parse_term($S, $op->first);
	push @{$S->{set_values}}, @{$S->{values}};
	$S->{values} = $saved_values;
	push @{$S->{sets}}, "$f = $set";
}

sub callarg
{
	my ($S, $op) = @_;
	$op = $op->first if is_unop($op, "null");
	return () if is_op($op, "pushmark");
	return $op;
}

sub try_funcall
{
	my ($S, $op) = @_;
	my @args;
	if (is_unop($op, "entersub")) {
		$op = $op->first;
		$op = $op->first if is_unop($op, "null");
		while (1) {
			last if is_null($op);
			push @args, callarg($S, $op);
			$op = $op->sibling;
		}
		return unless @args;
		$op = pop @args;
		return unless is_svop($op, "gv") || is_padop($op, "gv");
		my $gv = get_gv($S, $op);
		return unless $gv;
		my $func = $gv->NAME;
		if ($func eq "db_fetch") {
			return unless @args == 1;
			my $rg = $args[0];
			return unless is_unop($rg, "refgen");
			$rg = $rg->first if is_unop($rg->first, "null");
			return unless is_op($rg->first, "pushmark");
			my $codeop = $rg->first->sibling;
			return unless is_svop($codeop, "anoncode");
			my $sql = handle_subselect($S, $codeop, returns_dont_care => 1);
			return "exists ($sql)";
		}

		my @terms = map { parse_term($S, $_) } @args;
		return "$func(" . join(", ", @terms) . ")";
	}
}

sub parse_multi_assign
{
	my ($S, $op) = @_;

	my $hashop = $op->first;
	want_unop($S, $hashop, "srefgen");
	$hashop = $hashop->first;
	$hashop = $hashop->first while is_unop($hashop, "null");
	want_listop($S, $hashop, "anonhash");

	my $saved_values = $S->{values};
	$S->{values} = [];

	my $want_const = 1;
	my $field;
	for my $c (get_all_children($hashop)) {
		next if is_op($c, "pushmark");
		if ($want_const) {
			$field = want_const($S, $c);
			$want_const = 0;
		} else {
			my $set = parse_term($S, $c);
			push @{$S->{set_values}}, @{$S->{values}};
			push @{$S->{sets}}, "$field = $set";
			$S->{values} = [];
			$want_const = 1;
			$field = undef;
		}
	}

	$S->{values} = $saved_values;

	$op = $op->last;

	my $tab;
	if (is_op($op, "padsv")) {
		my $var = get_var($S, $op);
		$tab = $S->{vars}{$var};
	} elsif (is_unop($op, "entersub")) {
		$op = $op->first;
		$op = $op->first if is_unop($op, "null");
		$op = $op->sibling if is_op($op, "pushmark");
		$op = $op->first if is_unop($op, "rv2cv");
		my $gv = get_gv($S, $op);
		$tab = $gv->NAME if $gv;
	}
	bailout $S, "cannot get a table to update" unless $tab;
}

my %binop_map = (
	eq       => "=",
	seq      => "=",
	ne       => "<>",
	sne      => "<>",
	slt      => "<",
	gt       => ">",
	sgt      => ">",
	le       => "<=",
	sle      => "<=",
	ge       => ">=",
	sge      => ">=",
	add      => "+",
	subtract => "-",
	multiply => "*",
	divide   => "/",
);

sub parse_expr
{
	my ($S, $op) = @_;
	my $sqlop;
	if ($sqlop = $binop_map{$op->name}) {
		my $left = parse_term($S, $op->first);
		my $right = parse_term($S, $op->last);

		return "$left $sqlop $right";
	} elsif ($op->name eq "lt") {
		if (is_unop($op->last, "negate")) {
			my $r = try_parse_subselect($S, $op);
			return $r if $r;
		}
		# if the "subselect theory" fails, try a normal binop
		my $left = parse_term($S, $op->first);
		my $right = parse_term($S, $op->last);
		return "$left < $right";
	} elsif ($op->name eq "sassign") {
		parse_assign($S, $op);
		return ();
	} else {
		bailout $S, "unsupported binop " . $op->name;
	}
}

sub parse_entersub
{
	my ($S, $op) = @_;
	my $tab = try_parse_attr_assignment($S, $op);
	return () if $tab;
	return parse_term($S, $op);
}

sub parse_regex
{
	my ( $S, $op, $neg) = @_;
	my ( $like, $case) = ( $op->precomp, $op-> pmflags & B::PMf_FOLD);

	my ($tab, $field) = get_tab_field($S, $op->first);

	my $flavor = lc($S-> {gen_args}-> {flavor} || '');
	my $what = 'like';
	
	my $can_like = $like =~ /^\^?[-\s\w]*\$?$/; # like that begins with non-% can use indexes
	
	if ( $flavor eq 'mysql') {
	
		# mysql LIKE is case-insensitive
		goto LIKE if not $case and $can_like;

		return 
			"$tab.$field ".
			( $neg ? 'not ' : '') . 
			'regexp ' .
			( $case ? '' : 'binary ') .
			"'$like'"
			;
	} elsif ( $flavor eq 'postgresql') {
		# LIKE is case-sensitive
		if ( $can_like) {
			$what = 'ilike' if $case;
			goto LIKE;
		} 
		return 
			"$tab.$field ".
			( $neg ? '!' : '') . 
			'~' .
			( $case ? '*' : '') .
			" '$like'"
			;
	} elsif ( $flavor eq 'sqlite') {
		# SQLite as it is now is a bit tricky:
		# - there is support for REGEXP with a func provided the user
		#   supplies his own function;
		# - LIKE is case-insensitive (for ASCII, anyway, there's a bug there);
		# - GLOB is case-sensitive;
		# - there is also support for MATCH - with a user func
		# Since it does not appear that SQLite can use indices
		# for prefix matches with simple LIKE statements, we
		# just hijack REGEXP and MATCH for case-sensitive
		# and case-insensitive cases.  If I am wrong on that,
		# or if SQLite gets and ability to do index-based
		# prefix matching, this logic can be modified accordingly.
		if ($case) {
			$what = "match";
			$S->{gen_args}->{dbh}->func($what, 2, sub {
				return scalar $_[1] =~ /\Q$_[0]\E/i;
			}, "create_function");
		} else {
			$what = "regexp";
			$S->{gen_args}->{dbh}->func($what, 2, sub {
				return scalar $_[1] =~ /\Q$_[0]\E/;
			}, "create_function");
		}
		push @{$S->{values}}, $like;
		# $what = $neg ? "not $what" : $what;
		# return "$tab.$field $what ?";
		return ($neg ? "not " : "") . "$what(?, $tab.$field)";
	} else {
		# XXX is SQL-standard LIKE case-sensitive or not?
		die "Don't know how to set case-insensitive flag for this DBI flavor"
			if $case;
		die "Regex too complex for implementation using LIKE keyword: $like"
			if $like =~ /(?<!\\)[\[\]\(\)\{\}\?\|]/;
LIKE:
		$like =~ s/%/\\%/g;
		$like =~ s/_/\\_/g;
		$like =~ s/\.\*/%/g;
		$like =~ s/\./_/g;
		$like = "%$like" unless $like =~ s|^\^||;
		$like = "$like%" unless $like =~ s|\$$||;
		return "$tab.$field " . 
			( $neg ? 'not ' : '') . 
			"$what '$like'"
		;
	}
}

sub try_parse_range
{
	my ($S, $op) = @_;
	return try_parse_range($S, $op->first) if is_unop($op, "null");
	return unless is_unop($op, "flop");
	$op = $op->first;
	return unless is_unop($op, "flip");
	$op = $op->first;
	return unless is_logop($op, "range");
	return (parse_simple_term($S, $op->first),
			parse_simple_term($S, $op->other));
}

sub parse_or
{
	my ($S, $op) = @_;
	if (is_op($op->other, "last")) {
		bailout "there should be no \"last\" statements in $S->{operation}'s query sub"
			unless $S->{operation} eq "select";
		my ($from, $to) = try_parse_range($S, $op->first);
		bailout $S, "range operator expected" unless defined $to;
		$S->{offset} = $from;
		$S->{limit}  = $to-$from+1;
		return;
	} else {
		my $left  = parse_term($S, $op->first);
		my $right = parse_term($S, $op->first->sibling);
		return "$left or $right";
	}
}

my $action_orderby = {
	kind => 'termlist',
	key  => 'order_by',
};
my $action_groupby = {
	kind => 'fieldlist',
	key  => 'group_by',
};
my $action_limit = {
	kind => 'numassign',
	key  => 'limit',
};
my $action_offset = {
	kind => 'numassign',
	key  => 'offset',
};
my $action_distinct = {
	kind => 'notice',
	key  => 'distinct',
};
my %labelmap = (
	select => {
		orderby  => $action_orderby,
		order_by => $action_orderby,
		order    => $action_orderby,
		sortby   => $action_orderby,
		sort_by  => $action_orderby,
		sort     => $action_orderby,

		groupby  => $action_groupby,
		group_by => $action_groupby,
		group    => $action_groupby,

		limit    => $action_limit,

		offset   => $action_offset,

		distinct => $action_distinct,
	},
);

sub parse_labels
{
	my ($S, $lop) = @_;
	my $label = $labelmap{$S->{operation}}->{lc $lop->label};
	bailout $S, "label ", $lop->label, " is not understood"
		unless $label;
	my $op = $lop->sibling;
	# TODO add kind fieldlist
	if ($label->{kind} eq "termlist") {
		my @op;
		if (is_listop($op, "list")) {
			@op = get_all_children($op);
		} else {
			push @op, $op;
		}
		my $order = "";
		for $op (@op) {
			next if is_op($op, "pushmark");
			my $term;
			$term = parse_term($S, $op)
				unless $term = is_const($S, $op);
			if ($label->{key} eq "order_by") {
				# special case for sort order
				if ($term =~ /^asc/i) {
					next;  # skip "ascending"
				} elsif ($term =~ /^desc/i) {
					$order = "desc";
					next;
				} else {
					if ($order) {
						push @{$S->{$label->{key}}}, "$term $order";
						$order = "";
					} else {
						push @{$S->{$label->{key}}}, $term;
					}
				}
			} else {
				push @{$S->{$label->{key}}}, $term;
			}
		}
		$S->{skipnext} = 1;
	} elsif ($label->{kind} eq "fieldlist") {
		my @op;
		if (is_listop($op, "list")) {
			@op = get_all_children($op);
		} else {
			push @op, $op;
		}
		for $op (@op) {
			next if is_op($op, "pushmark");
			my ($t, $f) = get_tab_field($S, $op);
			push @{$S->{$label->{key}}},
				($S->{operation} eq "delete" || $S->{operation} eq "update") ?
				$f : "$t.$f";
		}
		$S->{skipnext} = 1;
	} elsif ($label->{kind} eq "numassign") {
		my ($const,$sv) = is_const($S, $op);
		if (!$sv && is_op($op, "padsv")) {
			if (find_aliased_tab($S, $op)) {
				bailout $S, "cannot use table variable after ", $lop->label;
			}
			$sv = $S->{padlist}->[1]->ARRAYelt($op->targ);
			$const = ${$sv->object_2svref};
		}
		bailout $S, "label ", $lop->label, " must be followed by an integer or integer variable"
			unless $sv && ref $sv eq "B::IV";
		$S->{$label->{key}} = $const;
		$S->{skipnext} = 1;
	} elsif ($label->{kind} eq "notice") {
		$S->{$label->{key}}++;
	} else {
		bailout $S, "internal error parsing label ", $op->label;
	}
}

sub parse_op
{
	my ($S, $op) = @_;

	if ($S->{skipnext}) {
		delete $S->{skipnext};
		return;
	}
	if (is_listop($op, "list")) {
		parse_list($S, $op);
	} elsif (is_listop($op, "lineseq")) {
		parse_list($S, $op);
	} elsif (is_listop($op, "return")) {
		parse_return($S, $op);
	} elsif (is_binop($op)) {
		push @{$S->{where}}, parse_expr($S, $op);
	} elsif (is_unop($op, "not")) {
		push @{$S->{where}}, parse_term($S, $op);
	} elsif (is_logop($op, "or")) {
		my $or = parse_or($S, $op);
		push @{$S->{where}}, $or if $or;
	} elsif (is_unop($op, "leavesub")) {
		parse_op($S, $op->first);
	} elsif (is_unop($op, "null")) {
		parse_op($S, $op->first);
	} elsif (is_op($op, "padsv")) {
		# XXX Skip for now, it is either a variable
		# that does not represent a table, or else
		# it is already associated with a table in $S.
	} elsif (is_op($op, "pushmark")) {
		# skip
	} elsif (is_cop($op, "nextstate")) {
		$S->{file} = $op->file;
		$S->{line} = $op->line;
		if ($op->label) {
			parse_labels($S, $op);
		}
	} elsif (is_cop($op)) {
		# XXX any other things?
		$S->{file} = $op->file;
		$S->{line} = $op->line;
		# skip
	} elsif (is_unop($op, "entersub")) {
		push @{$S->{where}}, parse_entersub($S, $op);
	} elsif (ref($op) eq "B::PMOP" && $op->name eq "match") {
		push @{$S->{where}}, parse_regex( $S, $op, 0);
	} else {
		print "$op\n";
		if (ref($op) eq "B::PMOP") {
			print "reg: ", $op->precomp, "\n";
		}
		print "type: ", $op->type, "\n";
		print "name: ", $op->name, "\n";
		print "desc: ", $op->desc, "\n";
		print "targ: ", $op->targ, "\n";
		bailout $S, "???";
	}
}

sub parse_sub
{
	my ($S, $sub) = @_;
	if ($DEVEL) {
		$Carp::Verbose = 1;
		require B::Concise;
		my $walker = B::Concise::compile('-terse', $sub);
		print "CODE DUMP:\n";
		$walker->();
		print "\n\n";
	}
	my $root = B::svref_2object($sub);
	$S->{padlist} = [$root->PADLIST->ARRAY];
	$root = $root->ROOT;
	parse_op($S, $root);
}

sub init
{
	my %args = @_;
	my $S = {
		gen_args   => \%args,
		file       => '??',
		line       => '??',
		subselect  => 's01',
		operation  => $args{operation},
		values     => [],
		sets       => [],
		set_values => [],
		order_by   => [],
		group_by   => [],
	};
	$S->{alias} = $args{prefix} ? "$args{prefix}_t01" : "t01";
	$S;
}

# Borrowed from IO::All by Ingy döt Net.
my $old_warn_handler = $SIG{__WARN__}; 
$SIG{__WARN__} = sub { 
	if ($_[0] !~ /^Useless use of .+ in void context/) {
		goto &$old_warn_handler if $old_warn_handler;
		warn(@_);
	}
};

1;
