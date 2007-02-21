package DBIx::Perlish;
# $Id$

use 5.008;
use warnings;
use strict;
use Carp;

use vars qw($VERSION @EXPORT @EXPORT_OK %EXPORT_TAGS $SQL @BIND_VALUES);
require Exporter;
use base 'Exporter';

$VERSION = '0.17';
@EXPORT = qw(db_fetch db_update db_delete db_insert sql);
@EXPORT_OK = qw(union intersect except);
%EXPORT_TAGS = (all => [@EXPORT, @EXPORT_OK]);

use DBIx::Perlish::Parse;
use DBI::Const::GetInfoType;

sub db_fetch  (&) { DBIx::Perlish->fetch ($_[0]) }
sub db_update (&) { DBIx::Perlish->update($_[0]) }
sub db_delete (&) { DBIx::Perlish->delete($_[0]) }
sub db_insert { DBIx::Perlish->insert(@_) }

sub union (&) {}
sub intersect (&) {}
sub except (&) {}

my $default_object;

sub get_dbh
{
	my ($lvl) = @_;
	my $dbh;
	if ($default_object) {
		$dbh = $default_object->{dbh};
	}
	eval { require PadWalker; };
	unless ($@) {
		unless ($dbh) {
			my $vars = PadWalker::peek_my($lvl);
			$dbh = ${$vars->{'$dbh'}} if $vars->{'$dbh'};
		}
		unless ($dbh) {
			my $vars = PadWalker::peek_our($lvl);
			$dbh = ${$vars->{'$dbh'}} if $vars->{'$dbh'};
		}
	}
	die "Database handle not set.  Maybe you forgot to call DBIx::Perlish::init()?\n" unless $dbh;
	unless (ref $dbh && ref $dbh eq "DBI::db") { # XXX maybe relax for other things?
		die "Invalid database handle found.\n";
	}
	$dbh;
}

sub init
{
	my %p;
	if (@_ == 1) {
		$p{dbh} = $_[0];
	} else {
		%p = @_;
	}
	die "The \"dbh\" parameter is required\n" unless $p{dbh};
	unless (ref $p{dbh} && ref $p{dbh} eq "DBI::db") { # XXX maybe relax for other things?
		die "Invalid database handle supplied in the \"dbh\" parameter.\n";
	}
	$default_object = DBIx::Perlish->new(dbh => $p{dbh});
}

sub new
{
	my ($class, %p) = @_;
	unless (ref $p{dbh} && ref $p{dbh} eq "DBI::db") { # XXX maybe relax for other things?
		die "Invalid database handle supplied in the \"dbh\" parameter.\n";
	}
	bless { dbh => $p{dbh} }, $class;
}

sub fetch
{
	my ($moi, $sub) = @_;
	my $me = ref $moi ? $moi : {};

	my $nret;
	my $dbh = $me->{dbh} || get_dbh(3);
	($me->{sql}, $me->{bind_values}, $nret) = gen_sql($sub, "select", 
		flavor => $dbh-> get_info($GetInfoType{SQL_DBMS_NAME}),
		dbh    => $dbh,
	);
	$SQL = $me->{sql}; @BIND_VALUES = @{$me->{bind_values}};
	if ($nret > 1) {
		my $r = $dbh->selectall_arrayref($me->{sql}, {Slice=>{}}, @{$me->{bind_values}}) || [];
		return wantarray ? @$r : $r->[0];
	} else {
		my $r = $dbh->selectcol_arrayref($me->{sql}, {}, @{$me->{bind_values}}) || [];
		return wantarray ? @$r : $r->[0];
	}
}

# XXX refactor update/delete into a single implemention if possible?
sub update
{
	my ($moi, $sub) = @_;
	my $me = ref $moi ? $moi : {};

	my $dbh = $me->{dbh} || get_dbh(3);
	($me->{sql}, $me->{bind_values}) = gen_sql($sub, "update",
		flavor => $dbh-> get_info($GetInfoType{SQL_DBMS_NAME}),
		dbh    => $dbh,
	);
	$SQL = $me->{sql}; @BIND_VALUES = @{$me->{bind_values}};
	$dbh->do($me->{sql}, {}, @{$me->{bind_values}});
}

sub delete
{
	my ($moi, $sub) = @_;
	my $me = ref $moi ? $moi : {};

	my $dbh = $me->{dbh} || get_dbh(3);
	($me->{sql}, $me->{bind_values}) = gen_sql($sub, "delete",
		flavor => $dbh-> get_info($GetInfoType{SQL_DBMS_NAME}),
		dbh    => $dbh,
	);
	$SQL = $me->{sql}; @BIND_VALUES = @{$me->{bind_values}};
	$dbh->do($me->{sql}, {}, @{$me->{bind_values}});
}

sub insert
{
	my ($moi, $table, @rows) = @_;
	my $me = ref $moi ? $moi : {};

	my $dbh = $me->{dbh} || get_dbh(3);
	for my $row (@rows) {
		my $sql = "insert into $table (";
		$sql .= join ",", keys %$row;
		$sql .= ") values (";
		my (@v, @b);
		for my $v (values %$row) {
			if (ref $v eq 'CODE') {
				push @v, scalar $v->();
			} else {
				push @v, "?";
				push @b, $v;
			}
		}
		$sql .= join ",", @v;
		$sql .= ")";
		return undef unless defined $dbh->do($sql, {}, @b);
	}
	return scalar @rows;
}

sub sql ($) {
	my $self = shift;
	if (ref $self && $self->isa("DBIx::Perlish")) {
		$self->{sql};
	} else {
		sub { $self }
	}
}
sub bind_values { $_[0]->{bind_values} ? @{$_[0]->{bind_values}} : () }

sub gen_sql
{
	my ($sub, $operation, %args) = @_;

	my $S = DBIx::Perlish::Parse::init(%args, operation => $operation);
	DBIx::Perlish::Parse::parse_sub($S, $sub);
	my $sql;
	my $nret = 9999;
	my $no_aliases;
	if ($operation eq "select") {
		$sql = "select ";
		$sql .= "distinct " if $S->{distinct};
		if ($S->{returns}) {
			$sql .= join ", ", @{$S->{returns}};
			$nret = @{$S->{returns}};
			for my $ret (@{$S->{returns}}) {
				$nret = 9999 if $ret =~ /\*/;
			}
		} else {
			$sql .= "*";
		}
		$sql .= " from ";
	} elsif ($operation eq "delete") {
		$no_aliases = 1;
		$sql = "delete from ";
	} elsif ($operation eq "update") {
		$no_aliases = 1;
		$sql = "update ";
	} else {
		die "unsupported operation: $operation\n";
	}
	my %tabs;
	for my $var (keys %{$S->{vars}}) {
		$tabs{$S->{var_alias}->{$var}} =
			$no_aliases ?
				"$S->{vars}->{$var}" :
				"$S->{vars}->{$var} $S->{var_alias}->{$var}";
	}
	for my $tab (keys %{$S->{tabs}}) {
		$tabs{$S->{tab_alias}->{$tab}} =
			$no_aliases ?
				"$tab" :
				"$tab $S->{tab_alias}->{$tab}";
	}
	die "no tables specified in $operation\n" unless keys %tabs;
	$sql .= join ", ", map { $tabs{$_} } sort keys %tabs;

	$S->{sets}     ||= [];
	$S->{where}    ||= [];
	$S->{group_by} ||= [];
	$S->{order_by} ||= [];
	my @sets     = grep { $_ ne "" } @{$S->{sets}};
	my @where    = grep { $_ ne "" } @{$S->{where}};
	my @group_by = grep { $_ ne "" } @{$S->{group_by}};
	my @order_by = grep { $_ ne "" } @{$S->{order_by}};

	if ($S->{autogroup_needed} && !$S->{no_autogroup} &&
		!@group_by && @{$S->{autogroup_by}})
	{
		@group_by = grep { $_ ne "" } @{$S->{autogroup_by}};
	}

	$sql .= " set "      . join ", ",    @sets     if @sets;
	$sql .= " where "    . join " and ", @where    if @where;
	$sql .= " group by " . join ", ",    @group_by if @group_by;
	$sql .= " order by " . join ", ",    @order_by if @order_by;

	if ($S->{limit}) {
		$sql .= " limit $S->{limit}";
	}
	if ($S->{offset}) {
		$sql .= " offset $S->{offset}";
	}
	my $v = $S->{set_values} || [];
	push @$v, @{$S->{ret_values} || []};
	push @$v, @{$S->{values} || []};

	for my $add (@{$S->{additions}}) {
		$sql .= " $add->{type} $add->{sql}";
		push @$v, @{$add->{vals}};
	}

	return ($sql, $v, $nret);
}


1;
__END__

=head1 NAME

DBIx::Perlish - a perlish interface to SQL databases


=head1 VERSION

This document describes DBIx::Perlish version 0.17


=head1 SYNOPSIS

    use DBI;
    use DBIx::Perlish;

    my $dbh = DBI->connect(...);
    DBIx::Perlish::init($dbh);

    # selects:
    my @rows = db_fetch {
        my $x : users;
        defined $x->id;
        $x->name !~ /\@/;
    };

    # sub-queries:
    my @rows = db_fetch {
        my $x : users;
        $x->id <- db_fetch {
            my $t2 : table1;
            $t2->col == 2 || $t2->col == 3;
            return $t2->user_id;
        };
        $x->name !~ /\@/;
    };

    # updates:
    db_update {
        data->num < 100;
        data->mutable;

        data->num = data->num + 1;
        data->name = "xyz";
    };

    # more updates:
    db_update {
        my $d : data;
        $d->num < 100, $d->mutable;

        $d = {
            num  => $d->num + 1,
            name => "xyz"
        };
    };

    # deletes:
    db_delete {
        my $t : table1;
        !defined $t->age  or
        $t->age < 18;
    };

    # inserts:
    my $id = 42;
    db_insert 'users', {
        id   => $id,
        name => "moi",
    };


=head1 DESCRIPTION

The C<DBIx::Perlish> module provides the ability to work with databases
supported by the C<DBI> module using Perl's own syntax for four most
common operations: SELECT, UPDATE, DELETE, and INSERT.

By using C<DBIx::Perlish>, you can write most of your database
queries using a domain-specific language with Perl syntax.
Since a Perl programmer knows Perl by definition,
and might not know SQL to the same degree, this approach
generally leads to a more comprehensible and maintainable
code.

The module is not intended to replace 100% of SQL used in your program.
There is a hope, however, that it can be used to replace
a substantial portion of it.

The C<DBIx::Perlish> module quite intentionally neither implements
nor cares about database administration tasks like schema design
and management.  The plain C<DBI> interface is quite sufficient for
that.  Similarly, and for the same reason, it does not take care of
establishing database connections or handling transactions.  All this
is outside the scope of this module.


=head2 Ideology

There are three sensible and semi-sensible ways of arranging code that
works with SQL databases in Perl:

=over

=item SQL sprinkling approach

One puts queries wherever one needs to do something with the database,
so bits and pieces of SQL are intermixed with the program logic.
This approach can easily become an incomprehensible mess that is difficult
to read and maintain.

=item Clean and tidy approach

Everything database-related is put into a separate module, or into a
collection of modules.  Wherever database access is required,
a corresponding sub or method from such a module is called from the
main program.  Whenever something is needed that the DB module does
not already provide, a new sub or method is added into it.

=item Object-relational mapping

One carefully designs the database schema and an associated collection
of classes, then formulates the design in terms of any of the existing
object-relational mapper modules like C<Class::DBI>, C<DBIx::Class>
or C<Tangram>, then uses objects which perform all necessary queries
under the hood.  This approach is even cleaner than "clean and tidy"
above, but it has other issues.  Some schemas do not map well into
the OO space.  Typically, the resulting performance is an issue
as well.  The performance issues can in some cases be alleviated
by adding hand-crafted SQL in strategic places, so in this regard
the object-relational mapping approach can resemble the "clean and tidy"
approach.

=back

The C<DBIx::Perlish> module is meant to eliminate the majority
of the "SQL sprinkling" style of database interaction.
It is also fully compatible with the "clean and tidy" method.

=head2 Procedural interface

=head3 init()

The C<init()> sub initializes procedural interface
to the module.

It accepts named parameters.
The C<init()> function understands only one such parameter,
C<dbh>, which must be a valid DBI database handler.
This parameter is required.

All other parameters are silently ignored.

Alternatively, C<init()> can be called with a single
positional parameter, in which case it is assumed to
be the DBI database handler.

If the supplied database handler is not valid, an
exception is thrown.

This procedure does not return anything meaningful.

Examples:

    my $dbh = DBH->connect(...);
    DBIx::Perlish::init(dbh => $dbh);

    my $dbh = DBH->connect(...);
    DBIx::Perlish::init($dbh);

=head3 db_fetch {}

The C<db_fetch {}> function queries and returns data from
the database.

The function parses the supplied query sub,
converts it into the corresponding SQL SELECT statement,
and executes it.

What it returns depends on two things: the context and the
return statement in the query sub, if any.

If there is a return statement which specifies exactly one
column, and C<db_fetch {}> is called in the scalar context,
a single scalar representing the requested column is returned
for the first row of selected data.  Example:

    my $somename = db_fetch { return user->name };

Borrowing DBI's terminology, this is analogous to

    my $somename =
        $dbh->selectrow_array("select name from user");

If there is a return statement which specifies exactly one
column, and C<db_fetch {}> is called in the list context,
an array containing the specified column for all selected
rows is returned.  Example:

    my @allnames = db_fetch { return user->name };

This is analogous to

    my @allnames =
        @{$dbh->selectcol_arrayref("select name from user")};

When there is no return statement, or if 
the return statement specifies multiple columns,
then an individual row is represented by a hash
reference with column names as the keys.

In the scalar context, a single hashref is returned, which
corresponds to the first row of selected data.  Example:

    my $h = db_fetch { my $u : user };
    print "name: $h->{name}, id: $h->{id}\n";

In DBI parlance that would look like

    my $h = $dbh->selectrow_hashref("select * from user");
    print "name: $h->{name}, id: $h->{id}\n";

In the list context, an array of hashrefs is returned,
one element for one row of selected data:

    my @users = db_fetch { my $u : user };
    print "name: $_->{name}, id: $_->{id}\n" for @users;

Again, borrowing from DBI, this is analogous to

    my @users = @{$dbh->selectall_arrayref("select * from user",
        {Slice=>{}})};
    print "name: $_->{name}, id: $_->{id}\n" for @users;

The C<db_fetch {}> function will throw an exception if it is unable to
find a valid database handle to use, or if it is unable to convert its
query sub to SQL.

In addition, if the database handle is configured to throw exceptions,
the function might throw any of the exceptions thrown by DBI.

L</Subqueries> are permitted in db_fetch's query subs.

Please see L</Query sub syntax> below for details of the
syntax allowed in query subs.

The C<db_fetch {}> function is exported by default.

=head3 db_update {}

The C<db_update {}> function updates rows of a database table.

The function parses the supplied query sub,
converts it into the corresponding SQL UPDATE statement,
and executes it.

The function returns whatever DBI's C<do> method returns.

The function will throw an exception if it is unable to find
a valid database handle to use, or if it is unable to convert
its query sub to SQL.

In addition, if the database handle is configured to throw exceptions,
the function might throw any of the exceptions thrown by DBI.

A query sub of the C<db_update {}> function must refer
to precisely one table (not counting tables referred to
by subqueries).

Neither C<return> statements nor C<last> statements are
allowed in the C<db_update {}> function's query subs.

L</Subqueries> are permitted in db_update's query subs.

Please see L</Query sub syntax> below for details of the
syntax allowed in query subs.

Examples:

    db_update {
        tbl->id == 41;
        tbl->id = tbl->id - 1;
        tbl->name = "luff";
    };

    db_update {
        my $t : tbl;
        $t->id == 40;
        $t = {
            id   => $t->id + 2,
            name => "LIFF",
        };
    };

    db_update {
        tbl->id == 40;
        tbl() = {
            id   => tbl->id + 2,
            name => "LIFF",
        };
    };

The C<db_update {}> function is exported by default.


=head3 db_delete {}

The C<db_delete {}> function deletes data from
the database.

The C<db_delete {}> function parses the supplied query sub,
converts it into the corresponding SQL DELETE statement,
and executes it.

The function returns whatever DBI's C<do> method returns.

The function will throw an exception if it is unable to find
a valid database handle to use, or if it is unable to convert
its query sub to SQL.

In addition, if the database handle is configured to throw exceptions,
the function might throw any of the exceptions thrown by DBI.

A query sub of the C<db_delete {}> function must refer
to precisely one table (not counting tables referred to
by subqueries).

Neither C<return> statements nor C<last> statements are
allowed in the C<db_delete {}> function's query subs.

L</Subqueries> are permitted in db_delete's query subs.

Please see L</Query sub syntax> below for details of the
syntax allowed in query subs.

Examples:

    db_delete { $x : users } # delete all users

    # delete with a subquery
    db_delete {
        my $u : users;
        $u->name <- db_fetch {
            visitors->origin eq "Uranus";
            return visitors->name;
        }
    }

The C<db_delete {}> function is exported by default.


=head3 db_insert()

The C<db_insert()> function inserts rows into a
database table.

This function is different from the rest 
because it does not take a query sub as the parameter.

Instead, it takes a table name as its first parameter,
and any number of hash references afterwards.

For each specified hashref, a new row is inserted
into the specified table.  The resulting insert statement
specifies hashref keys as the column names, with corresponding
values taken from hashref values.  Example:

    db_insert 'users', { id => 1, name => "the.user" };

A value can be a call to the exported C<sql()> function,
in which case it is inserted verbatim into the generated
SQL, for example:

    db_insert 'users', {
        id => sql("some_seq.nextval"),
        name => "the.user"
    };

The function returns the number of insert operations performed.
If any of the DBI insert operations fail, the function returns
undef, and does not perform remaining inserts.

The function will throw an exception if it is unable to find
a valid database handle to use.

In addition, if the database handle is configured to throw exceptions,
the function might throw any of the exceptions thrown by DBI.

The C<db_insert {}> function is exported by default.


=head3 union()

This is a helper sub which is meant to be used inside
query subs.  Please see L</Compound queries' statements>
for details.  The C<union()> can be exported via C<:all>
import declaration.

=head3 intersect()

This is a helper sub which is meant to be used inside
query subs.  Please see L</Compound queries' statements>
for details.  The C<intersect()> can be exported via C<:all>
import declaration.

=head3 except()

This is a helper sub which is meant to be used inside
query subs.  Please see L</Compound queries' statements>
for details.  The C<except()> can be exported via C<:all>
import declaration.


=head3 $SQL and @BIND_VALUES

The C<DBIx::Perlish> module provides two global variables
(not exported) to aid in debugging.
The C<$DBIx::Perlish::SQL> variable contains the text of 
the SQL which was most recently generated by the procedures above
(except C<db_insert()>).
The C<@DBIx::Perlish::BIND_VALUES> array contains the bind values
to be used with the corresponding SQL code.

=head3 Special treatment of the C<$dbh> variable

If the procedural interface is used, and the user did not
call C<init()> before issuing any of the C<db_query {}>,
C<db_update {}>, C<db_delete {}> or C<db_insert {}>, those
functions look for one special case before bailing out.

Namely, they try to locate a variable C<my $dbh> or C<our $dbh>,
in that order, in the scope in which they are used.  If such
variable is found, and if it contains a valid C<DBI> database
handler, they will use it for performing the actual query.
This allows one to write something like that, and expect the
module to do the right thing:

    my $dbh = DBI->connect(...);
    my @r = db_fetch { users->name !~ /\@/ };

The author cannot recommend relying on this feature in the
production code;  if in doubt, call C<init()> first
and you won't be unpleasantly surprized.

In order for this feature to be operational, the C<PadWalker>
module must be installed.


=head2 Query sub syntax

The important thing to remember is that although the query subs have Perl
syntax, they do B<not> represent Perl, but a specialized "domain specific"
database query language with Perl syntax.

A query sub can consist of the following types of statements:

=over

=item *

table variables declarations;

=item *

query filter statements;

=item *

return statements;

=item *

assignments;

=item *

result limiting statements;

=item *

conditional statements;

=item *

statements with label syntax;

=item *

compound queries' statements.

=back

The order of the statements is generally not important,
except that table variables have to be declared before use.

=head3 Table variables declarations

Table variables declarations allow one to associate
lexical variables with database tables.  They look
like this:

    my $var : tablename;

It is possible to associate several variables with the
same table;  this is the preferable mechanism if self-joins
are desired.

In case the table name is not known until runtime, it is also
possible to write for example

    my $var : table = $data->{tablename};

In this case the attribute "table" must be specified verbatim,
and the name of the table is taken from the right-hand side of the
assignment.

Another possibility for declaring table variables is
described in L</Statements with label syntax>.

Please note that L</db_update {}> and L</db_delete {}> must
only refer to a single table.

=head3 Query filter statements

Query filter statements have a general form of Perl expressions.
Binary comparison operators, logical "or" (both high and lower
precedence form), matching operators =~ and !~, binary arithmetic
operators, string concatenation, defined(expr),
and unary ! are all valid in the filters.
There is also a special back-arrow, "comes from" C<E<lt>-> binary
operator used for matching a column to a set of values, and for
subqueries.

Individual terms can refer to a table column using dereferencing
syntax (either C<tablename-E<gt>column> or C<$tablevar-E<gt>column>),
to an integer, floating point, or string constant, to a function
call, or to a scalar value in the outer scope (simple scalars,
hash elements, or dereferenced hashref elements are supported).

Inside constant strings, table column specifiers are interpolated;
the result of such interpolation is represented as a sequence
of explicit SQL concatenation operations.
The variable interpolation syntax is somewhat different from
normal Perl rules, which does not interpolate method calls.
So it is perfectly legal to write

    return "abc $t->name xyz";

When it is impossible to distinguish between the column name
and the following characters, the hash element syntax must be
used instead:

    return "abc$t->{name}xyz";

Of course, one may want to avoid the trouble altogether and use explicit Perl
concatenation in such cases:

    return "abc" . $t->name . "xyz";

Please note that specifying column names as hash elements
is I<only> valid inside interpolated strings;  this may change
in the future versions of the module.

Please also note that column specifiers of
C<tablename-E<gt>column> form cannot be embedded into strings;
again, use explicit Perl concatenation in such cases.

Function calls can take an arbitrary number of arguments.
Each argument to a function must currently be a term,
although it is expected that more general expressions will
be supported in the future.
The function call appear verbatim in the resulting SQL,
with the arguments translated from Perl syntax to SQL
syntax.  For example:

    lower($t1->name) eq lower($t2->lastname);

The C<lc> and C<uc> builtin functions are translated to
C<lower> and C<upper>, respectively.

A special case is when C<sql()> function (with a single
parameter) is called.  In this case the parameter of the
function call inserted verbatim into the generated SQL,
for example:

    db_update {
        tab->state eq "new";
        tab->id = sql "some_seq.nextval";
    };

The "comes from" C<E<lt>-> binary operator can be used in the
following manner:

    my @ary = (1,2,3);
    db_fetch {
        tab->id  <-  @ary;
    };

This is equivalent to SQL's C<IN I<list>> operator, where
the list comes from the C<@ary> array.

The C<E<lt>-> operator can also be used with L</Subqueries>,
below.


=head3 Return statements

Return statements determine which columns are returned by
a query under what names.
Each element in the return statement can be either
a reference to the whole table, an expression involving
table columns, or a string constant,
in which case it is taken as an alias to
the next element in the return statement:

    return ($table->col1, anothername => $table->col2);

If an element is a reference to the whole table,
it is understood that all columns from this table
are returned:

    return ($t1->col1, $t1->col2, $t2);

Table references cannot be aliased by a name.

One can also specify a "distinct" or "DISTINCT"
string constant in the beginning of the return list,
in which case duplicated rows will be eliminated
from the result set.

Return statements are only valid in L</db_fetch {}>.

Query subs representing subqueries using the reverse
arrow notation must have exactly one return statement
returning exactly one column (see L</Subqueries> below).


=head3 Assignments

Assignments can take two form: individual column assignments
or bulk assignments.  The former must have a reference to
a table column on the left-hand side, and an expression
like those accepted in filter statements on the right-hand
side:

    table1->id = 42;
    $t->column = $t->column + 1;

The bulk assignments must have a table specifier on the left-hand
side, and a hash reference on the right-hand side.
The keys of the hash represent column names, and the values
are expressions like those in the individual column
assignments:

    $t = {
        id     => 42,
        column => $t->column + 1
    };

or

    tablename() = {
        id     => 42,
        column => tablename->column + 1
    };

Please note a certain ugliness in C<tablename()> in the last example,
so it is probably better to either use table vars, or stick to the
single assignment syntax of the first example.

Assignment statements are only valid in L</db_update {}>.

=head3 Result limiting statements

The C<last> command can be used to limit the number of
results returned by a fetch operation.

If it stands on its own anywhere in the query sub, it means "stop
after finding the first row that matches other filters", so it
is analogous to C<LIMIT 1> in many SQL dialects.

It can also be used in conjunction with a range C<..> operator,
so that

    last unless 5..20;

is equivalent to

    OFFSET 5 LIMIT 16

Result limiting statements are only valid in L</db_fetch {}>.

=head3 Conditional statements

There is a limited support for parse-time conditional expressions.

At the query sub parsing stage, if the conditional does not mention
any tables or columns, and refers exclusively to the values from the
outer scope, it is evaluated, and the corresponding filter (or any other
kind of statement) is only put into the generated SQL if the condition
is true.

For example,

    my $type = "ICBM";
    db_fetch {
        my $p : products;
        $p->type eq $type if $type;
    };

will generate the equivalent to C<select * from products where type = 'ICBM'>,
while the same code would generate just C<select * from products> if C<$type>
were false.


=head3 Statements with label syntax

There is a number of special labels which query sub syntax allows.

Specifying label C<distinct:> anywhere in the query sub leads to duplicated
rows being eliminated from the result set.

Specifying label C<limit:> followed by a number (or a scalar variable
representing a number) limits the number of rows returned by the query.

Specifying label C<offset:> followed by a number N (or a scalar variable
representing a number N) skips first N rows from the returned result
set.

Specifying label C<order:>, C<orderby:>, C<order_by:>,
C<sort:>, C<sortby:>, or C<sort_by:>, followed by a list of
expressions will sort the result set according to the expressions.
For details about the sorting criteria see the documentation
for C<ORDER BY> clause in your SQL dialect reference manual.
Before a sorting expression in a list one may specify one of the
string constants "asc", "ascending", "desc", "descending" to
alter the sorting order, for example:

    db_fetch {
        my $t : tbl;
        order_by: asc => $t->name, desc => $t->age;
    };

Specifying label C<group:>, C<groupby:>, or C<group_by:>,
followed by a list of column specifiers is equivalent to
the SQL clause C<GROUP BY col1, col2, ...>.

The module implements an I<experimental> feature which
in some cases allows one to omit the explicit
C<group_by:> label.  If there is an explicit C<return> statement
which mentions an aggregate function alongside "normal"
column specifiers, and that return statement does not
reference the whole table, and the explicit C<group_by:> label
is not present in the query, the 
C<DBIx::Perlish> module will generate one automatically.
For example, the following query:

    db_query {
        my $t : tab;
        return $t->name, $t->type, count($t->age);
    };

will execute the equivalent of the following SQL statement:

  select name, type, count(age) from tab group by name, type

The C<avg()>, C<count()>, C<max()>, C<min()>, and C<sub()>
functions are considered to be aggregate.

Specifying label C<table:> followed by a lexical variable
declaration, followed by an assignment introduces an alternative
table declaration syntax.  The value of the expression on the right
hand side of the assignment is taken to be the name of the table:

    my $data = { table => "mytable" };
    db_fetch {
        table: my $t = $data->{table};
    };

This is useful if you don't know the names of your table until
runtime.

All special labels are case insensitive.

Special labels are only valid in L</db_fetch {}>.


=head3 Compound queries' statements

The SQL compound queries UNION, INTERSECT, and EXCEPT are supported
using the following syntax:

    db_fetch {
        {
            ... normal query statements ...
        }
        compound-query-keyword
        {
            ... normal query statements ...
        }
    };

Here I<compound-query-keyword> is one of C<union>,
C<intersect>, or C<except>.

This feature will only work if the C<use> statement for
the C<DBIx::Perlish> module was written with C<:all>
export declaration, since C<union>, C<intersect>, and C<except>
are subs that are not exported by default by the module.

It is the responsibility of the programmer to make sure
that results of the individual queries used in a compound
query are compatible with each other.


=head3 Subqueries

It is possible to use subqueries in L</db_fetch {}>, L</db_update {}>,
and L</db_delete {}>.

There are two variants of subqueries.  The first one is a
call, as a complete statement,
to L</db_fetch {}> anywhere in the body of the query sub.
This variant corresponds to the C<EXISTS (SELECT ...)> SQL
construct, for example:

    db_delete {
        my $t : table1;
        db_fetch {
            $t->id == table2->table_id;
        };
    };

Another variant corresponds to the C<column IN (SELECT ...)> SQL
construct.  It uses a special syntax with back-arrow C<E<lt>->
(read it as "comes from"),
which signifies that the column specifier on the left gets
its values from whatever is returned by a L</db_fetch {}> on
the right:

    db_delete {
        my $t : table1;
        $t->id  <-  db_fetch {
            return table2->table_id;
        };
    };

This variant puts a limitation on the return statement in the sub-query
query sub.  Namely, it must contain a return statement with exactly one
return value.


=head2 Object-oriented interface

=head3 new()

Constructs and returns a new DBIx::Perlish object.

Takes a single mandatory named parameter, C<dbh>,
which must be a valid DBI database handler.

Can throw an exception if the supplied parameters
are incorrect.

=head3 fetch()

An object-oriented version of L</db_fetch {}>.

=head3 update()

An object-oriented version of L</db_update {}>.

=head3 delete()

An object-oriented version of L</db_delete {}>.

=head3 insert()

An object-oriented version of L</db_insert()>.

=head3 sql()

Takes no parameters.
Returns the SQL string, most recently generated by database
queries performed by the object.
Returns undef if there were no queries made thus far.

Example:

    $db->query(sub { $u : users });
    print $db->sql, "\n";

The C<sql()> sub can also be called in a procedural fashion,
in which case it serves the purpose of injecting
verbatim pieces of SQL into query subs
(see L</Query filter statements>) or into the values
to be inserted via L</db_insert>.

The C<sql()> function is exported by default.

=head3 bind_values()

Takes no parameters.
Returns an array of bind values that were used in the most recent
database query performed by the object.
Returns an empty array if there were not queries made thus far.

Example:

    $db->query(sub { users->name eq "john" });
    print join(", ", $db->bind_values), "\n";


=head2 Database driver specifics

The generated SQL output can differ depending on
the particular database driver in use.

=head3 MySQL

Native MySQL regular expressions are used if possible and if
a simple C<LIKE> won't suffice.

=head3 Oracle

The function call C<sysdate()> is transformed into C<sysdate>
(without parentheses).

=head3 Postgresql

Native Postgresql regular expressions are used if possible and if
a simple C<LIKE> won't suffice.

=head3 SQLite

Native Perl regular expressions are used with SQLite even for
simple match cases, since SQLite does not know how to optimize
C<LIKE> applied to an indexed column with a constant prefix.


=head2 Implementation details and more ideology

To achieve its purpose, this module uses neither operator
overloading nor source filters.

The operator overloading would only work if individual tables were
represented by Perl objects.  This means that an object-relational
mapper like C<Tangram> can do it, but C<DBIx::Perlish> cannot.

The source filters are limited in other ways: the modules using them
are often incompatible with other modules that also use source filtering,
and it is B<very> difficult to do source filtering when any degree of
flexibility is required.  Only perl can parse Perl!

The C<DBIx::Perlish> module, on the other hand, leverages perl's ability
to parse Perl and operates directly on the already compiled Perl code.
In other words, it parses the Perl op tree (syntax tree).

The idea of this module came from Erlang.  Erlang has a so called
I<list comprehension syntax>, which allows one to generate lists
using I<generator> expressions and to select the list elements using
I<filter> expressions.  Furthermore, the authors of the Erlang database,
Mnesia, hijacked this syntax for the purpose of doing database queries
via a mechanism called I<parse transform>.
The end result was that the database queries in Erlang are expressed
by using Erlang's own syntax.

I found this approach elegant, and thought "why something like this
cannot be done in Perl"?


=head1 CONFIGURATION AND ENVIRONMENT

DBIx::Perlish requires no configuration files or environment variables.


=head1 DEPENDENCIES

The C<DBIx::Perlish> module needs at least perl 5.8.2, quite possibly
a somewhat higher version.  I have only tested it on
5.8.8 and 5.8.4.

This module requires C<DBI> to do anything useful.

In order to support the special handling of the C<$dbh> variable,
C<PadWalker> needs to be installed.

Other modules used used by C<DBIx::Perlish> are included
into the standard Perl distribution.


=head1 INCOMPATIBILITIES

None reported.


=head1 BUGS AND LIMITATIONS

No bugs have been reported.

Please report any bugs or feature requests to
C<bug-dbix-perlish@rt.cpan.org>, or through the web interface at
L<http://rt.cpan.org>.

A number of features found in many SQL dialects is not supported.

The module cannot handle more than 100 tables in a single
query sub.

Although variables closed over the query sub can be used
in it, only simple scalars, hash elements, and dereferenced
hasref elements are understood at the moment.

If you would like to see something implemented,
or find a nice Perlish syntax for some SQL feature,
please let me know!

=head1 AUTHOR

Anton Berezin  C<< <tobez@tobez.org> >>

=head1 ACKNOWLEDGEMENTS

I would like to thank
Dmitry Karasik,
Henrik Andersen,
Lars Thegler,
and Phil Regnauld
for discussions, suggestions and code contributions.

This work is in part sponsored by Telia Denmark.


=head1 SUPPORT

There is a project Wiki at
  http://dbix-perlish.tobez.org/wiki/

There is also the project website at
  http://dbix-perlish.tobez.org/


=head1 LICENSE AND COPYRIGHT

Copyright (c) 2007, Anton Berezin C<< <tobez@tobez.org> >>. All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions
are met:

1. Redistributions of source code must retain the above copyright
   notice, this list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright
   notice, this list of conditions and the following disclaimer in the
   documentation and/or other materials provided with the distribution.

THIS SOFTWARE IS PROVIDED BY AUTHOR AND CONTRIBUTORS ``AS IS'' AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
ARE DISCLAIMED.  IN NO EVENT SHALL AUTHOR OR CONTRIBUTORS BE LIABLE
FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
SUCH DAMAGE.
