package DBIx::Perlish;
# $Id$

use 5.008;
use warnings;
use strict;
use Carp;

use vars qw($VERSION @EXPORT $SQL @BIND_VALUES);
require Exporter;
use base 'Exporter';

$VERSION = '0.09';
@EXPORT = qw(db_fetch db_update db_delete db_insert);

use DBIx::Perlish::Parse;
use DBI::Const::GetInfoType;

sub db_fetch  (&) { DBIx::Perlish->fetch ($_[0]) }
sub db_update (&) { DBIx::Perlish->update($_[0]) }
sub db_delete (&) { DBIx::Perlish->delete($_[0]) }
sub db_insert { DBIx::Perlish->insert(@_) }

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
		$sql .= join ",", ('?') x keys %$row;
		$sql .= ")";
		return undef unless defined $dbh->do($sql, {}, values %$row);
	}
	return scalar @rows;
}

sub sql { $_[0]->{sql} }
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
		if ($S->{returns}) {
			$sql .= join ", ", @{$S->{returns}};
			$nret = @{$S->{returns}};
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
	$sql .= join ", ", map { $tabs{$_} } sort keys %tabs;

	if ($S->{sets} && @{$S->{sets}}) {
		$sql .= " set ";
		$sql .= join ", ", @{$S->{sets}};
	}

	if ($S->{where}) {
		$sql .= " where " . join " and ", @{$S->{where}};
	}
	if ($S->{limit}) {
		$sql .= " limit $S->{limit}";
	}
	if ($S->{offset}) {
		$sql .= " offset $S->{offset}";
	}
	my $v = $S->{set_values} || [];
	push @$v, @{$S->{values} || []};
	return ($sql, $v, $nret);
}


1;
__END__

=head1 NAME

DBIx::Perlish - a perlish interface to SQL databases


=head1 VERSION

This document describes DBIx::Perlish version 0.09


=head1 SYNOPSIS

    use DBI;
    use DBIx::Perlish;

    my $dbh = DBI->connect(...);
    DBIx::Perlish::init($dbh);

    # selects:
    my @rows = db_fetch {
        my $x : users;
        $x->id != 0;
        $x->name !~ /\@/;
    };

    # sub-queries:
    my @rows = db_fetch {
        my $x : users;
        $x->id <- db_fetch {
            my $t2 : table;
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
        my $t : table;
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

The C<DBIx::Perlish> module provides an ability to work with databases supported
by the C<DBI> module using Perl's own syntax for four most common
operations: SELECT, UPDATE, DELETE, and INSERT.

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

=head2 Who this module is NOT for

=over

=item *

if you don't need database access, this module is not for you;

=item *

if you think that modules that provide object-relational mappings are
cool, and if you use Class::DBI or DBIx::Class a lot and they don't annoy
you, this module is not for you;

=item *

if you are a hard-core DBA and dream your dreams in SQL, this module is
not for you;

=item *

otherwise, read on!

=back

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
OO space.  Typically, the resulting performance is an issue
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
Currently C<init()> understands only one such parameter,
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

Please note a certain ugliness in C<tbl()> in the last example,
so it is probably better to either use table vars, or stick to the
single assignment syntax of the first example.

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

The function returns the number of insert operations performed.
If any of the DBI insert operations fail, the function returns
undef, and does not perform remaining inserts.

The function will throw an exception if it is unable to find
a valid database handle to use.

In addition, if the database handle is configured to throw exceptions,
the function might throw any of the exceptions thrown by DBI.

The C<db_insert {}> function is exported by default.


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

return statements (only valid for fetch operations);

=item *

assignments (only valid for update operations);

=item *

result limiting statements (only valid for fetch operations).

=back

The order of the statement is generally not important.

...

The C<last> command is special.
If it stands on its own anywhere in the query sub, it means "stop
after finding the first row that matches other filters", so it
is analogous to C<LIMIT 1> in many SQL dialects.

It can also be used in conjunction with a range C<..> operator,
so that

    last unless 5..20;

is equivalent to

    OFFSET 5 LIMIT 16

=head3 Subqueries

Bebebebe

=head2 Object-oriented interface

=head3 new()

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

=head3 bind_values()

Takes no parameters.
Returns an array of bind values that were used in the most recent
database query performed by the object.
Returns an empty array if there were not queries made thus far.

Example:

    $db->query(sub { users->name eq "john" });
    print join(", ", $db->bind_values), "\n";


=head2 Implementation details and more ideology

To achieve its purpose, this module uses neither operator
overloading nor source filters.

The operator overloading would only work if individual tables were
represented by Perl objects.  This means that an object-relational
mapper like Tangram can do it, but C<DBIx::Perlish> cannot.

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


=head1 DIAGNOSTICS

=for author to fill in:
    List every single error and warning message that the module can
    generate (even the ones that will "never happen"), with a full
    explanation of each problem, one or more likely causes, and any
    suggested remedies.

=over

=item C<< Error message here, perhaps with %s placeholders >>

[Description of error here]

=item C<< Another error message here >>

[Description of error here]

[Et cetera, et cetera]

=back


=head1 CONFIGURATION AND ENVIRONMENT

DBIx::Perlish requires no configuration files or environment variables.


=head1 DEPENDENCIES

The C<DBIx::Perlish> module needs at least perl 5.8.0, quite possibly
a somewhat higher version.  I have only tested it on
5.8.8 and 5.8.4.

This module requires C<DBI> to do anything useful.

In order to support the special handling of the C<$dbh> variable,
C<PadWalker> needs to be installed.

Other modules used are parts of the standard Perl distribution.


=head1 INCOMPATIBILITIES

None reported.


=head1 BUGS AND LIMITATIONS

No bugs have been reported.

Please report any bugs or feature requests to
C<bug-dbix-perlish@rt.cpan.org>, or through the web interface at
L<http://rt.cpan.org>.

The module has a lot of limitations.
Currently, the following SQL features are not supported
(only those features for which I would like to add support
in the future are listed), in no particular order:

=over

=item *

ORDER BY clause;

=item *

GROUP BY clause;

=item *

use of SQL functions;

=item *

EXISTS-style sub-queries;

=item *

the ability to refer to tables mentioned in the outer scope 
of sub-queries from the inner scope.

=back

Surely I've missed some other things.

If you would like to see something implemented,
or find a nice Perlish syntax for some SQL feature,
please let me know!

=head1 AUTHOR

Anton Berezin  C<< <tobez@tobez.org> >>

=head1 ACKNOWLEDGEMENTS

A big thank you for discussions, suggestions and code contributions go
to
Dmitry Karasik,
Henrik Andersen,
Lars Thegler,
and Phil Regnauld.

This module would not have been written
if not for the inspiration provided
by Erlang's approach to Mnesia database queries syntax;
I'd like to thank the person who came up with that idea -
according to some, it was Hans Nilsson, but I am not sure.

This work is in part sponsored by Telia Denmark.


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
