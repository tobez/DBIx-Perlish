package DBIx::Perlish;
# $Id$

use 5.008;
use warnings;
use strict;
use Carp;

use vars qw($VERSION @EXPORT $SQL @BIND_VALUES);
require Exporter;
use base 'Exporter';

$VERSION = '0.05';
@EXPORT = qw(db_fetch db_update db_delete db_insert);

use PadWalker;
use DBIx::Perlish::Parse;

sub db_fetch  (&) { DBIx::Perlish->fetch ($_[0]) }
sub db_update (&) { DBIx::Perlish->update($_[0]) }
sub db_delete (&) { DBIx::Perlish->delete($_[0]) }
sub db_insert (&) { DBIx::Perlish->insert($_[0]) }

my $default_object;

sub get_dbh
{
	my ($lvl) = @_;
	my $dbh;
	if ($default_object) {
		$dbh = $default_object->{dbh};
	}
	unless ($dbh) {
		my $vars = PadWalker::peek_my($lvl);
		$dbh = ${$vars->{'$dbh'}} if $vars->{'$dbh'};
	}
	unless ($dbh) {
		my $vars = PadWalker::peek_our($lvl);
		$dbh = ${$vars->{'$dbh'}} if $vars->{'$dbh'};
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
	($me->{sql}, $me->{bind_values}, $nret) = gen_sql($sub, "select");
	$SQL = $me->{sql}; @BIND_VALUES = @{$me->{bind_values}};
	my $dbh = $me->{dbh} || get_dbh(3);
	if ($nret > 1) {
		my $r = $dbh->selectall_arrayref($me->{sql}, {Slice=>{}}, @{$me->{bind_values}}) || [];
		return wantarray ? @$r : $r->[0];
	} else {
		my $r = $dbh->selectcol_arrayref($me->{sql}, {}, @{$me->{bind_values}}) || [];
		return wantarray ? @$r : $r->[0];
	}
}

sub update
{
	die "update is not yet implemented\n";
}

sub delete
{
	die "delete is not yet implemented\n";
}

sub insert
{
	die "insert is not yet implemented\n";
}

sub sql { $_[0]->{sql} }
sub bind_values { $_[0]->{bind_values} ? @{$_[0]->{bind_values}} : () }

sub gen_sql
{
	my ($sub, $operation, %args) = @_;

	my $S = DBIx::Perlish::Parse::init(%args);
	DBIx::Perlish::Parse::parse_sub($S, $sub);
	my $sql = "select ";
	my $nret = 9999;
	if ($S->{returns}) {
		$sql .= join ", ", @{$S->{returns}};
		$nret = @{$S->{returns}};
	} else {
		$sql .= "*";
	}
	$sql .= " from ";
	my %tabs;
	for my $var (keys %{$S->{vars}}) {
		$tabs{$S->{var_alias}->{$var}} =
			"$S->{vars}->{$var} $S->{var_alias}->{$var}";
	}
	for my $tab (keys %{$S->{tabs}}) {
		$tabs{$S->{tab_alias}->{$tab}} =
			"$tab $S->{tab_alias}->{$tab}";
	}
	$sql .= join ", ", map { $tabs{$_} } sort keys %tabs;
	if ($S->{where}) {
		$sql .= " where " . join " and ", @{$S->{where}};
	}
	if ($S->{limit}) {
		$sql .= " limit $S->{limit}";
	}
	if ($S->{offset}) {
		$sql .= " offset $S->{offset}";
	}
	my $v = $S->{values} || [];
	return ($sql, $v, $nret);
}


1;
__END__

=head1 NAME

DBIx::Perlish - a perlish interface to SQL databases


=head1 VERSION

This document describes DBIx::Perlish version 0.05


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

    # sub-selects:
    my @rows = db_fetch {
        my $x : users;
        $x->id <- db_fetch {
            table2->col == 2 || table2->col == 3;
            return table2->user_id;
        };
        $x->name !~ /\@/;
    };

    db_update {
        data->num = data->num + 1;
        data->num < 100 and data->mutable;
    };

    db_delete {
        my $t : table;
        !defined $t->age  or
        $t->age < 18;
    };

    my $id = 42;
    db_insert {
        my $u : users;
        push $u, {
            id   => $id,
            name => "moi",
        };
    };


=head1 DESCRIPTION

=head2 Who this module is NOT for

=over 4

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

=over 4

=item SQL sprinkling approach

One puts queries wherever one needs to do something with the database,
so bits and pieces of SQL are intermixed with the program logic.
This approach can easily become an incomprehensible mess difficult
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

By using C<DBIx::Perlish>, you can write most of your database
queries using a domain-specific language with Perl syntax.
Since a Perl programmer knows Perl by definition,
and might not know SQL to the same degree, this approach
generally leads to a more comprehensible and maintainable
code.

=head2 Procedural interface

=head3 init()

The C<init()> sub initializes procedural interface
of the module.

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

Please see L</Query sub syntax> below for details of the
syntax allowed in query subs.

=head3 db_update {}

Please see L</Query sub syntax> below for details of the
syntax allowed in query subs.

=head3 db_delete {}

Please see L</Query sub syntax> below for details of the
syntax allowed in query subs.

=head3 db_insert {}

Please see L</Query sub syntax> below for details of the
syntax allowed in query subs.

=head3 $SQL and @BIND_VALUES

The C<DBIx::Perlish> module provides two global variables
(not exported) to aid in debugging.
The C<$DBIx::Perlish::SQL> variable contains the text of 
the SQL which was most recently generated by the procedures above.
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


=head2 Object-oriented interface

=head3 new()

=head3 fetch()

=head3 update()

=head3 delete()

=head3 insert()

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
overloading, like C<Tangram> does, nor source filters, like the
concept module C<DBIx::SQL::Perlish> does.

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

=head2 Comparison with other modules

DBIx::SimplePerl

Write something here.

DBIx::SQL::Perlish

As far as I know, this is a vaporware for now.

=head1 INTERFACE 

=for author to fill in:
    Write a separate section listing the public components of the modules
    interface. These normally consist of either subroutines that may be
    exported, or methods that may be called on objects belonging to the
    classes provided by the module.


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

=for author to fill in:
    A full explanation of any configuration system(s) used by the
    module, including the names and locations of any configuration
    files, and the meaning of any environment variables or properties
    that can be set. These descriptions must also include details of any
    configuration language used.
  
DBIx::Perlish requires no configuration files or environment variables.


=head1 DEPENDENCIES

=for author to fill in:
    A list of all the other modules that this module relies upon,
    including any restrictions on versions, and an indication whether
    the module is part of the standard Perl distribution, part of the
    module's distribution, or must be installed separately. ]

None.


=head1 INCOMPATIBILITIES

=for author to fill in:
    A list of any modules that this module cannot be used in conjunction
    with. This may be due to name conflicts in the interface, or
    competition for system or program resources, or due to internal
    limitations of Perl (for example, many modules that use source code
    filters are mutually incompatible).

None reported.


=head1 BUGS AND LIMITATIONS

=for author to fill in:
    A list of known problems with the module, together with some
    indication Whether they are likely to be fixed in an upcoming
    release. Also a list of restrictions on the features the module
    does provide: data types that cannot be handled, performance issues
    and the circumstances in which they may arise, practical
    limitations on the size of data sets, special cases that are not
    (yet) handled, etc.

No bugs have been reported.

Please report any bugs or feature requests to
C<bug-dbix-perlish@rt.cpan.org>, or through the web interface at
L<http://rt.cpan.org>.


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
I'd like to thank XXX for this.

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
