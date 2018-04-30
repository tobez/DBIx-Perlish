use warnings;
use strict;

package A;

use Test::More;
use t::test_utils;
use DBIx::Perlish qw/:all/;
sub fetch { db_fetch { my $t : table } }

sub check
{
	my ( $pkg, $err, $descr ) = @_;
	eval { 
		$pkg->fetch;
	};
	like( $@, $err, $descr);
}

package B;
our @ISA;
@ISA = qw(A);
use DBIx::Perlish qw/:all/;
sub dbh { 1 }
sub fetch { db_fetch { my $t : table } }

package C;
our @ISA;
@ISA = qw(B);
use DBIx::Perlish qw/:all/;
sub fetch { db_fetch { my $t : table } }

package main;

use Test::More;

A->check( qr/Database handle not set/, "plain call");
B->check( qr/Invalid database handle/, "connection direct");
C->check( qr/Invalid database handle/, "connection inherited");

done_testing;
