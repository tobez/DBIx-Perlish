use warnings;
use strict;
use DBIx::Perlish qw/:all/;
use Test::More;
use t::test_utils;

sub check
{
	my ( $err, $descr ) = @_;
	eval { 
		db_fetch { my $t : table }; 
	};
	like( $@, $err, $descr);
}

check( qr/Database handle not set/, "plain call");
DBIx::Perlish->connection( sub { 1 } );
check( qr/Invalid database handle/, "connection");
DBIx::Perlish->connection( undef );
check( qr/Database handle not set/, "call after undef");
DBIx::Perlish->connection( sub { 1 }, 'wrong' );
check( qr/Database handle not set/, "wrong connection");
DBIx::Perlish->connection( sub { 1 }, 'main' );
check( qr/Invalid database handle/, "connection again");

done_testing;
