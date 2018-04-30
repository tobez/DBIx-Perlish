use warnings;
use strict;
use Test::More tests => 4;
BEGIN { sub except (&;$) {} }
use DBIx::Perlish qw/:all/;
use t::test_utils;

test_bad_sql(sub { my $a : tab }, "bad db operation", qr/unsupported operation: ahaha/, "ahaha");

my $dbh;
eval { DBIx::Perlish::init(x => 1) };
my $err = $@||"";
like($err, qr/"dbh" parameter is required/, "bad init - no dbh");
eval { db_fetch { my $a : tab } };
$err = $@||"";
like($err, qr/Invalid database handle/, "bad fetch - no dbh present");

$DBIx::Perlish::Parse::DEVEL = 1;
eval { DBIx::Perlish::Parse::bailout("no coverage") };
$err = $@||"";
like($err, qr/no coverage/, "coverage: devel bailout");
$DBIx::Perlish::Parse::DEVEL = 0;
