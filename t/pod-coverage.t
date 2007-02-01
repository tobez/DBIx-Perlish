#!perl -T

use Test::More tests => 1;
eval "use Test::Pod::Coverage 1.04";
plan skip_all => "Test::Pod::Coverage 1.04 required for testing POD coverage" if $@;
pod_coverage_ok(
	"DBIx::Perlish",
	{ also_private => [ qr/^get_dbh$/, qr/^gen_sql$/ ] },
);
