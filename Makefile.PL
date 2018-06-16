use inc::Module::Install;
BEGIN { require 5.014 } # please CPAN::Reporter

name			'DBIx-Perlish';
license			'bsd';
all_from		'lib/DBIx/Perlish.pm';

requires		perl => '5.14.1';
requires		'DBI';
requires		'Devel::Declare';
requires		'Devel::Caller';
build_requires	'Test::More' => '0.42';

no_index		directory => 'devel';

repository 'https://github.com/tobez/DBIx-Perlish';

WriteAll;
