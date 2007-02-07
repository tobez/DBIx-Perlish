use inc::Module::Install;

name			'DBIx-Perlish';
license			'bsd';
all_from		'lib/DBIx/Perlish.pm';

requires		perl => '5.8.2';
requires		'DBI';
build_requires	'Test::More' => '0.42';

no_index		directory => 'devel';

features		'$dbh special handling' => [
	-default	=> 1,
	PadWalker	=> ''];

WriteAll;
