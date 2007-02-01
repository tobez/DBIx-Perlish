use inc::Module::Install;

name			'DBIx-Perlish';
all_from		'lib/DBIx/Perlish.pm';

build_requires	'Test::More' => '0.42';

no_index		directory => 'devel';

WriteAll;
