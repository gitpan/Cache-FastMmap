
#########################

use Test::More tests => 11;
BEGIN { use_ok('Cache::FastMmap') };
use strict;

#########################

# Insert your test code below, the Test::More module is use()ed here so read
# its man page ( perldoc Test::More ) for help writing this test script.

my $FC = Cache::FastMmap->new(init_file => 1, expire_time => 3, raw_values => 1);
ok( defined $FC );

ok( $FC->set('abc', '123'),    "expire set 1");
is( $FC->get('abc'), '123',    "expire get 1");
sleep(2);
ok( $FC->set('def', '456'),    "expire set 2");
is( $FC->get('abc'), '123',    "expire get 2");
is( $FC->get('def'), '456',    "expire get 3");
sleep(2);
ok( !defined $FC->get('abc'),  "expire get 4");
is( $FC->get('def'), '456',    "expire get 5");
sleep(2);
ok( !defined $FC->get('abc'),  "expire get 6");
ok( !defined $FC->get('def'),  "expire get 7");


