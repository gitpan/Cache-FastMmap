
#########################
our ($IsWin, $Tests);

BEGIN {
  $IsWin = 0;
  $Tests = 7;

  if ($^O eq "MSWin32") {
    $IsWin = 1;
    $Tests -= 2;
  }
}

use Test::More tests => $Tests;
BEGIN { use_ok('Cache::FastMmap') };
use strict;

#########################

# Insert your test code below, the Test::More module is use()ed here so read
# its man page ( perldoc Test::More ) for help writing this test script.

my $FC = Cache::FastMmap->new(init_file => 1, raw_values => 1);
ok( defined $FC );

# Check get_and_set()

ok( $FC->set("cnt", 1), "set counter" );
is( $FC->get_and_set("cnt", sub { return ++$_[1]; }), 2, "get_and_set 1" );
is( $FC->get_and_set("cnt", sub { return ++$_[1]; }), 3, "get_and_set 2" );

# Basic atomicness test

if (!$IsWin) {

if (fork()) {
  is( $FC->get_and_set("cnt", sub { sleep(2); return ++$_[1]; }), 4, "get_and_set 3");
  sleep(1);
  is( $FC->get("cnt"), 5, "get_and_set 4");

} else {
  sleep(1);
  $FC->get_and_set("cnt", sub { return ++$_[1]; });
  CORE::exit(0);
}

}

