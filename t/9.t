
#########################

use Test::More tests => 3;
BEGIN { use_ok('Cache::FastMmap') };

#########################

# Insert your test code below, the Test::More module is use()ed here so read
# its man page ( perldoc Test::More ) for help writing this test script.

my $FC = Cache::FastMmap->new(
  page_size => 8192,
  num_pages => 3,
  init_file => 1,
  raw_values => 1
);
ok( defined $FC );

sub rand_str {
  my $len = shift;
  my $str = '';
  $str .= rand() while length($str) < $len;
  return substr($str, 0, $len);
}

srand(0);

my $Bad = 0;
for (1 .. 100000) {
  my ($K, $V) = (rand_str(int(rand(20)+2)), rand_str(int(rand(20)+1)));
  $FC->set($K, $V);
  if ($V ne $FC->get($K)) {
    $Bad = 1;
    last;
  }
}

ok( !$Bad );

