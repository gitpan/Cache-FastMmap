
#########################

use Test::More tests => 3;
BEGIN { use_ok('Cache::FastMmap') };

#########################

# Insert your test code below, the Test::More module is use()ed here so read
# its man page ( perldoc Test::More ) for help writing this test script.

my $FC = Cache::FastMmap->new(
  page_size => 4096,
  num_pages => 13,
  init_file => 1,
  raw_values => 1
);
ok( defined $FC );

sub rand_str {
  return join '', map { chr(rand(26) + ord('a')) } 1 .. $_[0];
}

srand(1);

my $Bad = 0;

my @LastEntries;
for (1 .. 100000) {
  my ($K, $V) = (rand_str(int(rand(20)+3)), rand_str(int(rand(20)+1)));
  $FC->set($K, $V);
  unshift @LastEntries, [ $K, $V ];
  pop @LastEntries if @LastEntries > 100;

  for (@LastEntries) {
    if ($_->[1] ne $FC->get($_->[0])) {
      $Bad = 1;
      last;
    }
  }
  last if $Bad;
}

ok( !$Bad );

