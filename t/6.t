
#########################

use Test::More;
eval "use GTop ()";
if ($@) {
  plan skip_all => 'No GTop installed, no memory leak tests';
} else {
  plan tests => 9;
}
BEGIN { use_ok('Cache::FastMmap') };
use strict;

my $GTop = GTop->new;

#########################

# Insert your test code below, the Test::More module is use()ed here so read
# its man page ( perldoc Test::More ) for help writing this test script.

our ($DidRead, $DidWrite, $DidDelete, $HitCount);

our $FC = Cache::FastMmap->new(
  init_file => 1,
  raw_values => 1,
  num_pages => 17,
  page_size => 8192,
  read_cb => sub { $DidRead++; return undef; },
  write_cb => sub { $DidWrite++; },
  delete_cb => sub { $DidDelete++; },
  write_action => 'write_back'
);

ok( defined $FC );

TestLeak(\&NewLeak);
TestLeak(\&NewLeak);
TestLeak(\&NewLeak2);
TestLeak(\&NewLeak2);

# Prefill cache to make sure all pages mapped
for (1 .. 2000) {
  $FC->set(RandStr(15), RandStr(10));
}
$FC->get('foo');

our $StartKey = 1;
TestLeak(\&SetLeak);

$StartKey = 1;
TestLeak(\&GetLeak);

$FC->clear();

$StartKey = 1;
TestLeak(\&WBLeak);

$FC->get_keys(0);
$FC->get_keys(1);
$FC->get_keys(2);
TestLeak(\&ListLeak);

sub RandStr {
  return join '', map { chr(ord('a') + rand(26)) } (1 .. $_[0]);
}

sub TestLeak {
  my $Sub = shift;

  my $Before = $GTop->proc_mem($$)->size;
  eval {
    $Sub->();
  };
  if ($@) {
    ok(0, "leak test died: $@");
  }
  my $After = $GTop->proc_mem($$)->size;

  ok( ($After - $Before)/1024 < 30, "leak test > 30k");
}

sub NewLeak {

  for (1 .. 1000) {
    $FC = Cache::FastMmap->new(
      init_file => 0,
      raw_values => 1,
      num_pages => 17,
      page_size => 8192,
      read_cb => sub { $DidRead++; return undef; },
      write_cb => sub { $DidWrite++; },
      delete_cb => sub { $DidDelete++; },
      write_action => 'write_back'
    );
  }

}

sub NewLeak2 {

  for (1 .. 100) {
    $FC = Cache::FastMmap->new(
      init_file => 1,
      raw_values => 1,
      num_pages => 17,
      page_size => 8192,
      read_cb => sub { $DidRead++; return undef; },
      write_cb => sub { $DidWrite++; },
      delete_cb => sub { $DidDelete++; },
      write_action => 'write_back'
    );
  }

}


sub SetLeak {
  for (1 .. 20000) {
    $FC->set("blah" . $StartKey++ . "blah", RandStr(10));
  }
}

sub GetLeak {
  for (1 .. 20000) {
    $HitCount++ if $FC->get("blah" . $StartKey++. "blah");
  }
}

sub WBLeak {
  for (1 .. 5000) {
    my $Key = "blah" . $StartKey++ . "blah";
    $FC->set($Key, RandStr(10));
    my $PreDidWrite = $DidWrite;
    $FC->empty();
    $PreDidWrite + 1 == $DidWrite
      || die "write count mismatch";
    $FC->get($Key)
      && die "get success";
  }
}

sub ListLeak {
  for (1 .. 1000) {
    my $a = [ $FC->get_keys(0) ];
    my $b = [ $FC->get_keys(1) ];
    my $c = [ $FC->get_keys(2) ];
  }
}
